import re
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from typing import Literal

import pandas as pd


class BaseMunicipalityData:
    """
    A base class for fetching and managing Swiss municipality (Gemeinde) data.

    :ivar api_path: The base URL for the BFS (Federal Statistical Office) API.
    :ivar data: A DataFrame containing all official Gemeindestände (municipality states).
    :ivar gmde_dct: A dictionary mapping Gemeindestände to sets of BFS municipality codes.
    """

    def __init__(self, which_data: Literal['code_mapping', 'name_matching'], start_date: str = '01-01-1981'):
        """
        Initialize by fetching all official Gemeindestände.

        :param start_date: The start date to fetch all gemeindestände for.
        :type start_date: str
        :raises Exception: If the API fails to fetch data.
        """
        self.api_path = 'https://www.agvchapp.bfs.admin.ch/api/communes'  # noqa
        self.base_path = Path(__file__).resolve().parent / 'data'
        
        if which_data == 'code_mapping':
            self.data, self.gmde_dct = self._fetch_base_data(which_data, start_date)
            self.all_historical_bfs_codes = set()
            for name_dct in self.gmde_dct.values():
                self.all_historical_bfs_codes.update(name_dct.keys())
        elif which_data == 'name_matching':
            self.officials = self._fetch_base_data(which_data, start_date)
            self.officials = self.officials.rename(
                columns={'bfs_gmde_code': 'matched_id', 'bfs_gmde_name': 'matched_name'}
            )
        else:
            raise ValueError("The variable which_data can only be `code_mapping`or `name_matching`.")

    def _fetch_base_data(self, which_data: Literal['code_mapping', 'name_matching'], start_date: str = '01-01-1981') -> tuple[pd.DataFrame, dict]:
        """
        Fetch all official Gemeindestände from the BFS API.

        This method retrieves data about municipality states (Gemeindestände) within
        a specified date range and organizes it into a DataFrame and dictionary.

        :param start_date: The start date to fetch all gemeindestände for.
        :type start_date: str
        :return: A tuple containing:
                 - A DataFrame with Gemeindestände.
                 - A dictionary mapping Gemeindestände to BFS codes.
        :rtype: tuple[pd.DataFrame, dict]
        """
        today = datetime.today().strftime('%d-%m-%Y')
        url = f'{self.api_path}/mutations?includeTerritoryExchange=false&startPeriod={start_date}&endPeriod={today}'  # noqa

        # Fetch mutation data
        mutations = pd.read_csv(url)
        mutations['MutationDate'] = pd.to_datetime(
            mutations['MutationDate'], format='%d.%m.%Y'
        )
        unique_dates = [
            date.strftime("%d-%m-%Y") for date in mutations['MutationDate'].unique()
        ]
        unique_dates.sort(key=lambda date: datetime.strptime(date, "%d-%m-%Y"))

        print(
            f"Found {len(unique_dates)} Gemeindestände since 01-01-1981!"
            f"Latest: {unique_dates[-1]}"
        )

        number_of_municipalities, codes_per_stand, snapshots = [], [], []
        for date in tqdm(unique_dates, desc='Updating Gemeindestände'):

            snapshot = pd.read_csv(f'{self.api_path}/snapshot?date={date}')
            snapshot = snapshot[snapshot.Level == 3]

            if which_data == 'code_mapping':
                number_of_municipalities.append(int(snapshot['BfsCode'].nunique()))
                codes_per_stand.append(
                    dict(zip(snapshot['BfsCode'].tolist(), snapshot['Name'].tolist()))
                )

            elif which_data == 'name_matching':
                snapshot['gmde_stand'] = date
                snapshots.append(snapshot)

        if which_data == 'code_mapping':
            data = pd.DataFrame({
                'gemeindestand': unique_dates,
                'anz_gmde': number_of_municipalities
            })
            gmde_dct = dict(zip(unique_dates, codes_per_stand))
            return data, gmde_dct

        elif which_data == 'name_matching':
            snapshots_combined = pd.concat(snapshots)
            snapshots_combined = (
                snapshots_combined
                .rename(columns={'Name': 'bfs_gmde_name', 'BfsCode': 'bfs_gmde_code'})
                .sort_values(['bfs_gmde_name', 'gmde_stand'])
                .drop_duplicates(subset=['bfs_gmde_name'], keep='last')
                .filter(['bfs_gmde_name', 'bfs_gmde_code', 'gmde_stand'])
            )

            ambiguous = (
                snapshots_combined[snapshots_combined['bfs_gmde_name'].str.contains(r'\(')]
                .sort_values('bfs_gmde_name')
            )
            ambiguous['bfs_gmde_name'] = ambiguous['bfs_gmde_name'].apply(lambda x: re.sub(r'\(.*?\)', '', x).strip())
            ambiguous['bfs_gmde_code'] = ambiguous['bfs_gmde_code'].astype(str)
            ambiguous_grouped = (
                ambiguous
                .groupby('bfs_gmde_name')
                .agg({
                    'bfs_gmde_code': lambda x: ', '.join(x.tolist()),
                    'gmde_stand': lambda x: ', '.join(x.tolist())
                })
                .reset_index()
            )

            official_names = pd.concat([snapshots_combined, ambiguous_grouped])

            try:
                common_names = pd.read_csv(self.base_path / 'common_names_updated_20-11-2024.csv')
                official_names = pd.concat([official_names, common_names])
            except FileNotFoundError as e:
                print(
                    "Common name variants file not found, proceeding without."
                    f"Output: {e}"
                )

            return official_names
