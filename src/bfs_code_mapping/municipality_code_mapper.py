import asyncio
import aiohttp
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from typing import Union

import io
import pandas as pd

# Constants
TARGET_PATH = Path('../results')


class MunicipalityCodeMapper:
    """
    A class for managing and mapping Swiss municipality (Gemeinde) data.

    Attributes
    ----------
    api_path : str
        The base URL for the BFS (Federal Statistical Office) API.
    data : pd.DataFrame
        A DataFrame containing all official Gemeindestände (municipality states).
    gmde_dct : dict
        A dictionary mapping Gemeindestände to sets of BFS municipality codes.
    """
    def __init__(self):
        """
        Initialize the GemeindeMapper by fetching all official Gemeindestände.

        Raises
        ------
        Exception
            If the API fails to fetch data.
        """
        self.api_path = 'https://www.agvchapp.bfs.admin.ch/api/communes'  # noqa
        self.data, self.gmde_dct = self._get_all_gemeindestände()
        self.all_historical_bfs_codes = set()
        for name_dct in self.gmde_dct.values():
            self.all_historical_bfs_codes.update(name_dct.keys())

    @staticmethod
    def _check_for_unpoulated_areas(gemeinde_set: set[int]) -> list[int]:
        """
        Remove shared territories and lakes from the municipality set.

        Parameters
        ----------
        gemeinde_set : set of int
            A set of BFS municipality codes.

        Returns
        -------
        list of int
            The filtered municipality set.
        """
        gmde_freie_geb = {
            2285: "Staatswald Galm",
            2391: "Staatswald Galm",
            5020: "C'za Medeglia/Robasacco",
            5391: "C'za Cadenazzo/Monteceneri",
            5238: "C'za Corticiasca/Valcolla",
            5394: "C'za Capriasca/Lugano",
            6072: "Kommunanz Gluringen-Ritzingen",
            6391: "Kommunanz Reckingen-Gluringen/Grafschaft",
        }

        if any(i in gemeinde_set for i in gmde_freie_geb.keys()):
            removed_territories = [n for n in gemeinde_set
                                   if n in gmde_freie_geb.keys()]
            print(f"""Found territory shared by several municipalities of Switzerland (Kommunanz)!
                  Temporarily removing {removed_territories} for Gemeindestands search...""")  # noqa
            gemeinde_set = [n for n in gemeinde_set
                            if n not in gmde_freie_geb.keys()]

        # Lakes have bfs-numbers >= 9000, foreign area >= 7000
        if any(i >= 7000 for i in gemeinde_set):
            removed_territories = [n for n in gemeinde_set if n >= 7000]
            print(f"""Found lake (kant. Seeareal) or foreign territory!
                  Temporarily removing {removed_territories} for Gemeindestands search...""")  # noqa
            gemeinde_set = [n for n in gemeinde_set if n < 7000]

        return set(gemeinde_set)

    @staticmethod
    def _nearest(official_gemeindestaende: list[datetime], target: datetime, prefer: str = 'nearest') -> str:
        """
        Find the nearest official Gemeindestand to the given date.

        Parameters
        ----------
        official_gemeindestaende : list of datetime
            List of official Gemeindestände as datetime objects.
        target : datetime
            The target date to find the closest match.
        prefer : str, optional
            Preference for the closest date. Options:
            - "nearest" (default): Return the closest date.
            - "smaller": Return the closest smaller date.
            - "larger": Return the closest larger date.

        Raises
        ------
        ValueError
            If no matching date is found based on the preference.
        """
        if prefer == "nearest":
            closest = min(official_gemeindestaende, key=lambda x: abs(x - target))
        elif prefer == "smaller":
            smaller_dates = [date for date in official_gemeindestaende if date <= target]
            if not smaller_dates:
                raise ValueError("No official Gemeindestand found that is smaller than the target date.")
            closest = max(smaller_dates)
        elif prefer == "larger":
            larger_dates = [date for date in official_gemeindestaende if date >= target]
            if not larger_dates:
                raise ValueError("No official Gemeindestand found that is larger than the target date.")
            closest = min(larger_dates)
        else:
            raise ValueError("Invalid 'prefer' value. Use 'nearest', 'smaller', or 'larger'.")

        return closest.strftime("%d-%m-%Y")

    def _find_closest_official_gemeindestand(self, given_date: str, prefer: str = 'nearest') -> str:
        """
        Find the closest official Gemeindestand to the given date.

        Parameters
        ----------
        given_date : str
            The target date in `dd-mm-YYYY` format.
        prefer : str, optional
            Preference for the closest date. Options:
            - "nearest" (default): Return the closest date.
            - "smaller": Return the closest smaller date.
            - "larger": Return the closest larger date.

        Returns
        -------
        str
            The closest official Gemeindestand in `dd-mm-YYYY` format.

        Raises
        ------
        ValueError
            If no official Gemeindestand is found.

        Raises
        ------
        ValueError
            If no official Gemeindestand is found based on the preference.
        """
        if given_date in self.data.gemeindestand.tolist():
            return given_date
        else:
            official_gemeindestaende_datetime = [datetime.strptime(date, "%d-%m-%Y") for date in self.data.gemeindestand]
            given_date_datetime = datetime.strptime(given_date, "%d-%m-%Y")
            nearest_gemeindestand = self._nearest(official_gemeindestaende_datetime, given_date_datetime, prefer)
            print(f"Date {given_date} does not correspond to official Gemeindestand, using closest: {nearest_gemeindestand}.")
            return nearest_gemeindestand

    def _get_all_gemeindestände(self):
        """
        Fetch all official Gemeindestände from the BFS API.

        Returns
        -------
        tuple of (pd.DataFrame, dict)
            A DataFrame containing Gemeindestände and a dictionary mapping Gemeindestände to BFS codes.
        """
        today = datetime.today().strftime('%d-%m-%Y')
        url = f'{self.api_path}/mutations?includeTerritoryExchange=false&startPeriod=01-01-1981&endPeriod={today}'
        mutations = pd.read_csv(url)
        mutations['MutationDate'] = pd.to_datetime(mutations['MutationDate'], format='%d.%m.%Y')
        unique_dates = [date.strftime("%d-%m-%Y") for date in mutations['MutationDate'].unique()]
        unique_dates.sort(
            key=lambda date: datetime.strptime(date, "%d-%m-%Y"))
        print(f"Found {len(unique_dates)} Gemeindestände since 01-01-1981! Latest: {unique_dates[-1]}")
        number_of_municipalities, codes_per_stand = [], []
        for date in tqdm(unique_dates, desc='Updating Gemeindestände'):
            snapshots = pd.read_csv(f'{self.api_path}/snapshot?date={date}')
            snapshots = snapshots[snapshots.Level == 3]
            number_of_municipalities.append(int(snapshots['BfsCode'].nunique()))
            codes_per_stand.append(
                dict(zip(snapshots['BfsCode'].tolist(), snapshots['Name'].tolist()))
            )

        data = pd.DataFrame({'gemeindestand': unique_dates, 'anz_gmde': number_of_municipalities})
        gmde_dct = dict(zip(unique_dates, codes_per_stand))

        return data, gmde_dct

    def _check_gemeindestand(self, candidate: str, gemeinde_set: set[str]) -> bool:
        """
        Check if a potential Gemeindestand includes all BFS Gemeinde Codes 
        of the data to be mapped.

        Parameters
        ----------
        candidate : str
            The date of the potential Gemeindestand in `dd-mm-YYYY` format.
        gemeide_set : set[int]
            A set of all the BFS codes found in the data to be mapped.

        Returns
        -------
        bool
            True if all the codes are included in the candidate Gemeindestand.
        """
        # Check if all municipalities are part of the gemeindestand candidate
        unique_gemeinden = set(self.gmde_dct.get(candidate, {}).keys())
        return bool(not gemeinde_set - unique_gemeinden)

    def _check_for_non_bfs_codes(self, df: pd.DataFrame, code_column: str, name_column: str, gemeinde_set: set[str]) -> tuple[set[str]]:
        wrong_codes = gemeinde_set - self.all_historical_bfs_codes
        if len(wrong_codes) > 0:
            print(f'Non-BFS codes detected! Removing {set(int(code) for code in wrong_codes)}')
            gemeinde_set -= wrong_codes
            wrong_codes_dict = {}
            for code in wrong_codes:
                wrong_codes_dict[int(code)] = df.loc[df[code_column] == code, name_column].values[0]
        return gemeinde_set, wrong_codes_dict

    def _correct_wrong_codes(self, inferred_gmde_stand: str, wrong_codes_dict: dict):
        correct_dict = {v: k for k, v in self.gmde_dct.get(inferred_gmde_stand, {}).items()}
        wrong_dict = {v: k for k, v in wrong_codes_dict.items()}
        return {wrong_dict[name]: correct_dict.get(name, wrong_dict[name]) for name in wrong_dict.keys()}

    def _infer_gemeindestand(self, gemeinde_set: set[int]) -> str:
        """
        Tries to infer the Gemeindestand (state of municipalities)
        of a given column of a DataFrame.

        Parameters:
        -----------
        gemeinde_set: set
            The DataFrame for which to find its Gemeindestand.

        Returns:
        --------
        str
        """
        gemeindestaende = self.data['gemeindestand'].tolist()
        for gemeindestand in reversed(gemeindestaende):
            unique_gemeinden = set(self.gmde_dct.get(gemeindestand, {}).keys())
            if bool(not gemeinde_set - unique_gemeinden):
                print(f"Inferred Gemeindestand: {gemeindestand}")
                return gemeindestand

        raise ValueError("No Gemeindestand could be inferred :(")

    def find_gemeindestand(self, df: pd.DataFrame, code_column: str, name_column: str) -> str:
        """
        Tries to find the Gemeindestand (state of municipalities)
        of a given column of a DataFrame.

        Parameters:
        -----------
        df: pd.DataFrame
            The DataFrame for which to find its Gemeindestand.
        column_name: str
            The name of the column that contains the municipality codes.

        Returns:
        --------
        str
        """
        gemeinde_set = set(df[code_column].unique())

        # Test if shared territories (Kummunanzen), lakes or
        # foreign territories are part of the DataFrame
        gemeinde_set = self._check_for_unpoulated_areas(gemeinde_set)
        num_gmde = len(gemeinde_set)

        # Remove codes that have never been used by BFS
        gemeinde_set, wrong_codes_dict = self._check_for_non_bfs_codes(df, code_column, name_column, gemeinde_set)

        candidates = (
            self.data[self.data['anz_gmde'] == num_gmde]['gemeindestand']
            .values
        )

        # Search Gemeindestand by municipality count
        if candidates.size == 1 and self._check_gemeindestand(candidates[0],
                                                              gemeinde_set):
            print(f"Found Gemeindestand: {candidates[0]}.")
            return candidates[0], None
        elif candidates.size > 1:
            for candidate in candidates:
                if self._check_gemeindestand(candidate, gemeinde_set):
                    print(f"Found Gemeindestand: {candidate}.")
                    return candidate, None

        # If no Gemeindestand was found: try to infer one
        try:
            candidate = self._infer_gemeindestand(gemeinde_set)
            corrections_dict = self._correct_wrong_codes(candidate, wrong_codes_dict)
            return candidate, corrections_dict
        except ValueError as e:
            raise e

    async def fetch_mapping(self, session, origin: str, target: str) -> pd.DataFrame:
        """
        Fetches the municipality mapping data from the API for a given origin and target date.

        Parameters
        ----------
        session : aiohttp.ClientSession
            An instance of `aiohttp.ClientSession` used to perform the HTTP request.
        origin : str
            The starting date of the mapping period in `dd-mm-YYYY` format.
        target : str
            The ending date of the mapping period in `dd-mm-YYYY` format.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the municipality mapping data.
        """
        url = f"{self.api_path}/correspondances?includeUnmodified=true&includeTerritoryExchange=false&startPeriod={origin}&endPeriod={target}"  # noqa
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.text()
            mapping = pd.read_csv(io.StringIO(data))
            return mapping

    async def create_mapping(self, origin: str, target: str, export: bool = False, output_path: str = './mappings') -> pd.DataFrame:
        """
        Creates a municipality mapping between two dates and optionally exports the result to an Excel file.

        Parameters
        ----------
        origin : str
            The starting date of the mapping period in `dd-mm-YYYY` format.
        target : str
            The ending date of the mapping period in `dd-mm-YYYY` format.
        export : bool, optional
            If True, the resulting DataFrame is exported to an Excel file (default is False).
        output_path : str
            Path to store the mappings.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the municipality mapping data.
        """

        origin = self._find_closest_official_gemeindestand(origin, prefer='smaller')
        target = self._find_closest_official_gemeindestand(target)

        async with aiohttp.ClientSession() as session:
            mapping = await self.fetch_mapping(session, origin, target)
            if export:
                Path(output_path).mkdir(parents=True, exist_ok=True)
                mapping.to_excel(output_path / f'mapping_{origin}_{target}.xlsx', index=False)
            return mapping

    async def create_multi_mapping(self, origin: str, targets: Union[list[str], tuple[str]],
                                   return_names: bool = True) -> pd.DataFrame:
        """
        Creates a mapping for multiple target dates starting from a given
        origin date, merging the results.

        Parameters
        ----------
        origin : str
            The starting date of the mapping period in `dd-mm-YYYY` format.
        targets : list of str or tuple of str
            A list of target dates in `dd-mm-YYYY` format for which to create the mapping or
            a tuple of two dates (start, end) - this will create mapping to all Gemeindestände
            between the two including start and end.
        return_names : bool, optional
            If True, the returned DataFrame includes the municipality names.
            If False, the name columns are dropped (default is True).

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the merged municipality mappings for all target dates.
        """
        print("Creating the mapping. This might take some time...")

        origin = self._find_closest_official_gemeindestand(origin, prefer='smaller')
        if isinstance(targets, tuple):
            assert len(targets) == 2, "Your date range should have exactly two values!"
            start = self._find_closest_official_gemeindestand(targets[0], prefer='smaller')
            end = self._find_closest_official_gemeindestand(targets[1], prefer='larger')
            gemeindestaende = self.data.gemeindestand.to_list()
            start_index, end_index = gemeindestaende.index(start), gemeindestaende.index(end) + 1
            targets = gemeindestaende[start_index:end_index]
        elif isinstance(targets, list):
            for i, target in enumerate(targets):
                targets[i] = self._find_closest_official_gemeindestand(target)
        else:
            raise TypeError('test test')

        targets.append(origin)
        date_strings = sorted([datetime.strptime(date, "%d-%m-%Y") for date in set(targets)])
        earliest_date = date_strings[0]
        targets = date_strings[1:]

        tasks = []
        connector = aiohttp.TCPConnector(limit=20)  # Increase the limit to allow more concurrent connections
        async with aiohttp.ClientSession(connector=connector) as session:
            start_date = earliest_date.strftime("%d-%m-%Y")
            for target in targets:
                target = target.strftime("%d-%m-%Y")
                tasks.append(
                    self.fetch_mapping(session, start_date, target)
                )
                start_date = target

            mappings = await asyncio.gather(*tasks)

        # Process the mappings as before
        full_mapping = None
        start_date = earliest_date.strftime("%d-%m-%Y")
        for i, mapping in enumerate(mappings):
            target = targets[i].strftime("%d-%m-%Y")

            mapping = mapping[['InitialCode', 'InitialName', 'TerminalCode', 'TerminalName']]
            mapping.columns = [
                f'bfs_gmde_nummer_{start_date}',
                f'bfs_gmde_name_{start_date}',
                f'bfs_gmde_nummer_{target}',
                f'bfs_gmde_name_{target}'
            ]

            if full_mapping is None:
                full_mapping = mapping
            else:
                full_mapping = full_mapping.merge(
                    mapping,
                    on=[f'bfs_gmde_nummer_{start_date}', f'bfs_gmde_name_{start_date}'],
                    how='left'
                )

            start_date = target

        if not return_names:
            return full_mapping.drop(columns=[col for col in full_mapping.columns if 'name' in col])
        return full_mapping

    async def map_dataframe(self, df: pd.DataFrame, code_column: str, name_column: str,
                            **kwargs: Union[str, list[str], tuple[str]]) -> pd.DataFrame:
        """
        Maps the old (origin) Gemeindestand to a newer one (target)
        by adding a new column to a given DataFrame.

        Parameters:
        -----------
        df: pd.DataFrame
            The DataFrame for which to find its Gemeindestand.

        column_name: str
            The name of the column that contains the municipality codes.

        kwargs:
            origin: str (default=False)
                If known the original Gemeindestand can be passed as an
                argument, otherwise the functen tries to infer one.
            target: str (default=<newest gemeindestand>), list[str], tuple[str]
                A target Gemeindestand to add to the DataFrame as a string.
                A list of target dates in `dd-mm-YYYY` format for which to create the mapping.
                A tuple of two dates (start, end) - this will create mapping to all Gemeindestände
                    between the two including start and end.
            return_names : bool, optional
                If True, the returned DataFrame includes the municipality names.
                If False, the name columns are dropped (default is True).

        Returns:
        --------
        pd.DataFrame
        """
        newest_gemeindestand = self.data.iat[-1, 0]

        origin = kwargs.get('origin', False)
        target = kwargs.get('target', newest_gemeindestand)
        return_names = kwargs.get('return_names', False)

        if isinstance(origin, str) and (set(df[code_column].unique()) - self.all_historical_bfs_codes):
            gemeinde_set = set(df[code_column].unique())
            gemeinde_set, wrong_codes_dict = self._check_for_non_bfs_codes(df, code_column, name_column, gemeinde_set)
            corrections_dict = self._correct_wrong_codes(origin, wrong_codes_dict)
            print(f'Found the following corrections: {corrections_dict}')
            df[code_column] = df[code_column].replace(corrections_dict)

        if not isinstance(origin, str):
            origin, corrections_dict = self.find_gemeindestand(df, code_column, name_column)
            if corrections_dict:
                df[code_column] = df[code_column].replace(corrections_dict)

        if isinstance(target, (tuple, list)):
            mapping = await self.create_multi_mapping(origin, target, return_names)
        else:
            mapping = await self.create_mapping(origin, target, return_names)

        df = df.rename(columns={code_column: f'bfs_gmde_nummer_{origin}'})

        print(f"Mapped DataFrame from {origin} to {target} Gemeindestand.")
        return df.merge(
            mapping,
            on=[f'bfs_gmde_nummer_{origin}'],
            how='left'
        )

    async def map_multiple_gemeindestaende_to_newest(self, df: pd.DataFrame, code_column, stand_column):
        """
        """
        gemeindestaende = df[~df[stand_column].isna()][stand_column].unique()
        date_strings = sorted([date for date in gemeindestaende if ', ' not in date], key=lambda x: datetime.strptime(x, "%d-%m-%Y"))
        newest = date_strings[-1]

        df[f'bfs_gmde_nummer_{newest}'] = pd.NA

        origins = date_strings[:-1]

        connector = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.fetch_mapping(session, origin, newest) for origin in origins]
            mappings = await asyncio.gather(*tasks)

        for origin, mapping in zip(origins, mappings):
            mapping = mapping[['InitialCode', 'TerminalCode']].copy()
            mapping.columns = [
                code_column,
                f'bfs_gmde_nummer_{newest}_update'
            ]
            mapping[stand_column] = origin
            mapping[code_column] = mapping[code_column].astype(str)
            mapping[f'bfs_gmde_nummer_{newest}_update'] = mapping[f'bfs_gmde_nummer_{newest}_update'].astype(str)

            df = df.merge(mapping, on=[stand_column, code_column], how='left')
            df[f'bfs_gmde_nummer_{newest}'] = df[f'bfs_gmde_nummer_{newest}'].fillna(df.pop(f'bfs_gmde_nummer_{newest}_update'))

        df.loc[df[stand_column] == newest, f'bfs_gmde_nummer_{newest}'] = df[code_column]

        return df
