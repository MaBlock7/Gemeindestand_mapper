import asyncio
import aiohttp
from datetime import datetime
from pathlib import Path

import io
import pandas as pd

from utils.utils import COUNT_GMDE_COL, GMDE_STAND_COL

# Constants
CSV_FILE_PATH = Path('./utils/data/anzahl_gmde_pro_stand.csv')
PKL_FILE_PATH = Path('./utils/data/gemeindestaende.pkl')
TARGET_PATH = Path('../results')


class GemeindeMapper:
    def __init__(self):
        self.api_path = 'https://sms.bfs.admin.ch/WcfBFSSpecificService.svc/AnonymousRest/communes/'  # noqa
        self.data = self.load_data(CSV_FILE_PATH, 'csv')
        self.gmde_dct = self.load_data(PKL_FILE_PATH, 'pickle')

    @staticmethod
    def load_data(file_path, file_format):
        try:
            if file_format == 'csv':
                return pd.read_csv(file_path)
            elif file_format == 'pickle':
                return pd.read_pickle(file_path)
        except FileNotFoundError as e:
            message = f"{file_format} file not found at path: {file_path}"
            raise FileNotFoundError(message) from e
        except Exception as e:
            raise Exception(f"Error reading {file_format} file: {e}") from e

    @staticmethod
    def _check_gemeindestand(candidate: str, gemeinde_set: set) -> bool:
        date_obj = (
            datetime.strptime(candidate, '%d-%m-%Y').strftime('%Y-%m-%d')
        )
        fp = (
            CSV_FILE_PATH.parent / 'snapshots' /
            f'gemeindestand_{date_obj}.csv'
        )

        if not fp.exists():
            return False

        temp_data = pd.read_csv(fp, usecols=['Identifier'])
        unique_gemeinden = set(temp_data['Identifier'])

        # Check if all elements in the list are in the unique values set
        return all(elem in unique_gemeinden for elem in gemeinde_set)

    @staticmethod
    def _check_for_unpoulated_areas(gemeinde_set: set) -> list:
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

        return gemeinde_set

    def _infer_gemeindestand(self, gemeinde_set: set) -> str:
        """Tries to infer the Gemeindestand (state of municipalities)
           of a given column of a DataFrame.

        Parameters:
        -----------
        gemeinde_set: set
            The DataFrame for which to find its Gemeindestand.

        Returns:
        --------
        str
        """
        gemeindestand_lst = self.data[GMDE_STAND_COL].to_list()
        gemeindestand_lst.sort(
            key=lambda date: datetime.strptime(date, "%d-%m-%Y"))

        for gemeindestand in reversed(gemeindestand_lst):
            unique_gemeinden = set(self.gmde_dct[gemeindestand])
            if all(elem in unique_gemeinden for elem in gemeinde_set):
                print(f"Inferred Gemeindestand: {gemeindestand}")
                return gemeindestand

        raise ValueError("No Gemeindestand could be inferred :(")

    def find_gemeindestand(self, df: pd.DataFrame, column_name: str) -> str:
        """Tries to find the Gemeindestand (state of municipalities)
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
        gemeinde_set = set(df[column_name].unique())

        # Test if shared territories (Kummunanzen), lakes or
        # foreign territories are part of the DataFrame
        gemeinde_set = self._check_for_unpoulated_areas(gemeinde_set)
        num_gmde = len(gemeinde_set)

        candidates = (
            self.data[self.data[COUNT_GMDE_COL] == num_gmde][GMDE_STAND_COL]
            .values
        )

        # Search Gemeindestand by municipality count
        if candidates.size == 1 and self._check_gemeindestand(candidates[0],
                                                              gemeinde_set):
            print(f"Found Gemeindestand: {candidates[0]}.")
            return candidates[0]
        elif candidates.size > 1:
            for candidate in candidates:
                if self._check_gemeindestand(candidate, gemeinde_set):
                    print(f"Found Gemeindestand: {candidate}.")
                    return candidate

        # If no Gemeindestand was found: try to infer one
        try:
            return self._infer_gemeindestand(gemeinde_set)
        except ValueError as e:
            raise e

    async def fetch_mapping(self, session, origin: str, target: str) -> pd.DataFrame:
        """Fetches the municipality mapping data from the API for a given origin and target date.

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
        url = f"{self.api_path}correspondances?includeUnmodified=true&includeTerritoryExchange=false&startPeriod={origin}&endPeriod={target}"  # noqa
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.text()
            mapping = pd.read_csv(io.StringIO(data))
            return mapping

    async def create_mapping(self, origin: str, target: str, export: bool = False) -> pd.DataFrame:
        """Creates a municipality mapping between two dates and optionally exports the result to an Excel file.

        Parameters
        ----------
        origin : str
            The starting date of the mapping period in `dd-mm-YYYY` format.
        target : str
            The ending date of the mapping period in `dd-mm-YYYY` format.
        export : bool, optional
            If True, the resulting DataFrame is exported to an Excel file (default is False).

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the municipality mapping data.
        """
        async with aiohttp.ClientSession() as session:
            mapping = await self.fetch_mapping(session, origin, target)
            if export:
                mapping.to_excel(TARGET_PATH / f'mapping_{origin}_{target}.xlsx', index=False)
            return mapping

    async def create_multi_mapping(self, origin: str, targets: list[str], return_names: bool = True) -> pd.DataFrame:
        """Creates a mapping for multiple target dates starting from a given origin date, merging the results.

        Parameters
        ----------
        origin : str
            The starting date of the mapping period in `dd-mm-YYYY` format.
        targets : list of str
            A list of target dates in `dd-mm-YYYY` format for which to create the mapping.
        return_names : bool, optional
            If True, the returned DataFrame includes the municipality names. If False, the name columns are dropped (default is True).

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the merged municipality mappings for all target dates.
        """
        print("Creating the mapping. This might take some time...")
        targets.append(origin)
        date_strings = sorted([datetime.strptime(date, "%d-%m-%Y") for date in set(targets)])
        earliest_date = date_strings[0]
        targets = date_strings[1:]

        tasks = []
        connector = aiohttp.TCPConnector(limit=20)  # Increase the limit to allow more concurrent connections
        async with aiohttp.ClientSession(connector=connector) as session:
            for target in targets:
                tasks.append(self.fetch_mapping(session, earliest_date.strftime("%d-%m-%Y"), target.strftime("%d-%m-%Y")))

            mappings = await asyncio.gather(*tasks)

        # Process the mappings as before
        full_mapping = None
        for i, mapping in enumerate(mappings):
            target = targets[i].strftime("%d-%m-%Y")
            mapping = mapping[['InitialCode', 'InitialName', 'TerminalCode', 'TerminalName']]
            mapping.columns = [
                f'bfs_gmde_nummer_{earliest_date.strftime("%d-%m-%Y")}',
                f'bfs_gmde_name_{earliest_date.strftime("%d-%m-%Y")}',
                f'bfs_gmde_nummer_{target}',
                f'bfs_gmde_name_{target}'
            ]

            if full_mapping is None:
                full_mapping = mapping
            else:
                full_mapping = full_mapping.merge(
                    mapping,
                    left_on=[f'bfs_gmde_nummer_{earliest_date.strftime("%d-%m-%Y")}', f'bfs_gmde_name_{earliest_date.strftime("%d-%m-%Y")}'],
                    right_on=[f'bfs_gmde_nummer_{earliest_date.strftime("%d-%m-%Y")}', f'bfs_gmde_name_{earliest_date.strftime("%d-%m-%Y")}'],
                    how='left'
                )

            earliest_date = targets[i]

        if not return_names:
            return full_mapping.drop(columns=[col for col in full_mapping.columns if 'name' in mapping])
        return full_mapping

    def map_gemeindestand(self, df: pd.DataFrame,
                          column_name: str, **kwargs: str) -> pd.DataFrame:
        """Maps the old (origin) Gemeindestand to a newer one (target)
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
            target: str (default='01-01-2023')
                The target Gemeindestand to add to the DataFrame

        Returns:
        --------
        pd.DataFrame
        """
        newest_gemeindestand = self.data.iat[-1, 0]

        origin = kwargs.get('origin', False)
        target = kwargs.get('target', newest_gemeindestand)

        if not isinstance(origin, str):
            origin = self.find_gemeindestand(df, column_name)

        mapping = self.create_mapping(origin, target)
        mapping = mapping.rename(columns={'InitialCode': column_name,
                                          'TerminalCode': f'gmde_stand_{target}',
                                          'TerminalName': f'gmde_name_{target}'})

        print(f"Mapped DataFrame from {origin} to {target} Gemeindestand.")
        return df.merge(
            mapping[[column_name, f'gmde_stand_{target}', f'gmde_name_{target}']],
            on=column_name,
            how='outer'
        )


"""
def create_mapping(self, origin: str,
                    target: str, export: bool = False) -> pd.DataFrame:
    url = f"{API_PATH}correspondances?includeUnmodified=true&includeTerritoryExchange=false&startPeriod={origin}&endPeriod={target}"  # noqa
    mapping = self.load_data(url, 'csv')
    if export:
        mapping.to_excel(TARGET_PATH / f'mapping_{origin}_{target}.xlsx', index=False)
    return mapping

def create_multi_mapping(self, origin: str, targets: list[str]) -> pd.DataFrame:
    # Ensure origin is included in the targets list for consistent processing
    targets.append(origin)

    # Convert strings to datetime objects and sort the dates
    date_strings = sorted([datetime.strptime(date, "%d-%m-%Y") for date in set(targets)])

    # Initialize the earliest date and the remaining targets
    earliest_date = date_strings[0]
    targets = date_strings[1:]

    # Initialize an empty DataFrame to store the final mapping
    full_mapping = pd.DataFrame()

    for target in targets:
        # Create a mapping for each date pair
        temp_mapping = self.create_mapping(earliest_date.strftime("%d-%m-%Y"), target.strftime("%d-%m-%Y"), export=False)

        # Select and rename the necessary columns
        temp_mapping = temp_mapping[['InitialCode', 'InitialName', 'TerminalCode', 'TerminalName']]
        temp_mapping.columns = [f'bfs_gmde_nummer_{earliest_date.strftime("%d-%m-%Y")}', 
                                f'bfs_gmde_name_{earliest_date.strftime("%d-%m-%Y")}', 
                                f'bfs_gmde_nummer_{target.strftime("%d-%m-%Y")}', 
                                f'bfs_gmde_name_{target.strftime("%d-%m-%Y")}']

        if full_mapping.empty:
            # If the full mapping is empty, start with the first temp_mapping
            full_mapping = temp_mapping
        else:
            # Merge the current temp_mapping with the full mapping
            full_mapping = full_mapping.merge(temp_mapping, 
                                            left_on=[f'bfs_gmde_nummer_{earliest_date.strftime("%d-%m-%Y")}', f'bfs_gmde_name_{earliest_date.strftime("%d-%m-%Y")}'], 
                                            right_on=[f'bfs_gmde_nummer_{earliest_date.strftime("%d-%m-%Y")}', f'bfs_gmde_name_{earliest_date.strftime("%d-%m-%Y")}'],
                                            how='left')

        # Update the earliest date to the current target for the next iteration
        earliest_date = target

    return full_mapping
"""