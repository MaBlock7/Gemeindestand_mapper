import asyncio
import aiohttp
from datetime import datetime
from pathlib import Path
from typing import Union

import io
import pandas as pd

from .base_class import BaseMunicipalityData


class MunicipalityCodeMapper(BaseMunicipalityData):
    """
    A class for managing and mapping Swiss municipality (Gemeinde) data.

    :ivar api_path: The base URL for the BFS (Federal Statistical Office) API.
    :ivar data: A DataFrame containing all official Gemeindestände (municipality states).
    :ivar gmde_dct: A dictionary mapping Gemeindestände to sets of BFS municipality codes.
    """
    def __init__(self, start_date: str = '01-01-1981'):
        """
        Initialize the GemeindeMapper by fetching all official Gemeindestände.

        :param start_date: The start date to fetch all gemeindestände for.
        :type start_date: str
        :raises Exception: If the API fails to fetch data.
        """
        super().__init__(which_data='code_mapping', start_date=start_date)

    @staticmethod
    def _check_for_unpoulated_areas(gemeinde_set: set[int]) -> list[int]:
        """
        Remove shared territories and lakes from the municipality set.

        This method identifies and removes BFS codes corresponding to shared territories,
        lakes, and foreign areas that are not part of individual Swiss municipalities.

        :param gemeinde_set: A set of BFS municipality codes.
        :type gemeinde_set: set[int]
        :return: The filtered municipality set.
        :rtype: list[int]
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

        # Remove shared territories
        shared_territories = {int(code) for code in gemeinde_set if code in gmde_freie_geb}
        if shared_territories:
            print(
                f"Found shared territories (Kommunanz): {shared_territories}. "
                f"Temporarily removing them for Gemeindestands search..."
            )
            gemeinde_set -= shared_territories

        # Lakes have bfs-numbers >= 9000, foreign area >= 7000
        non_municipal_areas = {int(code) for code in gemeinde_set if code >= 7000}
        if non_municipal_areas:
            print(
                f"Found lakes or foreign territories: {non_municipal_areas}. "
                f"Temporarily removing them for Gemeindestands search..."
            )
            gemeinde_set -= non_municipal_areas

        return gemeinde_set

    def _check_gemeindestand(
        self, candidate: str, gemeinde_set: set[str]
    ) -> bool:
        """
        Check if a potential Gemeindestand includes all BFS Gemeinde Codes of the data to be mapped.

        :param candidate: The date of the potential Gemeindestand in "dd-mm-YYYY" format.
        :type candidate: str
        :param gemeinde_set: A set of all the BFS codes found in the data to be mapped.
        :type gemeinde_set: set[int]
        :return: True if all the codes are included in the candidate Gemeindestand.
        :rtype: bool
        """
        unique_gemeinden = set(self.gmde_dct.get(candidate, {}).keys())
        return bool(not gemeinde_set - unique_gemeinden)

    def _check_for_non_bfs_codes(
        self,
        df: pd.DataFrame,
        code_column: str,
        name_column: str,
        gemeinde_set: set[str]
    ) -> tuple[set[str]]:
        """
        Check for non-BFS codes in the given dataset and remove them.

        :param df: The DataFrame containing the municipality data.
        :type df: pd.DataFrame
        :param code_column: The column containing BFS codes.
        :type code_column: str
        :param name_column: The column containing municipality names.
        :type name_column: str
        :param gemeinde_set: A set of BFS municipality codes.
        :type gemeinde_set: set[str]
        :return: A tuple containing the cleaned gemeinde_set and a dictionary of wrong codes.
        :rtype: tuple[set[str], dict]
        """
        wrong_codes = gemeinde_set - self.all_historical_bfs_codes
        if len(wrong_codes) > 0:
            print(
                f"Non-BFS codes detected! "
                f"Removing {set(int(code) for code in wrong_codes)}"
            )
            gemeinde_set -= wrong_codes
            wrong_codes_dict = {}
            for code in wrong_codes:
                wrong_codes_dict[int(code)] = (
                    df.loc[df[code_column] == code, name_column].values[0]
                )
        return gemeinde_set, wrong_codes_dict

    def _correct_wrong_codes(
        self, inferred_gmde_stand: str, wrong_codes_dict: dict
    ) -> dict[int, int]:
        """
        Correct wrong municipality codes using the inferred Gemeindestand.

        :param inferred_gmde_stand: The inferred Gemeindestand date.
        :type inferred_gmde_stand: str
        :param wrong_codes_dict: A dictionary of wrong municipality codes.
        :type wrong_codes_dict: dict
        :return: A dictionary mapping incorrect codes to corrected codes.
        :rtype: dict
        """
        correct_dict = {v: k for k, v in self.gmde_dct.get(inferred_gmde_stand, {}).items()}
        wrong_dict = {v: k for k, v in wrong_codes_dict.items()}
        return {wrong_dict[name]: correct_dict.get(name, wrong_dict[name]) for name in wrong_dict.keys()}

    def _nearest(
        self, official_gemeindestaende: list[datetime], target: datetime, prefer: str = "nearest"
    ) -> str:
        """
        Find the nearest official Gemeindestand to the given date.

        This method identifies the closest date from a list of official Gemeindestände
        based on the given target date and preference. If no match is found for "smaller"
        or "larger," it falls back to "nearest."

        :param official_gemeindestaende: List of official Gemeindestände as datetime objects.
        :type official_gemeindestaende: list[datetime]
        :param target: The target date to find the closest match.
        :type target: datetime
        :param prefer: Preference for the closest date. Options:
                    - "nearest" (default): Return the closest date.
                    - "smaller": Return the closest smaller date or fallback to "nearest."
                    - "larger": Return the closest larger date or fallback to "nearest."
        :type prefer: str
        :return: The closest official Gemeindestand in "dd-mm-YYYY" format.
        :rtype: str
        """
        if prefer == "nearest":
            closest = min(
                official_gemeindestaende, key=lambda date: abs(date - target)
            )
        elif prefer == "smaller":
            smaller_dates = [
                date for date in official_gemeindestaende if date <= target
            ]
            closest = max(smaller_dates, default=None)
            if closest is None:  # Fallback to nearest
                print("No smaller date found. Falling back to nearest.")
                return self._nearest(official_gemeindestaende, target, "nearest")
        elif prefer == "larger":
            larger_dates = [
                date for date in official_gemeindestaende if date >= target
            ]
            closest = min(larger_dates, default=None)
            if closest is None:  # Fallback to nearest
                print("No larger date found. Falling back to nearest.")
                return self._nearest(official_gemeindestaende, target, "nearest")
        else:
            raise ValueError(
                "Invalid 'prefer' value. Use 'nearest', 'smaller', or 'larger'."
            )

        return closest.strftime("%d-%m-%Y")

    def _find_closest_official_gemeindestand(
        self, given_date: str, prefer: str = 'nearest'
    ) -> str:
        """
        Find the closest official Gemeindestand to the given date.

        :param given_date: The target date in "dd-mm-YYYY" format.
        :type given_date: str
        :param prefer: Preference for the closest date. Options:
                       - "nearest" (default): Return the closest date.
                       - "smaller": Return the closest smaller date.
                       - "larger": Return the closest larger date.
        :type prefer: str
        :return: The closest official Gemeindestand in "dd-mm-YYYY" format.
        :rtype: str
        :raises ValueError: If no official Gemeindestand is found.
        """
        if given_date in self.data.gemeindestand.tolist():
            return given_date

        # Convert dates to datetime objects for comparison
        official_gemeindestaende_datetime = [
            datetime.strptime(date, "%d-%m-%Y") for date in self.data['gemeindestand']
        ]
        given_date_datetime = datetime.strptime(given_date, "%d-%m-%Y")

        # Find the nearest official Gemeindestand
        nearest_gemeindestand = self._nearest(
            official_gemeindestaende_datetime, given_date_datetime, prefer
        )

        print(
            f"Date {given_date} does not correspond to an official Gemeindestand. "
            f"Using the closest: {nearest_gemeindestand}."
        )
        return nearest_gemeindestand

    def _infer_gemeindestand(self, gemeinde_set: set[int]) -> str:
        """
        Infer the Gemeindestand of a given set of municipality codes.

        :param gemeinde_set: A set of BFS municipality codes.
        :type gemeinde_set: set[int]
        :return: The inferred Gemeindestand date.
        :rtype: str
        :raises ValueError: If no Gemeindestand could be inferred.
        """
        gemeindestaende = self.data['gemeindestand'].tolist()
        for gemeindestand in reversed(gemeindestaende):
            unique_gemeinden = set(self.gmde_dct.get(gemeindestand, {}).keys())
            if bool(not gemeinde_set - unique_gemeinden):
                print(f"Inferred Gemeindestand: {gemeindestand}")
                return gemeindestand
        raise ValueError

    def find_gemeindestand(
        self, df: pd.DataFrame, code_column: str, name_column: str
    ) -> tuple[str, Union[dict, None]]:
        """
        Find the Gemeindestand of a given DataFrame.

        :param df: The DataFrame containing municipality data.
        :type df: pd.DataFrame
        :param code_column: The column containing BFS codes.
        :type code_column: str
        :param name_column: The column containing municipality names.
        :type name_column: str
        :return: The Gemeindestand date.
        :rtype: tuple[str, Union[dict, None]]
        :raises ValueError: If no Gemeindestand could be found or inferred.
        """
        gemeinde_set = set(df[code_column].unique())

        # Remove shared territories, lakes, and foreign territories
        gemeinde_set = self._check_for_unpoulated_areas(gemeinde_set)
        num_gmde = len(gemeinde_set)

        # Remove codes that have never been used by BFS
        gemeinde_set, wrong_codes_dict = self._check_for_non_bfs_codes(
            df, code_column, name_column, gemeinde_set
        )

        candidates = (
            self.data[self.data['anz_gmde'] == num_gmde]['gemeindestand'].values
        )

        # Search Gemeindestand by municipality count
        if candidates.size == 1 and self._check_gemeindestand(candidates[0], gemeinde_set):
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
            raise ValueError(
                "No Gemeindestand could be found or inferred. Please check manually."
            ) from e

    async def _fetch_mapping(
        self, session: aiohttp.ClientSession, origin: str, target: str
    ) -> pd.DataFrame:
        """
        Fetch the municipality mapping data from the API for a given origin and target date.

        :param session: An instance of `aiohttp.ClientSession` used to perform the HTTP request.
        :type session: aiohttp.ClientSession
        :param origin: The starting date of the mapping period in "dd-mm-YYYY" format.
        :type origin: str
        :param target: The ending date of the mapping period in "dd-mm-YYYY" format.
        :type target: str
        :return: A pandas DataFrame containing the municipality mapping data.
        :rtype: pd.DataFrame
        """
        url = f"{self.api_path}/correspondances?includeUnmodified=true&includeTerritoryExchange=false&startPeriod={origin}&endPeriod={target}"  # noqa
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.text()
            return pd.read_csv(io.StringIO(data))

    async def create_mapping(
        self,
        origin: str,
        target: str,
        export_path: Union[str, None] = None,
    ) -> pd.DataFrame:
        """
        Create a municipality mapping between two dates and optionally export the result to an Excel file.

        :param origin: The starting date of the mapping period in "dd-mm-YYYY" format.
        :type origin: str
        :param target: The ending date of the mapping period in "dd-mm-YYYY" format.
        :type target: str
        :param export: If True, the resulting DataFrame is exported to an Excel file (default is False).
        :type export: bool, optional
        :return: A pandas DataFrame containing the municipality mapping data.
        :rtype: pd.DataFrame
        """
        origin = self._find_closest_official_gemeindestand(origin, prefer='smaller')
        target = self._find_closest_official_gemeindestand(target)

        async with aiohttp.ClientSession() as session:
            mapping = await self._fetch_mapping(session, origin, target)
            if export_path:
                Path(export_path).mkdir(parents=True, exist_ok=True)
                output_path = f"{export_path}/mapping_{origin}_{target}.xlsx"
                print(f"Save mapping to: {output_path}")
                mapping.to_excel(output_path, index=False)
            return mapping

    def _validate_and_transform_targets(
        self, targets: Union[list[str], tuple[str]]
    ) -> list[str]:
        """
        Validate and transform target dates into a sorted list of unique dates.

        This method ensures that the provided targets are either a list or a tuple of dates
        in "dd-mm-YYYY" format and transforms them into a list of valid Gemeindestände.

        :param targets: A list or tuple of target dates.
        :type targets: Union[list[str], tuple[str]]
        :return: A sorted list of unique Gemeindestand dates.
        :rtype: list[str]
        :raises TypeError: If the targets are not a list or tuple.
        :raises AssertionError: If the provided tuple does not have exactly two dates.
        """
        if isinstance(targets, tuple):
            assert len(targets) == 2, "Date range should have exactly two values!"

            # Find the closest official Gemeindestände for the start and end dates
            start = self._find_closest_official_gemeindestand(targets[0], "smaller")
            end = self._find_closest_official_gemeindestand(targets[1], "larger")

            # Extract the range of dates between start and end
            gemeindestaende = self.data['gemeindestand'].tolist()
            start_index = gemeindestaende.index(start)
            end_index = gemeindestaende.index(end) + 1
            targets = gemeindestaende[start_index:end_index]
        elif isinstance(targets, list):
            # Validate and transform each target date in the list
            targets = [
                self._find_closest_official_gemeindestand(target) for target in targets
            ]
        else:
            raise TypeError("Targets must be a list or tuple.")
        return targets

    async def create_multi_mapping(
        self,
        origin: str,
        targets: Union[list[str], tuple[str]],
        return_names: bool = True
    ) -> pd.DataFrame:
        """
        Create a mapping for multiple target dates starting from a given origin date.

        This method fetches mappings for multiple target dates asynchronously and
        merges the results into a single DataFrame.

        :param origin: The starting date of the mapping period in "dd-mm-YYYY" format.
        :type origin: str
        :param targets: A list or tuple of target dates in "dd-mm-YYYY" format.
        :type targets: Union[list[str], tuple[str]]
        :param return_names: If True, include municipality names in the output (default is True).
        :type return_names: bool, optional
        :return: A pandas DataFrame containing the merged mappings for all target dates.
        :rtype: pd.DataFrame
        """
        origin = self._find_closest_official_gemeindestand(origin, prefer='smaller')
        targets = self._validate_and_transform_targets(targets)

        targets.append(origin)
        date_strings = sorted(
            [datetime.strptime(date, "%d-%m-%Y") for date in set(targets)]
        )
        earliest_date = date_strings[0]
        targets = date_strings[1:]

        tasks = []
        connector = aiohttp.TCPConnector(limit=20)  # Increase the limit to allow more concurrent connections
        async with aiohttp.ClientSession(connector=connector) as session:
            start_date = earliest_date.strftime("%d-%m-%Y")
            for target in targets:
                target = target.strftime("%d-%m-%Y")
                tasks.append(
                    self._fetch_mapping(session, start_date, target)
                )
                start_date = target

            mappings = await asyncio.gather(*tasks)

        # Process the mappings as before
        full_mapping = None
        start_date = earliest_date.strftime("%d-%m-%Y")
        for i, mapping in enumerate(mappings):
            target = targets[i].strftime("%d-%m-%Y")

            cols = ['InitialCode', 'InitialName', 'TerminalCode', 'TerminalName']
            mapping = mapping[cols]
            mapping.columns = [
                f'bfs_gmde_code_{start_date}',
                f'bfs_gmde_name_{start_date}',
                f'bfs_gmde_code_{target}',
                f'bfs_gmde_name_{target}'
            ]

            if full_mapping is None:
                full_mapping = mapping
            else:
                full_mapping = full_mapping.merge(
                    mapping,
                    on=[f'bfs_gmde_code_{start_date}', f'bfs_gmde_name_{start_date}'],
                    how='left'
                )

            start_date = target

        if not return_names:
            full_mapping = full_mapping.drop(
                columns=[col for col in full_mapping.columns if "name" in col]
            )
        return full_mapping

    async def map_dataframe(
        self,
        df: pd.DataFrame,
        code_column: str,
        name_column: str,
        **kwargs: Union[str, list[str], tuple[str]]
    ) -> pd.DataFrame:
        """
        Map the BFS municipality codes to another one by adding new column(s) to the DataFrame.

        :param df: The DataFrame containing municipality data.
        :type df: pd.DataFrame
        :param code_column: The column containing BFS codes.
        :type code_column: str
        :param name_column: The column containing municipality names.
        :type name_column: str
        :param kwargs: Additional arguments:
                       - `origin`: The original Gemeindestand (optional).
                       - `target`: The target Gemeindestand (default is the newest one).
                       - `return_names`: If True, include municipality names (default is False).
        :return: The updated DataFrame with the mapped Gemeindestand.
        :rtype: pd.DataFrame
        """
        newest_gemeindestand = self.data.iat[-1, 0]

        origin = kwargs.get('origin', False)
        target = kwargs.get('target', newest_gemeindestand)
        return_names = kwargs.get('return_names', False)

        invalid_codes = bool(set(df[code_column].unique()) - self.all_historical_bfs_codes)
        if isinstance(origin, str) and invalid_codes:
            gemeinde_set = set(df[code_column].unique())
            gemeinde_set, wrong_codes_dict = self._check_for_non_bfs_codes(
                df, code_column, name_column, gemeinde_set
            )
            corrections_dict = self._correct_wrong_codes(origin, wrong_codes_dict)
            print(f"Found the following corrections: {corrections_dict}")
            df[code_column] = df[code_column].replace(corrections_dict)

        if not isinstance(origin, str):
            origin, corrections_dict = self.find_gemeindestand(df, code_column, name_column)
            if corrections_dict:
                df[code_column] = df[code_column].replace(corrections_dict)

        if isinstance(target, (tuple, list)):
            mapping = await self.create_multi_mapping(origin, target, return_names)
        else:
            mapping = await self.create_mapping(origin, target, return_names)

        df = df.rename(columns={code_column: f'bfs_gmde_code_{origin}'})

        print("Mapped DataFrame successfully!")
        return df.merge(
            mapping,
            on=[f'bfs_gmde_code_{origin}'],
            how='left'
        )

    async def map_multiple_gemeindestaende_to_newest(
        self,
        df: pd.DataFrame,
        code_column: int,
        stand_column: str
    ) -> pd.DataFrame:
        """
        If one has a column containing BFS codes from different states, this method
        can be used to map those to the latest Gemeindestand. For this it requires
        a column with the Gemeindestand of the respective row in the format `dd-mm-YY`.

        :param df: The DataFrame containing municipality data.
        :type df: pd.DataFrame
        :param code_column: The column containing BFS codes.
        :type code_column: int
        :param stand_column: The column containing Gemeindestand dates.
        :type stand_column: str
        :return: The DataFrame with codes mapped to the newest Gemeindestand.
        :rtype: pd.DataFrame
        """
        gemeindestaende = df[~df[stand_column].isna()][stand_column].unique()
        date_strings = sorted(
            [date for date in gemeindestaende],
            key=lambda x: datetime.strptime(x, "%d-%m-%Y")
        )
        newest = date_strings[-1]

        df[f'bfs_gmde_code_{newest}'] = 0
        df[code_column] = df[code_column].fillna(0).astype(int)
        origins = date_strings[:-1]

        connector = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self._fetch_mapping(session, origin, newest) for origin in origins]
            mappings = await asyncio.gather(*tasks)

        for origin, mapping in zip(origins, mappings):
            mapping = mapping[['InitialCode', 'TerminalCode']].copy()
            mapping.columns = [
                code_column,
                f'bfs_gmde_code_{newest}_update'
            ]
            mapping[stand_column] = origin
            mapping[code_column] = mapping[code_column].astype(int)
            mapping[f'bfs_gmde_code_{newest}_update'] = mapping[f'bfs_gmde_code_{newest}_update'].astype(int)

            df = df.merge(mapping, on=[stand_column, code_column], how='left')

            df.loc[~df[f'bfs_gmde_code_{newest}_update'].isna(), f'bfs_gmde_code_{newest}'] = df[f'bfs_gmde_code_{newest}_update']
            df = df.drop(columns=[f'bfs_gmde_code_{newest}_update'])

        df.loc[df[stand_column] == newest, f'bfs_gmde_code_{newest}'] = df[code_column]

        return df
