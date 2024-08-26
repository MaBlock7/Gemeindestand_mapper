from datetime import datetime
from pathlib import Path
import argparse
import pandas as pd
import pickle

TARGET_PATH = Path('./utils/data')
API_PATH = 'https://sms.bfs.admin.ch/WcfBFSSpecificService.svc/AnonymousRest/communes/'  # noqa

# Constants for column names
MUTATION_DATE_COL = 'MutationDate'
COUNT_GMDE_COL = 'anz_gmde'
GMDE_STAND_COL = 'gemeindestand'


def get_gmde_stand_dates(start_point: str, end_point: str) -> list:
    """Get a list of all dates (`dd-mm-YYYY`) where the Gemeindestand changed.

    Parameters:
    -----------
    start_point: str
        Date from where to retrieve the mutation data.

    end_point: str
        Date up to where to retrieve the mutation data.

    Returns:
    --------
    list
    """
    url_mut = f"{API_PATH}mutations?startPeriod={start_point}&endPeriod={end_point}"  # noqa

    mutations = pd.read_csv(url_mut, usecols=[MUTATION_DATE_COL])
    mutations['MutationDate'] = (
        mutations[MUTATION_DATE_COL].str.replace('.', '-')
    )

    gmde_stand_dates = mutations[MUTATION_DATE_COL].unique()
    print(f'Number of Gemeindestände found: {len(gmde_stand_dates)}')

    return gmde_stand_dates


def create_snapshots(gmde_stand_dates: list) -> None:
    """For each date, add the numbers of municipalities
       at that point in timeto a dataframe

    Parameters:
    -----------
    gmde_stand_dates: list
        List of dates where the Gemeindestand changed.
    get_missing: bool
        If True, only retrieves the missing gemeindestände.
    """
    count_df = pd.DataFrame({GMDE_STAND_COL: [],
                             COUNT_GMDE_COL: []})
    gmde_dct = {}

    for gmde_stand_date in gmde_stand_dates:
        url_snap =  f"{API_PATH}snapshots?useBfsCode=true&startPeriod={gmde_stand_date}&endPeriod={gmde_stand_date}"  # noqa
        snapshots = pd.read_csv(url_snap,
                                usecols=['Identifier', 'Level',
                                         'Parent', 'Name_de',
                                         'Name_en', 'Name_fr', 'Name_it'])
        snapshots = snapshots[snapshots['Level'] == 3]

        count_df.loc[len(count_df)] = [gmde_stand_date,
                                       snapshots.iloc[:, 0].nunique()]

        print(f'...exporting snapshot for {gmde_stand_date}')

        date_obj = (
            datetime.strptime(gmde_stand_date, '%d-%m-%Y').strftime('%Y-%m-%d')
        )
        snapshots.to_csv(TARGET_PATH / 'snapshots' /
                         f'gemeindestand_{date_obj}.csv', index=False)
        gmde_dct[gmde_stand_date] = snapshots['Identifier'].to_list()

    if Path(TARGET_PATH / 'anzahl_gmde_pro_stand.csv').exists():
        count_df.to_csv(TARGET_PATH / 'anzahl_gmde_pro_stand.csv', index=False, mode='a', header=False)
    else:
        count_df.to_csv(TARGET_PATH / 'anzahl_gmde_pro_stand.csv', index=False)

    all_files = list(Path(TARGET_PATH / 'snapshots').glob('*.csv'))
    gmde_dct = {}
    for file in all_files:
        df = pd.read_csv(file)
        file_name = file.name.removesuffix('.csv')
        date = f"{file_name[-2:]}-{file_name[-5:-3]}-{file_name[-10:-6]}"
        gmde_dct[date] = df.Identifier.unique()

    with open(TARGET_PATH / 'gemeindestaende.pkl', 'wb') as outfile:
        pickle.dump(gmde_dct, outfile)

    # Clean up utils data
    count_df = pd.read_csv(TARGET_PATH / 'anzahl_gmde_pro_stand.csv')
    count_df = count_df.drop_duplicates()
    count_df.to_csv(TARGET_PATH / 'anzahl_gmde_pro_stand.csv', index=False)
