import pandas as pd
import asyncio
from datetime import datetime
from bfs_code_mapping import MunicipalityCodeMapper
from name_matching import MunicipalityNameMatcher
from name_matching.config import CONFIG


if __name__ == '__main__':
    """
    official_data = pd.read_csv('name_matching/data/official_bfs_gemeinden_2010-2024.csv')
    official_data['stand'] = official_data['stand'].apply(lambda x: ', '.join([datetime.strptime(part, '%Y-%m-%d').strftime('%d-%m-%Y') for part in x.split(', ')]))

    residence_names = pd.read_csv('name_matching/data/data-1730366425179.csv', encoding='utf-8')
    residence_names['residence'] = residence_names.residence.str.split(r'\bund\b|\bet\b|\be\b').explode('residence').reset_index(drop=True)

    hometown_names = pd.read_csv('name_matching/data/data-1730366364994.csv', encoding='utf-8')
    hometown_names['hometown'] = hometown_names.hometown.str.split(r'\bund\b|\bet\b|\be\b').explode('hometown').reset_index(drop=True)

    name_matcher = MunicipalityNameMatcher(official_data[['gmde_name', 'bfs_nr']], id_col='bfs_nr', name_col='gmde_name', config=CONFIG)

    results_df = name_matcher.match_dataframe(residence_names, 'residence')
    results_df.sort_values('confidence').to_excel('residence_test.xlsx', index=False)
    results_df = results_df.merge(official_data[['bfs_nr', 'stand']], left_on='matched_id', right_on='bfs_nr', how='left')
    results_df.to_excel('test_results.xlsx', index=False)
    """
    results_df = pd.read_excel('test_results.xlsx')
    code_mapper = MunicipalityCodeMapper()

    updated_df = asyncio.run(code_mapper.map_multiple_gemeindestaende_to_newest(results_df, 'matched_id', 'stand'))
    updated_df.to_excel('test_updated.xlsx', index=False)
