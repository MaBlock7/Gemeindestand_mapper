import pandas as pd
import asyncio
from municipality_mapping import (
    MunicipalityCodeMapper, MunicipalityNameMatcher, CONFIG
)


if __name__ == '__main__':
    # Initialize the name matcher and code mapper
    name_matcher = MunicipalityNameMatcher(config=CONFIG)
    code_mapper = MunicipalityCodeMapper()

    # Load the data to match and map
    residence_names = pd.read_csv('name_matching/data/data-1730366425179.csv', encoding='utf-8')
    residence_names['residence'] = (
        residence_names['residence']
        .str.split(r'\bund\b|\bet\b|\be\b')
        .explode('residence')
        .reset_index(drop=True)
    )

    hometown_names = pd.read_csv('name_matching/data/data-1730366364994.csv', encoding='utf-8')
    hometown_names['hometown'] = (
        hometown_names['hometown']
        .str.split(r'\bund\b|\bet\b|\be\b')
        .explode('hometown')
        .reset_index(drop=True)
    )

    results_df = name_matcher.match_dataframe(residence_names, 'residence')
    results_df.sort_values('confidence')

    results_df = (
        results_df
        .merge(
            name_matcher.officials[['matched_id', 'gmde_stand']],
            on='matched_id',
            how='left'
        )
    )
    results_df.to_excel('test_results.xlsx', index=False)

    # results_df = pd.read_excel('test_results.xlsx')
    updated_df = asyncio.run(
        code_mapper.map_multiple_gemeindestaende_to_newest(results_df, 'matched_id', 'gmde_stand')
    )
    updated_df.to_excel('test_updated.xlsx', index=False)
