from pathlib import Path
import pandas as pd

SOURCE_PATH = Path('../bfs_source_data/')
TARGET_PATH = Path('../processed_data/')
INIT_GMDE_FILE = 'Gemeindestand_31_05_1981.xlsx'
MUTATION_FILE = 'Mutationen_1981_2023.xlsx'



def process_column_names(df, suffix=False):
    """Function to process the column names of a DataFrame

    Parameters
    ----------
    df: pd.DataFrame
    suffix: bool
        if True, adds (_old, _new) to columns of the same name

    Returns
    -------
    pd.DataFrame
    """

    if 'Mutationsnummer' in df.columns:
        df = df.drop(columns='Mutationsnummer')

    if suffix:
        new_col_names = [c + '_old' if '.1' not in c else c[:-2] + '_new'
                         for c in df.columns]
        new_col_names[-1] = new_col_names[-1].replace('_old', '')
        df.columns = new_col_names

    df.columns = [c.lower().replace(' ', '_').replace('-', '_')
                  for c in df.columns]

    return df


def create_date_dictionary(date_list):
    date_dict = {}
    year_suffix = {}

    for date_str in date_list:
        date_parts = date_str.split('-')
        year = date_parts[0]

        if year not in year_suffix:
            year_suffix[year] = 'a'
        else:
            year_suffix[year] = chr(ord(year_suffix[year]) + 1)

        suffix = year_suffix[year]
        date_dict[date_str] = f"{year[2:4]}_{suffix}"

    return date_dict


def contains_duplicates(df, column_old, column_new):
    # Find duplicate values in the specified column
    duplicate_values = df[df.duplicated(subset=[column_old], keep=False)]
    if not duplicate_values.empty:
        return True
    else: 
        return False


def create_mutations(df, column_old, column_new, duplicates):
    if duplicates:
        print(list(zip(df[column_old], df[column_new])))
        return list(zip(df[column_old], df[column_new]))
    else:
        df = df[df[column_old] != df[column_new]]
        return dict(zip(df[column_old], df[column_new]))


def create_mutation_matrix(df, new_col, mutations, duplicates):
    if duplicates:
        pass
        # for tup in mutations:     
    else:
        df[new_col] = df.iloc[:, -1:].replace(mutations)
        return df


def save_gmde_stand_file(df, file_name):
    output_df = df.copy()
    output_df.columns = ['kt', 'bez_code',
                         'gmde_code', 'gmde_name', 'gmde_stand']
    output_df.to_excel(TARGET_PATH / file_name, index=False)


def remove_uninhabited(df):
    special_gmde_codes = [2391, 5391, 5394, 9150, 9155]
    return df[~(df.bfs_gde_nummer.isin(special_gmde_codes))]


def process_gmde_data(g_df, m_df):
    to_drop = m_df.bfs_gde_nummer_old.to_list()
    g_df = g_df[~(g_df.bfs_gde_nummer.isin(to_drop))]

    m_df = m_df.filter(regex='_new')
    m_df.columns = [c.replace('_new', '') for c in m_df.columns]
    m_df = m_df.drop_duplicates()

    g_df = pd.concat([g_df, m_df], axis=0)
    g_df = g_df.sort_values(by='bfs_gde_nummer')

    return g_df


def main():
    # read data
    gmde_df = pd.read_excel(SOURCE_PATH / INIT_GMDE_FILE,
                            engine='openpyxl')
    mutation_df = pd.read_excel(SOURCE_PATH / MUTATION_FILE, header=1,
                                engine='openpyxl')

    # reformat column names and remove unnecessary ones
    gmde_df = process_column_names(gmde_df)
    gmde_df = gmde_df.drop(columns=['hist._nummer',
                                    'datum_der_aufnahme',
                                    'bezirksname'])
    mutation_df = process_column_names(mutation_df, suffix=True)

    # add column with Gemeindestand
    dates = mutation_df.datum_der_aufnahme.unique()
    mapping = create_date_dictionary(dates)
    mutation_df['gemeindestand'] = (
        mutation_df['datum_der_aufnahme'].replace(mapping)
    )

    for idx, gemeindestand in enumerate(mutation_df.gemeindestand.unique()):

        temp_df = mutation_df.query("gemeindestand == @gemeindestand")
        dup_bool = contains_duplicates(temp_df,
                               'bfs_gde_nummer_old',
                               'bfs_gde_nummer_new')
        
        mutations = create_mutations(temp_df,
                                             'bfs_gde_nummer_old',
                                             'bfs_gde_nummer_new',
                                             duplicates=dup_bool)

        gmde_df = process_gmde_data(gmde_df, temp_df)
        gmde_df = remove_uninhabited(gmde_df)
        gmde_df['gemeindestand'] = gemeindestand

        if idx == 0:
            mutation_matrix = pd.DataFrame({
                gemeindestand: gmde_df.bfs_gde_nummer})
        else:
            if isinstance(mutations, dict):
                mutation_matrix = create_mutation_matrix(mutation_matrix,
                                                        gemeindestand,
                                                        mutations,
                                                        dup_bool)
            # else:
                
        save_gmde_stand_file(gmde_df, f'Gemeindestand_{gemeindestand}.xlsx')

    file_name = TARGET_PATH / 'Gemeindestand_Mutationsmatrix.xlsx'
    mutation_matrix.to_excel(file_name, index=False)


# GEMEINDESTAND-CREATOR

l_init = gmde_df['bfs_gde_nummer'].to_list() # Data 01.06.1981

change_matrices = []
for gemeindestand in mutation_df.gemeindestand.unique():
    temp_df = mutation_df.query("gemeindestand == @gemeindestand")
    edges = list(zip(temp_df.bfs_gde_nummer_old, temp_df.bfs_gde_nummer_new))
    change_matrices.append(edges)
    
for i, gemeindestand in enumerate(mutation_df.gemeindestand.unique()):
    graphs = build_graphs(l_init, change_matrices[:i+1])
    gemeinde_mapping = {}
    for root, graph in graphs.items():
        leaves = (
            [node for node, out_degree in graph.out_degree() if out_degree == 0] 
            + [node for node in list(nx.nodes_with_selfloops(graph))]
        )
        gemeinde_mapping[root] = leaves

    col1 = []
    for values in gemeinde_mapping.values():
        for v in values:
            if v not in col1:
                col1.append(v)

    result = pd.DataFrame({gemeindestand: col1})
    result.to_excel(TARGET_PATH / f'Gemeindestand_{gemeindestand}.xlsx', index=False)


# GEMEINDEMAPPING-TABLE

col1 = []
col2 = []
for key, value in gemeinde_mapping.items():
    for v in value:
        col1.append(key)
        col2.append(v)

result = pd.DataFrame({'82_a': col1,
                       '23_a': col2})
result.to_excel(TARGET_PATH / 'Gemeindemapping_82a_23a.xlsx', index=False)

if __name__ == "__main__":
    main()
