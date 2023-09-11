from pathlib import Path
import networkx as nx
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


def build_graphs(initial_list, change_matrices):
    """Function to create a graph for each municipality
    to represent changes such as merging, splitting, and
    renaming of municipalities.

    Parameters
    ----------
    initial_list: list
        List of municipality numbers
    change_matrices: Union[List | Tuples]
        List of edges per Gemeindestand

    Returns
    -------
    Union[dict | nx.DiGraph]
    """
    # Create a dictionary to store the trees
    graphs = {}

    # Iterate over each element in the initial list
    for root in initial_list:
        # Create a directed graph
        G = nx.DiGraph()
        G.add_node(root)

        # Iterate through the change matrices to build the graph
        for edges in change_matrices:
            for edge in edges:
                orig_value, new_value = edge
                if orig_value in G:
                    G.add_node(new_value)
                    G.add_edge(orig_value, new_value)

                    condition1 = G.has_edge(orig_value, orig_value)
                    condition2 = ((orig_value, orig_value) not in edges)
                    if (condition1 & condition2):
                        G.remove_edge(orig_value, orig_value)

        # Store the graph in the dictionary
        graphs[root] = G

    return graphs


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
    mutation_df = (
        mutation_df
        .rename(columns={'datum_der_aufnahme': 'gemeindestand'})
    )

    # GEMEINDESTAND-CREATOR
    l_init = gmde_df['bfs_gde_nummer'].to_list()  # Data 01.06.1981

    change_matrices = []
    for gemeindestand in mutation_df.gemeindestand.unique():
        temp_df = mutation_df.query("gemeindestand == @gemeindestand")
        edges = list(
            zip(temp_df.bfs_gde_nummer_old, temp_df.bfs_gde_nummer_new)
        )
        change_matrices.append(edges)

    for i, gemeindestand in enumerate(mutation_df.gemeindestand.unique()):
        graphs = build_graphs(l_init, change_matrices[:i+1])
        gemeinde_mapping = {}
        for root, graph in graphs.items():
            leaves = (
                [node for node, out_degree in graph.out_degree()
                 if out_degree == 0]
                + [node for node in list(nx.nodes_with_selfloops(graph))]
            )
            gemeinde_mapping[root] = leaves

        col1 = []
        for values in gemeinde_mapping.values():
            for v in values:
                if v not in col1:
                    col1.append(v)

        result = pd.DataFrame({gemeindestand: col1})
        result.to_excel(TARGET_PATH / 'gmde_stde' /
                        f'Gemeindestand_{gemeindestand}.xlsx', index=False)

    # NUMBER OF MUNICIPALITIES PER GEMEINDESTAND
    directory = TARGET_PATH / 'gmde_stde'
    df = pd.DataFrame({'gemeindestand': [],
                       'anz_gmde': []})
    for file in directory.glob('*'):
        df_temp = pd.read_excel(file)
        df.loc[len(df)] = [df_temp.columns.values[0],
                           df_temp.iloc[:, 0].nunique()]

    df = df.sort_values(['gmde_stand']).reset_index(drop=True)
    df.to_excel(TARGET_PATH / 'Anzahl_Gemeinden_pro_Gemeindestand.xlsx',
                index=False)


if __name__ == "__main__":
    main()
