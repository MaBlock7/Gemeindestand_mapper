# Overview

- **Data Source**: [Swiss Federal Statistical Office API](https://www.agvchapp.bfs.admin.ch/de/mutated-communes/query)
- **Data Storage**: Processed data is saved in the `processed_data/snapshots` directory. Municipality counts for all states are stored in `src/utils/data/anzahl_gmde_pro_stand.csv`.

## Getting Started

### 1. Retrieving Municipality State Data

The `api.py` script is used to retrieve and update data on municipality states. This includes fetching historical and current municipality states and storing them in the `processed_data/snapshots` directory. The script also generates a table of municipality counts, which is essential for mapping older municipality states to newer ones.

#### **Usage**

To retrieve all municipality states from January 1, 1981, run the following commands:

```shell
cd src
python update_utils_data.py -h
```

```shell
cd src
python update_utils_data.py -h
```

This will show the following options:

```shell
options:
  -h, --help            Show this help message and exit.
  -sp START_POINT, --start_point START_POINT
                        Specify the start date (format: 'dd-mm-YYYY').
  -ep END_POINT, --end_point END_POINT
                        Specify the end date (format: 'dd-mm-YYYY').
```

#### **Examples**

- Retrieve all from 01-01-1981 to today (default setting)

```shell
python update_utils_data.py
```

- Retrieve states within a specific date range:

```shell
python update_utils_data.py -sp '01-01-2023' -ep '28-02-2024'
```

### 2. Mapping Municipality States

The gemeindestand_mapping.ipynb notebook is used to map data from one municipality state to another.

### **Steps**

1. **Prepare the Data:**

- Place the original data file to be mapped into the raw_data directory.
- Ensure the file is correctly formatted. This can be done by using the file_formater.ipynb notebook or by manually adjusting the file, particularly if it’s in Excel format.

2. **Configure and Run the Mapping:**

- Open gemeindestand_mapping.ipynb.
- Adjust the folder and file name paths to point to your data.
Execute the cells to perform the mapping.

## Additional Information
- Check the file src/utils/data/anzahl_gmde_pro_stand.csv for the latest stored Gemeindestand and ensure all necessary data is up to date.
- For further customization or troubleshooting, refer to the respective notebooks and scripts within the src directory.
