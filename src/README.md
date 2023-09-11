## Overview

Data source: https://www.agvchapp.bfs.admin.ch/de/mutated-communes/query

- Data is automatically retrieved from the REST api (see bfs_source_data/directions_bfs)

## Run code
### 1. Run the api.py

The api.py script can be used to retrieve data on all municipality states (Gemeindestände) which is stored under processed_data/snapshots. Furthermore, it creates a table that stores the municipality counts for all municipality states. This data is later used in the mapping of an older municipality state on to a newer one.

```shell
python api.py -h
```

```shell
options:
  -h, --help            show this help message and exit
  -sp START_POINT, --start_point START_POINT
  -ep END_POINT, --end_point END_POINT
```

### 2. Run gemeinde_mapper.ipynb