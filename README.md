# Overview

This is a tool to retrieve data on the municipality mutations and mapping tables from older municipality states (Gemeindestände) to newer ones.

# Project Setup
```
GEMEINDESTAND_MAPPER
├── .venv
├── raw
│   ├── directions_bfs
│   │   └── how_to_access_bfs_rest_api.pdf
│   └── Gleiche Rechte für Mann und Frau.xlsx
├── results
│   └── Abstimmung_1981_mapped.xlsx
└── src
    ├── utils
    │   ├── __init__.py
    │   ├── data
    │   │   ├── snapshots
    │   │   │   └── ...
    │   │   ├── anzahl_gmde_pro_stand.csv
    │   │   └── gemeindestaende.pkl
    │   ├── utils.py
    │   └── mapper.py
    ├── gemeindestand_mapping.ipynb
    ├── file_formatter.ipynb
    ├── update_utils_data.py
    └── README.md
```

## Initial Venv Setup (only required once)

```shell
cd Gemeindestand_mapper

python3 -m venv .venv
```

## Activate Environment

```shell
source .venv/bin/activate
pip install -r requirements.txt
```
