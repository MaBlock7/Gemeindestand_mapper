# Overview

This is a tool to retrieve data on the municipality mutations and mapping tables from older municipality states (Gemeindestände) to newer ones.

# Project Setup
```
GEMEINDESTAND_MAPPER
├── .env
├── bfs_source_data
│   ├── directions_bfs
│   │   └── how_to_access_bfs_rest_api.pdf
│   └── Gleiche Rechte für Mann und Frau.xlsx
├── processed_data
│   ├── snapshots
│   ├── anzahl_gmde_pro_stand.csv
│   └── gemeindestaende.pkl
├── results
│   └── Abstimmung_1981_mapped.xlsx
└── src
    ├── utils
    │   ├── __init__.py
    │   ├── api.py
    │   └── mapper.py
    ├── gemeinde_mapper.ipynb
    └── README.md
```

## Initial Venv Setup (only required once)

```shell
cd Gemeindestand_mapper

python3 -m venv .venv
```

## Activate Environment

```shell
source .env/bin/activate
pip install -r requirements.txt
```
