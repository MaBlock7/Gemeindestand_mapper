
# Swiss Municipality Name Matcher & Code Mapper

This project provides tools for matching Swiss municipality names to their official municipality names and BFS codes, as well as mapping municipality codes from one state (Gemeindestand) to another over time. The project leverages data from the Swiss Federal Statistical Office API to ensure accurate and up-to-date information.

## Features

1. **Municipality Name Matcher**:  
   Matches input names to official municipality names using advanced normalization, fuzzy matching, and TF-IDF similarity.

2. **Municipality Code Mapper**:  
   Maps BFS codes between different Gemeindestände, supports asynchronous operations, and validates mappings against historical data.

3. **Integration with BFS API**:  
   Fetches municipality state data dynamically, including historical snapshots and the latest updates.

4. **Highly Configurable Matching**:  
   Customizable rules for handling common mistakes, foreign municipality indicators, and false positives.

## Requirements

- Python 3.11 or higher
- Dependencies listed in `requirements.txt`

## Installation

1. Clone the repository:
   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```

2. (Optional) Create virtual environment:
   ```bash
   # Create a virtual environment in a folder named 'venv'
   python -m venv venv

   # Activate the virtual environment (Max/Linux)
   source venv/bin/activate

   # Activate the virtual environment (Windows)
   venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### 1. Municipality Name Matcher

The `MunicipalityNameMatcher` class allows matching of municipality names against official BFS data.

#### Example Usage:
```python
from municipality_name_matcher import MunicipalityNameMatcher, CONFIG

matcher = MunicipalityNameMatcher(config=CONFIG)

# Single name matching
matched_id, matched_name, confidence = matcher.match_name("Zurich")
print(matched_id, matched_name, confidence)

# DataFrame matching
import pandas as pd

query_df = pd.DataFrame({"municipality_name": ["Zurich", "Genva", "Muri b. Bern", "St. Gallen"]})
matched_df = matcher.match_dataframe(query_df, query_column="municipality_name")
print(matched_df)
```

### 2. Municipality Code Mapper

The `MunicipalityCodeMapper` class maps BFS codes between Gemeindestände.

#### Example Usage:
```python
from municipality_code_mapper import MunicipalityCodeMapper

mapper = MunicipalityCodeMapper()

# Create a simple mapping from one Gemeindestand to another
mapping = await mapper.create_mapping("01-01-2000", "01-01-2024")
print(mapping)

# Create a complex mapping from one Gemeindestand to o selection of others
mapping = await mapper.create_multi_mapping("01-01-2000", ["01-01-2005", "01-01-2010", "01-01-2015", "01-01-2020"])
print(mapping)

# We can also map to previous Gemeindestände, keep in mind this will lead to duplicates because of a one to many mapping
mapping = await mapper.create_multi_mapping("01-01-2015", ["01-01-2005", "01-01-2010", "01-01-2020"])
print(mapping)

# If you want to map to a range of Gemeindestände, you can pass a range (includes lower and upper bound if it exists)
mapping = await mapper.create_multi_mapping("01-01-2000", ("01-01-2005", "01-01-2010"))
print(mapping)

# Map a DataFrame
df = pd.DataFrame({"bfs_code": [261, 3203], "gemeinde_name": ["Zürich", "St. Gallen"]})
mapped_df = await mapper.map_dataframe(df, code_column='bfs_code', name_column='gemeinde_name', target=['01-01-2016', '01-01-2017', '01-01-2018', '01-01-2019', '01-01-2020', '01-01-2021', '01-01-2022', '01-01-2023', '01-01-2024'])
print(mapped_df)
```

### 3. Fetching and Updating Data

Both `MunicipalityNameMatcher` and `MunicipalityCodeMapper` automatically fetch the most recent Gemeindestands data from the BFS api through the base class `BaseMunicipalityData`.
This ensures, that they are as independent as possible from any local files. Only exception is the common names file which contains
common (inofficial) names of municipalities such as `Zugo` for `Zug` or `Zurich` instead of `Zürich`.

- This common names file was downloaded from: [BFS Gemeindenamen mit gebräuchlichen Übersetzungen](https://www.bfs.admin.ch/bfs/de/home/grundlagen/agvch/gemeindenamen-gebraeuchlichen-uebersetzungen.html)
- And should always be stored at: [code/municipality_mapping/data/common_names.csv](./code/municipality_mapping/data/common_names.csv)

This file should be updated by newer versions if neccessary.

### 4. Run Tests

   ```bash
   python -m municipality_mapping.tests.test_municipality_matcher
   ```

## Data Source

- **API**: [Swiss Federal Statistical Office API](https://www.agvchapp.bfs.admin.ch/de/mutated-communes/query)  

## Contribution

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request with a detailed description.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Additional Notes

- The name matcher and code mapper can handle edge cases like foreign municipalities, shared territories, and historical BFS codes.

For further questions, please refer to the inline documentation in the source code.
