# DSE Cal-Adapt Climate Pipeline (Luigi branch)

Pull downscaled climate projections (LOCA2, 3km) for any US national park via a resumable Luigi pipeline. If it crashes, just re-run and it picks up where it left off.

## Setup

You need Docker Desktop and VS Code with the Dev Containers extension.

```bash
# open this folder in VS Code, then:
# Cmd+Shift+P -> "Dev Containers: Reopen in Container"
# first time takes ~8 min, after that it's instant
```

## Running the pipeline

From the VS Code terminal (inside the container):

```bash
# required: park name (fuzzy searched), --variable, --scenario
python run_pipeline.py "joshua tree" --variable T_Max --scenario historical
python run_pipeline.py "yosemite" --variable Precip --scenario all
python run_pipeline.py "mojave" --variable all --scenario ssp585
python run_pipeline.py "grand canyon" --variable T_Max --scenario historical --output plot
```

The pipeline fuzzy searches your park name and tells you what it found:
```
Found: "Joshua Tree National Park"
Park:      Joshua Tree National Park (JoshuaTree)
Variables: ['T_Max']
Scenarios: ['Historical Climate']
```

## Flags

| Flag | Options | Required? |
|------|---------|-----------|
| `park` | any name, fuzzy searched | yes |
| `--variable` | T_Max, T_Min, Precip, all | yes |
| `--scenario` | historical, ssp245, ssp370, ssp585, all | yes |
| `--timescale` | monthly, daily, yearly | no (default: monthly) |
| `--output` | csv, plot, timeseries | no (default: csv) |
| `--workers` | number | no (default: 4) |

CSVs go to `data/csv/{ParkName}/`. Plots go to `data/plots/`.

- `csv` — raw data as CSV files
- `plot` — spatial heatmap (requires extra Coiled fetch for gridded data)
- `timeseries` — line chart from the CSVs (no extra Coiled, just reads what was fetched)

## Finding parks

You can also search interactively:

```bash
python -c "
from andrewAdaptLibrary import ParkCatalog
catalog = ParkCatalog('USA_National_Park_Service_Lands_20170930_4993375350946852027/USA_Federal_Lands.shp')
print(catalog.search('yosemite'))
"
```

## Notebooks

Still here for exploration and plotting:

| Notebook | What it does |
|----------|-------------|
| `Tutorial_Coiled_Setup` | Setup walkthrough + learning the library |
| `ParkCatalog_Demo` | Fuzzy search parks, check data availability |
| `Spatial_Comparison` | 2x2 spatial heatmaps comparing scenarios |
| `Full_Data_Extraction` | Original notebook version (pipeline replaces this) |
