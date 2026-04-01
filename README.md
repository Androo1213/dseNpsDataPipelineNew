# DSE Cal-Adapt Climate Pipeline (formal package branch)

Same pipeline, but the library is now a proper installable Python package called `dse-climate`. No more `sys.path` hacks -- just `from dse_climate import ParkCatalog`.

## Setup

You need Docker Desktop and VS Code with the Dev Containers extension.

```bash
# open this folder in VS Code, then:
# Cmd+Shift+P -> "Dev Containers: Reopen in Container"
```

The container runs `pip install -e .` automatically so the package is ready to go.

## Installing outside the container

If you want to use the library in your own project without Docker:

```bash
pip install git+https://github.com/SchmidtDSE/climate_data_pipeline@formal-package
```

Or for development:

```bash
git clone https://github.com/SchmidtDSE/climate_data_pipeline
cd climate_data_pipeline
git checkout formal-package
pip install -e ".[all]"   # includes coiled + climakitae
```

## Finding parks

```python
from dse_climate import ParkCatalog
catalog = ParkCatalog("path/to/USA_Federal_Lands.shp")
catalog.search("yosemite")  # fuzzy search across 437 NPS units
```

## Notebooks

Same 4 notebooks as main, but imports use `from dse_climate import ...`:

| Notebook | What it does |
|----------|-------------|
| `Tutorial_Coiled_Setup` | Setup walkthrough + learning the library |
| `ParkCatalog_Demo` | Fuzzy search parks, check data availability |
| `Full_Data_Extraction` | Pull T_Max, T_Min across all scenarios, export CSVs |
| `Spatial_Comparison` | 2x2 spatial heatmaps comparing scenarios |

## Package structure

```
src/dse_climate/
    __init__.py    <- public API (ParkCatalog, get_climate_data, etc.)
    _core.py       <- all the implementation
pyproject.toml     <- package metadata + dependencies
```
