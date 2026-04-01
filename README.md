# DSE Cal-Adapt Climate Pipeline

Pull downscaled climate projections (LOCA2, 3km) for any US national park. Supports Historical, SSP 2-4.5, SSP 3-7.0, and SSP 5-8.5 scenarios across 15 CMIP6 models.

## Quick start

You need Docker Desktop and VS Code with the Dev Containers extension.

```bash
# open this folder in VS Code, then:
# Cmd+Shift+P -> "Dev Containers: Reopen in Container"
# first time takes ~8 min, after that it's instant
```

Pick the **Python (py-env)** kernel in the top right when you open a notebook.

## Finding parks

Every notebook uses `ParkCatalog` which has fuzzy search across all 437 NPS units:

```python
from andrewAdaptLibrary import ParkCatalog

catalog = ParkCatalog("USA_National_Park_Service_Lands_.../USA_Federal_Lands.shp")
catalog.search("yosemite")       # finds "Yosemite National Park"
catalog.search("grand canion")   # still finds Grand Canyon (fuzzy)
```

Then pass the exact name into the data fetching functions.

## Notebooks

| Notebook | What it does |
|----------|-------------|
| `Tutorial_Coiled_Setup` | Walks you through setting up Coiled, understanding the library, and fetching your first data |
| `ParkCatalog_Demo` | Shows how to search for parks and check what data exists for them |
| `Full_Data_Extraction` | Production run: pulls T_Max, T_Min for Joshua Tree + Mojave across all scenarios, exports CSVs |
| `Spatial_Comparison` | 2x2 heatmaps comparing temperature/precip across scenarios within park boundaries |

## Alt: browser instead of VS Code

```bash
./docker/run_docker.sh build      # first time
./docker/run_docker.sh notebook   # opens JupyterLab at http://localhost:8888
```
