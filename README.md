# DSE Cal-Adapt Climate Pipeline (Luigi branch)

Same climate data pipeline, but the extraction workflow is now a resumable Luigi pipeline instead of a notebook. If it crashes, just re-run and it picks up where it left off.

## Setup

You need Docker Desktop and VS Code with the Dev Containers extension.

```bash
# open this folder in VS Code, then:
# Cmd+Shift+P -> "Dev Containers: Reopen in Container"
# first time takes ~8 min, after that it's instant
```

## Finding parks

Use the ParkCatalog Demo notebook or a quick python shell to fuzzy search:

```python
from andrewAdaptLibrary import ParkCatalog
catalog = ParkCatalog("USA_National_Park_Service_Lands_.../USA_Federal_Lands.shp")
catalog.search("yosemite")  # finds exact name to use
```

Then add the park to `pipelines/config.py` under `PARKS` and `PARK_SHORT`.

## Running the pipeline

From the container terminal (VS Code terminal or `./docker/run_docker.sh shell`):

```bash
python run_pipeline.py                          # all parks in config
python run_pipeline.py --parks JoshuaTree       # just one park
python run_pipeline.py --parks JoshuaTree Mojave --workers 4
```

Output CSVs land in `data/csv/full_extraction/`. Re-running skips anything already done.

## Notebooks

The notebooks still work for exploration and plotting:

| Notebook | What it does |
|----------|-------------|
| `Tutorial_Coiled_Setup` | Setup walkthrough + learning the library |
| `ParkCatalog_Demo` | Fuzzy search parks, check data availability |
| `Spatial_Comparison` | 2x2 spatial heatmaps comparing scenarios |

`Full_Data_Extraction` is still there but the pipeline replaces it for production runs.

## Pipeline structure

```
run_pipeline.py          <- entry point, starts Coiled cluster
pipelines/config.py      <- parks, variables, scenarios to extract
pipelines/climate_extraction.py  <- Luigi tasks (fetch, combine, compute T_Avg)
```
