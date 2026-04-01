"""Pipeline configuration — parks, variables, scenarios, and paths."""

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NPS_SHAPEFILE = os.path.join(
    PROJECT_ROOT,
    "USA_National_Park_Service_Lands_20170930_4993375350946852027",
    "USA_Federal_Lands.shp",
)

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "csv", "full_extraction")

PARKS = ["Joshua Tree National Park", "Mojave National Preserve"]

# Short names for file paths (no spaces)
PARK_SHORT = {
    "Joshua Tree National Park": "JoshuaTree",
    "Mojave National Preserve": "Mojave",
}

VARIABLES = ["T_Max", "T_Min", "Precip"]

SCENARIOS = ["Historical Climate", "SSP 2-4.5", "SSP 3-7.0", "SSP 5-8.5"]

HISTORICAL_YEARS = (1950, 2014)
FUTURE_YEARS = (2015, 2100)

# Coiled cluster defaults
COILED_REGION = "us-west1"
COILED_N_WORKERS = 8
COILED_WORKER_MEMORY = "16 GiB"
