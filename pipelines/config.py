"""Pipeline configuration — paths and Coiled defaults."""

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NPS_SHAPEFILE = os.path.join(
    PROJECT_ROOT,
    "USA_National_Park_Service_Lands_20170930_4993375350946852027",
    "USA_Federal_Lands.shp",
)

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "csv")
PLOT_DIR = os.path.join(PROJECT_ROOT, "data", "plots")

HISTORICAL_YEARS = (1950, 2014)
FUTURE_YEARS = (2015, 2100)

# Coiled cluster defaults
COILED_REGION = "us-west1"
COILED_N_WORKERS = 4
COILED_WORKER_MEMORY = "16 GiB"


def to_short_name(full_name: str) -> str:
    """'Joshua Tree National Park' -> 'JoshuaTree'"""
    name = full_name
    for suffix in ["National Park", "National Preserve", "National Monument",
                    "National Recreation Area", "National Seashore",
                    "National Historic Site"]:
        name = name.replace(suffix, "")
    return name.strip().replace(" ", "")
