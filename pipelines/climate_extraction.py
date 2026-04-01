"""Luigi pipeline for climate data extraction.

Mirrors the Full_Data_Extraction notebook as a resumable task graph.
Each task writes a CSV; Luigi skips tasks whose output files already exist.

Usage:
    python run_pipeline.py                     # all parks, all variables
    python run_pipeline.py --parks JoshuaTree  # single park
"""

import os
import sys
import luigi
import pandas as pd

# Ensure library is importable
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "lib"))

from andrewAdaptLibrary import (
    ParkCatalog,
    get_climate_data,
    SCENARIO_MAP,
)
from pipelines.config import (
    NPS_SHAPEFILE,
    OUTPUT_DIR,
    PARKS,
    PARK_SHORT,
    VARIABLES,
    HISTORICAL_YEARS,
    FUTURE_YEARS,
)


# ---------------------------------------------------------------------------
# Shared state: ParkCatalog and Coiled cluster (initialized once per process)
# ---------------------------------------------------------------------------
_catalog = None
_cluster = None


def get_catalog():
    global _catalog
    if _catalog is None:
        _catalog = ParkCatalog(NPS_SHAPEFILE)
    return _catalog


def get_cluster():
    """Return the running Coiled cluster (started by run_pipeline.py)."""
    global _cluster
    return _cluster


def set_cluster(cluster):
    global _cluster
    _cluster = cluster


# ---------------------------------------------------------------------------
# Task 1: Fetch climate data for one (park, variable, period) combo
# ---------------------------------------------------------------------------
class FetchClimateTask(luigi.Task):
    """Fetch climate data for a single variable and time period.

    Produces a CSV at data/csv/full_extraction/{park}_{variable}_{period}.csv
    """
    park = luigi.Parameter()          # full park name
    variable = luigi.Parameter()      # e.g. "T_Max"
    period = luigi.Parameter()        # "historical" or "future"

    def output(self):
        short = PARK_SHORT[self.park]
        return luigi.LocalTarget(
            os.path.join(OUTPUT_DIR, f"{short}_{self.variable}_{self.period}.csv")
        )

    def run(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        catalog = get_catalog()
        boundary = catalog.get_boundary(self.park)
        cluster = get_cluster()

        if self.period == "historical":
            scenarios = ["Historical Climate"]
            time_slice = HISTORICAL_YEARS
        else:
            scenarios = ["SSP 2-4.5", "SSP 3-7.0", "SSP 5-8.5"]
            time_slice = FUTURE_YEARS

        data = get_climate_data(
            variables=[self.variable],
            scenarios=scenarios,
            boundary=boundary,
            time_slice=time_slice,
            timescale="monthly",
            backend="coiled",
            coiled_cluster=cluster,
        )

        df = data[self.variable]
        short = PARK_SHORT[self.park]
        df["park"] = short

        with self.output().open("w") as f:
            df.to_csv(f, index=False)


# ---------------------------------------------------------------------------
# Task 2: Combine historical + future into one CSV per (park, variable)
# ---------------------------------------------------------------------------
class CombinePeriodsTask(luigi.Task):
    """Concatenate historical and future CSVs into one file per variable."""
    park = luigi.Parameter()
    variable = luigi.Parameter()

    def requires(self):
        return {
            "historical": FetchClimateTask(
                park=self.park, variable=self.variable, period="historical"
            ),
            "future": FetchClimateTask(
                park=self.park, variable=self.variable, period="future"
            ),
        }

    def output(self):
        short = PARK_SHORT[self.park]
        return luigi.LocalTarget(
            os.path.join(OUTPUT_DIR, f"{short}_{self.variable}.csv")
        )

    def run(self):
        dfs = []
        for period, target in self.input().items():
            with target.open("r") as f:
                dfs.append(pd.read_csv(f))

        combined = pd.concat(dfs, ignore_index=True)

        with self.output().open("w") as f:
            combined.to_csv(f, index=False)


# ---------------------------------------------------------------------------
# Task 3: Compute T_Avg from T_Max and T_Min
# ---------------------------------------------------------------------------
class ComputeTAvgTask(luigi.Task):
    """Compute T_Avg = (T_Max + T_Min) / 2 for a park."""
    park = luigi.Parameter()

    def requires(self):
        return {
            "T_Max": CombinePeriodsTask(park=self.park, variable="T_Max"),
            "T_Min": CombinePeriodsTask(park=self.park, variable="T_Min"),
        }

    def output(self):
        short = PARK_SHORT[self.park]
        return luigi.LocalTarget(
            os.path.join(OUTPUT_DIR, f"{short}_T_Avg.csv")
        )

    def run(self):
        with self.input()["T_Max"].open("r") as f:
            tmax = pd.read_csv(f)
        with self.input()["T_Min"].open("r") as f:
            tmin = pd.read_csv(f)

        merge_cols = ["simulation", "time", "scenario", "timescale", "park"]
        merged = tmax.merge(tmin[merge_cols + ["T_Min"]], on=merge_cols)
        merged["T_Avg"] = (merged["T_Max"] + merged["T_Min"]) / 2

        with self.output().open("w") as f:
            merged.to_csv(f, index=False)


# ---------------------------------------------------------------------------
# Task 4: Top-level wrapper — run everything for all parks
# ---------------------------------------------------------------------------
class FullExtractionPipeline(luigi.WrapperTask):
    """Extract T_Max, T_Min, T_Avg, and Precip for all configured parks."""
    parks = luigi.ListParameter(default=list(PARK_SHORT.values()))

    def requires(self):
        # Resolve short names back to full names
        short_to_full = {v: k for k, v in PARK_SHORT.items()}
        park_names = [short_to_full.get(p, p) for p in self.parks]

        tasks = []
        for park in park_names:
            # Combined CSVs for each variable
            for var in VARIABLES:
                tasks.append(CombinePeriodsTask(park=park, variable=var))
            # T_Avg (depends on T_Max + T_Min being combined)
            tasks.append(ComputeTAvgTask(park=park))
        return tasks
