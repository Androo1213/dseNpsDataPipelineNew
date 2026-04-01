"""Luigi pipeline for climate data extraction.

Each task writes a CSV; Luigi skips tasks whose output files already exist.
Tasks accept dynamic parameters so the CLI controls what gets fetched.
"""

import os
import sys
import luigi
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "lib"))

from andrewAdaptLibrary import (
    ParkCatalog,
    get_climate_data,
)
from pipelines.config import (
    NPS_SHAPEFILE,
    OUTPUT_DIR,
    HISTORICAL_YEARS,
    FUTURE_YEARS,
    to_short_name,
)


_catalog = None
_cluster = None


def get_catalog():
    global _catalog
    if _catalog is None:
        _catalog = ParkCatalog(NPS_SHAPEFILE)
    return _catalog


def get_cluster():
    global _cluster
    return _cluster


def set_cluster(cluster):
    global _cluster
    _cluster = cluster


class FetchClimateTask(luigi.Task):
    """Fetch climate data for one (park, variable, scenario) combo."""
    park = luigi.Parameter()
    variable = luigi.Parameter()
    scenario = luigi.Parameter()      # friendly name e.g. "Historical Climate"
    timescale = luigi.Parameter(default="monthly")

    def output(self):
        short = to_short_name(self.park)
        scen_short = self.scenario.replace(" ", "_")
        return luigi.LocalTarget(
            os.path.join(OUTPUT_DIR, short, f"{self.variable}_{scen_short}.csv")
        )

    def run(self):
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        catalog = get_catalog()
        boundary = catalog.get_boundary(self.park)

        if self.scenario == "Historical Climate":
            time_slice = HISTORICAL_YEARS
        else:
            time_slice = FUTURE_YEARS

        data = get_climate_data(
            variables=[self.variable],
            scenarios=[self.scenario],
            boundary=boundary,
            time_slice=time_slice,
            timescale=self.timescale,
            backend="coiled",
            coiled_cluster=get_cluster(),
        )

        df = data[self.variable]
        df["park"] = to_short_name(self.park)

        out_path = self.output().path
        df.to_csv(out_path, index=False)


class ComputeTAvgTask(luigi.Task):
    """Compute T_Avg = (T_Max + T_Min) / 2 for one scenario."""
    park = luigi.Parameter()
    scenario = luigi.Parameter()
    timescale = luigi.Parameter(default="monthly")

    def requires(self):
        return {
            "T_Max": FetchClimateTask(
                park=self.park, variable="T_Max",
                scenario=self.scenario, timescale=self.timescale,
            ),
            "T_Min": FetchClimateTask(
                park=self.park, variable="T_Min",
                scenario=self.scenario, timescale=self.timescale,
            ),
        }

    def output(self):
        short = to_short_name(self.park)
        scen_short = self.scenario.replace(" ", "_")
        return luigi.LocalTarget(
            os.path.join(OUTPUT_DIR, short, f"T_Avg_{scen_short}.csv")
        )

    def run(self):
        tmax = pd.read_csv(self.input()["T_Max"].path)
        tmin = pd.read_csv(self.input()["T_Min"].path)

        merge_cols = ["simulation", "time", "scenario", "timescale", "park"]
        merged = tmax.merge(tmin[merge_cols + ["T_Min"]], on=merge_cols)
        merged["T_Avg"] = (merged["T_Max"] + merged["T_Min"]) / 2

        merged.to_csv(self.output().path, index=False)


class ExtractionPipeline(luigi.WrapperTask):
    """Top-level task: fetch requested variables/scenarios for one park."""
    park = luigi.Parameter()
    variables = luigi.ListParameter()
    scenarios = luigi.ListParameter()
    timescale = luigi.Parameter(default="monthly")

    def requires(self):
        tasks = []
        for scenario in self.scenarios:
            for var in self.variables:
                tasks.append(FetchClimateTask(
                    park=self.park, variable=var,
                    scenario=scenario, timescale=self.timescale,
                ))
            # Auto-compute T_Avg if both T_Max and T_Min are requested
            if "T_Max" in list(self.variables) and "T_Min" in list(self.variables):
                tasks.append(ComputeTAvgTask(
                    park=self.park, scenario=scenario,
                    timescale=self.timescale,
                ))
        return tasks
