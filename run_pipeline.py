#!/usr/bin/env python3
"""Run the climate data extraction pipeline.

Usage:
    python run_pipeline.py                          # all parks
    python run_pipeline.py --parks JoshuaTree       # single park
    python run_pipeline.py --parks JoshuaTree Mojave --workers 2
"""

import argparse
import time
import coiled
import luigi

from pipelines.config import (
    COILED_REGION,
    COILED_N_WORKERS,
    COILED_WORKER_MEMORY,
    PARK_SHORT,
)
from pipelines.climate_extraction import FullExtractionPipeline, set_cluster


def main():
    parser = argparse.ArgumentParser(description="Climate data extraction pipeline")
    parser.add_argument(
        "--parks",
        nargs="+",
        default=list(PARK_SHORT.values()),
        help="Short park names (e.g. JoshuaTree Mojave)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=COILED_N_WORKERS,
        help=f"Number of Coiled workers (default: {COILED_N_WORKERS})",
    )
    args = parser.parse_args()

    # Start Coiled cluster
    print(f"Starting Coiled cluster ({args.workers} workers)...")
    t0 = time.perf_counter()

    cluster = coiled.Cluster(
        name="luigi-extraction",
        region=COILED_REGION,
        n_workers=args.workers,
        worker_memory=COILED_WORKER_MEMORY,
        spot_policy="spot_with_fallback",
        idle_timeout="30 minutes",
        package_sync=True,
    )
    client = cluster.get_client()
    print(f"Cluster ready in {time.perf_counter() - t0:.0f}s "
          f"({len(client.scheduler_info()['workers'])} workers)")

    # Make cluster available to Luigi tasks
    set_cluster(cluster)

    # Run pipeline
    success = luigi.build(
        [FullExtractionPipeline(parks=args.parks)],
        local_scheduler=True,
        log_level="INFO",
    )

    cluster.close()
    print(f"\nPipeline {'succeeded' if success else 'FAILED'}")
    print(f"Total time: {time.perf_counter() - t0:.0f}s")


if __name__ == "__main__":
    main()
