#!/usr/bin/env python3
"""Climate data extraction pipeline with fuzzy park search.

Usage:
    python run_pipeline.py "joshua tree" --variable T_Max --scenario historical
    python run_pipeline.py "yosemite" --variable Precip --scenario all
    python run_pipeline.py "mojave" --variable all --scenario all --output plot
"""

import argparse
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "lib"))

from pipelines.config import (
    NPS_SHAPEFILE,
    COILED_REGION,
    COILED_N_WORKERS,
    COILED_WORKER_MEMORY,
    PLOT_DIR,
    to_short_name,
)

VARIABLE_CHOICES = ["T_Max", "T_Min", "Precip", "all"]
SCENARIO_CHOICES = ["historical", "ssp245", "ssp370", "ssp585", "all"]

SCENARIO_EXPAND = {
    "historical": ["Historical Climate"],
    "ssp245": ["SSP 2-4.5"],
    "ssp370": ["SSP 3-7.0"],
    "ssp585": ["SSP 5-8.5"],
    "all": ["Historical Climate", "SSP 2-4.5", "SSP 3-7.0", "SSP 5-8.5"],
}


def fuzzy_find_park(query: str) -> str:
    """Fuzzy search for a park and return the exact name."""
    from andrewAdaptLibrary import ParkCatalog

    catalog = ParkCatalog(NPS_SHAPEFILE)
    results = catalog.search(query)

    if not results:
        print(f"No parks found matching '{query}'")
        sys.exit(1)

    # results is list of (name, score) tuples — take the top match
    match, score = results[0]
    print(f'Found: "{match}" (score: {score:.0f}/100)')
    return match


def main():
    parser = argparse.ArgumentParser(
        description="Extract climate data for a national park",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""examples:
  python run_pipeline.py "joshua tree" --variable T_Max --scenario historical
  python run_pipeline.py "yosemite" --variable Precip --scenario all
  python run_pipeline.py "mojave" --variable all --scenario ssp585 --output plot""",
    )
    parser.add_argument(
        "park",
        help="Park name (fuzzy searched — e.g. 'yosemite', 'grand canyon')",
    )
    parser.add_argument(
        "--variable", required=True,
        choices=VARIABLE_CHOICES,
        help="Variable to extract (required)",
    )
    parser.add_argument(
        "--scenario", required=True,
        choices=SCENARIO_CHOICES,
        help="Scenario to extract (required)",
    )
    parser.add_argument(
        "--timescale", default="monthly",
        choices=["monthly", "daily", "yearly"],
        help="Temporal resolution (default: monthly)",
    )
    parser.add_argument(
        "--output", default="csv",
        choices=["csv", "plot"],
        help="Output format (default: csv)",
    )
    parser.add_argument(
        "--workers", type=int, default=COILED_N_WORKERS,
        help=f"Coiled workers (default: {COILED_N_WORKERS})",
    )
    args = parser.parse_args()

    # Fuzzy search
    park_name = fuzzy_find_park(args.park)
    short_name = to_short_name(park_name)

    # Expand variable/scenario
    variables = ["T_Max", "T_Min", "Precip"] if args.variable == "all" else [args.variable]
    scenarios = SCENARIO_EXPAND[args.scenario]

    print(f"Park:      {park_name} ({short_name})")
    print(f"Variables: {variables}")
    print(f"Scenarios: {scenarios}")
    print(f"Timescale: {args.timescale}")
    print(f"Output:    {args.output}")
    print()

    # Start Coiled cluster
    import coiled
    print(f"Starting Coiled cluster ({args.workers} workers)...")
    t0 = time.perf_counter()

    cluster = coiled.Cluster(
        name=f"extract-{short_name.lower()}",
        region=COILED_REGION,
        n_workers=args.workers,
        worker_memory=COILED_WORKER_MEMORY,
        spot_policy="spot_with_fallback",
        idle_timeout="20 minutes",
        package_sync=True,
    )
    client = cluster.get_client()
    print(f"Cluster ready in {time.perf_counter() - t0:.0f}s "
          f"({len(client.scheduler_info()['workers'])} workers)\n")

    # Run Luigi pipeline
    import luigi
    from pipelines.climate_extraction import ExtractionPipeline, set_cluster

    set_cluster(cluster)

    success = luigi.build(
        [ExtractionPipeline(
            park=park_name,
            variables=variables,
            scenarios=scenarios,
            timescale=args.timescale,
        )],
        local_scheduler=True,
        log_level="INFO",
    )

    # Plot mode: generate spatial heatmap after extraction
    if args.output == "plot" and success:
        print("\nGenerating spatial plot...")
        from andrewAdaptLibrary import (
            ParkCatalog, get_spatial_snapshot, plot_spatial_comparison,
        )
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        catalog = ParkCatalog(NPS_SHAPEFILE)
        boundary = catalog.get_boundary(park_name)

        snapshot = get_spatial_snapshot(
            variable=variables[0],
            scenarios=scenarios,
            boundary=boundary,
            time_period=(2050, 2069),
            coiled_cluster=cluster,
        )

        fig = plot_spatial_comparison(
            snapshot, boundary=boundary,
            title=f"{park_name} — {variables[0]}",
        )

        os.makedirs(PLOT_DIR, exist_ok=True)
        plot_path = os.path.join(PLOT_DIR, f"{short_name}_{variables[0]}.png")
        fig.savefig(plot_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved: {plot_path}")

    cluster.close()
    print(f"\nPipeline {'succeeded' if success else 'FAILED'}")
    print(f"Total time: {time.perf_counter() - t0:.0f}s")


if __name__ == "__main__":
    main()
