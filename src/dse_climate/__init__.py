"""dse_climate — Climate data utilities for Cal-Adapt / LOCA2.

Public API:
    CatalogExplorer    — discover available climate data at runtime
    ParkCatalog        — search and retrieve NPS park boundaries

    get_climate_data   — fetch climate data (direct_s3, climakitae, or coiled)
    get_spatial_snapshot — fetch gridded spatial snapshots for plotting

    load_boundary      — load a shapefile boundary
    get_lat_lon_bounds — extract lat/lon bounding box from a boundary

    plot_spatial_comparison — 2×2 heatmap grid of spatial data
    export_to_csv      — export results to R-friendly CSV files

    VARIABLE_MAP       — human-readable variable name mappings
    SCENARIO_MAP       — scenario name → experiment_id mappings
"""

from dse_climate._core import (
    # Classes
    CatalogExplorer,
    ParkCatalog,
    # High-level API
    get_climate_data,
    get_spatial_snapshot,
    plot_spatial_comparison,
    export_to_csv,
    # Boundary helpers
    load_boundary,
    get_lat_lon_bounds,
    boundary_to_wkt,
    boundary_from_wkt,
    # Spatial processing
    clip_to_boundary,
    spatial_average,
    cosine_weighted_spatial_mean,
    # Temporal processing
    annual_aggregate,
    compute_anomalies,
    smooth,
    compute_t_avg,
    # Constants
    VARIABLE_MAP,
    SCENARIO_MAP,
    TIMESCALE_MAP,
    STANDARD_CRS,
)

__all__ = [
    "CatalogExplorer",
    "ParkCatalog",
    "get_climate_data",
    "get_spatial_snapshot",
    "plot_spatial_comparison",
    "export_to_csv",
    "load_boundary",
    "get_lat_lon_bounds",
    "boundary_to_wkt",
    "boundary_from_wkt",
    "clip_to_boundary",
    "spatial_average",
    "cosine_weighted_spatial_mean",
    "annual_aggregate",
    "compute_anomalies",
    "smooth",
    "compute_t_avg",
    "VARIABLE_MAP",
    "SCENARIO_MAP",
    "TIMESCALE_MAP",
    "STANDARD_CRS",
]
