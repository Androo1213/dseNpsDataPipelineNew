# Pipeline Stages

## Task graph

```
run_pipeline.py
  │
  ├── fuzzy search park name
  ├── start Coiled cluster
  │
  └── ExtractionPipeline (Luigi WrapperTask)
        │
        ├── FetchClimateTask (T_Max, Historical)  ──→  data/csv/JoshuaTree/T_Max_Historical_Climate.csv
        ├── FetchClimateTask (T_Max, SSP 2-4.5)   ──→  data/csv/JoshuaTree/T_Max_SSP_2-4.5.csv
        ├── FetchClimateTask (T_Min, Historical)   ──→  data/csv/JoshuaTree/T_Min_Historical_Climate.csv
        ├── FetchClimateTask (T_Min, SSP 2-4.5)    ──→  data/csv/JoshuaTree/T_Min_SSP_2-4.5.csv
        │
        └── ComputeTAvgTask (SSP 2-4.5)            ──→  data/csv/JoshuaTree/T_Avg_SSP_2-4.5.csv
              │
              ├── requires: T_Max SSP 2-4.5 CSV
              └── requires: T_Min SSP 2-4.5 CSV
```

(Diagram shows a 2-variable, 2-scenario run. Actual graph scales with your `--variable` and `--scenario` flags.)

## What each stage does

### 1. FetchClimateTask

This is where the real work happens. For one (park, variable, scenario) combo it:
- Loads the park boundary from the NPS shapefile
- Calls `get_climate_data()` which dispatches work to Coiled workers
- Coiled workers open Zarr stores on S3, subset by time/space, convert units, spatially average, and return a DataFrame
- Saves the DataFrame as a CSV

Each fetch is one CSV file. If you request T_Max + T_Min across 4 scenarios, that's 8 FetchClimateTasks.

### 2. ComputeTAvgTask

Pure local math, no Coiled. Reads the T_Max and T_Min CSVs, merges them, computes `(T_Max + T_Min) / 2`, saves T_Avg CSV. Only runs if you requested both T_Max and T_Min.

### 3. ExtractionPipeline

Just a wrapper that says "I need all these FetchClimateTasks and ComputeTAvgTasks done." It doesn't do any work itself.

## Where Luigi actually helps

Luigi's main trick: before running a task, it checks if the output file already exists. If it does, skip it.

**When this matters:**
- You run `--variable all --scenario all`, it finishes Historical + SSP 2-4.5, then your laptop disconnects. You re-run the same command. Luigi sees the Historical and SSP 2-4.5 CSVs exist, skips them, and only fetches SSP 3-7.0 and SSP 5-8.5.
- You already ran T_Max for a park last week. Now you want T_Min too. Run with `--variable T_Min` and it only fetches the new stuff.

**When this doesn't really matter:**
- A single FetchClimateTask either completes fully or doesn't produce a CSV at all. There's no partial CSV. So if Coiled times out mid-fetch, that entire task reruns from scratch — Luigi can't resume *within* a fetch, only *between* fetches.

## Honest assessment

The bottleneck in this pipeline is the Coiled fetch — that's where all the time and money goes. Each FetchClimateTask takes 30s-5min depending on the data volume and cluster size. The ComputeTAvgTask takes < 1 second.

**What Luigi gives us:**
- Resume across tasks (big win when doing multi-variable multi-scenario runs)
- Clean separation of concerns (each task = one CSV)
- CLI that's easier to script than a notebook

**What Luigi can't help with:**
- Coiled cluster failures (timeout, spot instance preemption) — if the cluster dies mid-fetch, that task fails and you re-run
- Cost — still the same number of Coiled worker-hours regardless of whether Luigi or a notebook orchestrates it
- Within-task retries — Luigi marks a task as failed if it throws, it doesn't auto-retry by default (you can add `retry_count` but it'd just re-submit the same Coiled job)

**Bottom line:** Luigi is most valuable when you're doing big multi-scenario extractions where you want to be able to walk away and pick up later. For a single quick fetch (`--variable T_Max --scenario historical`), it's basically the same as running the notebook — Luigi just adds a thin wrapper. The real win is that the CLI is scriptable and the resume-across-tasks behavior means you don't lose work on long runs.
