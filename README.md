# City of Cape Town Data Engineering Challenge

## Overview

This project implements the **Data Engineering tasks** of the City of Cape Town challenge.

The focus is on building a maintainable data engineering pipeline using:

* Python
* AWS S3 data sources
* Dagster
* Docker
* Modular extraction, transformation and validation components
* Data quality checks and runtime metrics

This implementation focuses on the **engineering pipeline and code quality** rather than building a complete cloud ETL platform, medallion architecture, or traditional data warehouse.

Some outputs remain in JSON format where the challenge only requires validation, metrics or evidence of processing. They are intentionally not converted into tabular/warehouse structures where this was not required by the assessment.

## Run the Project

From the project root:

```bash
docker compose down
docker compose build
docker compose up -d
```

Run all completed Data Engineering tasks:

```bash
docker compose exec dagster python /opt/dagster/run_all.py
```

The pipeline runs:

```text
Task 1 → Task 2 → Task 5.1
```

The pipeline stops if a task fails.

## Outputs

Results are written to:

```text
output/
```

On Windows:

```powershell
Get-ChildItem .\output
```

The output directory is mounted between Docker and the host, so results are available locally after execution.

## Dagster

Dagster provides orchestration and visibility into the pipeline.

The UI is available at:

```text
http://localhost:3000
```

The `run_all.py` script provides a reproducible command-line execution path without requiring manual execution through the UI.

## Engineering Approach

The project is intentionally modular:

```text
Extraction
    ↓
Transformation
    ↓
Validation
    ↓
Output
    ↓
Monitoring
```

The implementation prioritises:

* Clean and maintainable code
* Reusable modules
* Data quality validation
* Failure thresholds
* Runtime measurement
* Reproducible Docker execution
* Separation of pipeline logic from orchestration

## Scope

The submission focuses specifically on the **Data Engineering requirements** of the assessment.

Cloud ETL infrastructure, medallion architecture and a traditional data warehouse were not introduced because they were outside the requirements of the Data Engineering tasks.
