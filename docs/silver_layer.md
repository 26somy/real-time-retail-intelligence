# Silver Layer Transformation

## Purpose

The Silver layer converts raw Bronze JSON events into cleaned, typed, deduplicated Delta tables.

Bronze stores raw event files exactly as they were landed from the ingestion pipeline. Silver makes the data reliable and query-ready for Gold aggregations, SQL analysis, and BI reporting.

## Source

Bronze data is stored in ADLS Gen2:

- `bronze/sales`
- `bronze/inventory`
- `bronze/returns`

Files are partitioned by date:

```text
year=YYYY/month=MM/day=DD/
