# Gold Layer Design

## Purpose

The Gold layer converts cleaned Silver event data into business-ready analytics tables.

Silver contains cleaned event-level records. Gold contains aggregated and enriched metrics that can be used directly by dashboards, reporting tools, and AI agents.

## Planned Gold Tables

### 1. `gold_sales_summary`

Business question:

> How much did we sell by date, store, product, and category?

Source:

```text
silver/sales