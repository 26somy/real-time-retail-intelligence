# Architecture

## Data Flow

Producers
(Store POS, Website, Mobile App, Inventory System)
        ↓
Azure Event Hub
        ↓
Azure Databricks Structured Streaming
        ↓
Bronze Layer (Raw Events)
        ↓
Silver Layer (Cleaned & Validated Events)
        ↓
Gold Layer (Business KPIs & Insights)
        ↓
Power BI Dashboards
        ↓
AI Operations Assistant

---

## Bronze Layer

Purpose:
- Store raw events exactly as received
- Preserve audit history
- Support replay and reprocessing

Examples:
- sales_events_bronze
- inventory_events_bronze
- return_events_bronze

---

## Silver Layer

Purpose:
- Remove duplicates
- Validate schema
- Standardize data
- Handle null values

Examples:
- sales_events_silver
- inventory_events_silver
- return_events_silver

---

## Gold Layer

Purpose:
- Business-ready analytics
- KPI calculations
- Dashboard consumption
- AI analytics

Examples:
- product_stockout_risk
- store_performance
- revenue_summary