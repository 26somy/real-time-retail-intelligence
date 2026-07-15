# AI-Powered Real-Time Retail Operations Command Center

## Business Problem

Modern retail companies already have dashboards and reporting systems, but operations teams still struggle to react quickly to fast-changing business conditions across stores, websites, mobile apps, and delivery channels.

Critical issues such as inventory shortages, sudden demand spikes, store-level revenue drops, and abnormal product sales patterns often require manual investigation across multiple systems.

By the time teams identify the root cause, the business may have already lost revenue or delivered a poor customer experience.

## Project Goal

The goal of this project is to build a real-time retail operations intelligence platform that continuously processes sales and inventory events, detects operational risks, and supports faster business decision-making using AI-assisted insights.

The platform will help answer questions such as:

- Which products are at risk of stockout?
- Which stores are experiencing unusual revenue drops?
- Which products are seeing sudden demand spikes?
- What is the estimated revenue impact of inventory issues?
- How can an operations manager investigate business issues using AI-assisted insights?

## Planned Architecture

Retail sales and inventory events will be generated using Python and streamed into Azure Event Hub.

Azure Databricks will process these events using PySpark and Delta Lake. Data will be organized using the Medallion Architecture:

- Bronze layer: raw incoming events
- Silver layer: cleaned and standardized events
- Gold layer: business-ready analytics tables

Power BI will be used for dashboards, and an AI analytics layer will later be added to explain business issues in plain English.

## Technology Stack

- Python
- Azure Event Hub
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Delta Lake
- Azure Data Factory
- Power BI
- OpenAI / AI analytics layer

## Key Learning Goals

This project is designed to demonstrate:

- Real-time data ingestion
- Event-driven architecture
- Lakehouse design
- Bronze, Silver, and Gold data modeling
- PySpark transformations
- Delta Lake storage
- Business KPI creation
- Dashboard-ready data modeling
- AI-assisted operational analytics

## Implementation Status

### Completed

- Created Azure resource group: `rg-retail-command-center-dev`
- Created ADLS Gen2 storage account: `retailcmdlake26`
- Created ADLS containers:
  - `bronze`
  - `silver`
  - `gold`
  - `checkpoints`
- Designed event schemas for:
  - Sales events
  - Inventory events
  - Return events
- Built Python event producer:
  - `src/producers/retail_stream_app.py`
- Built Event Hub to Bronze consumer:
  - `src/consumers/eventhub_to_bronze_consumer.py`
- Successfully streamed events through Event Hub and landed them as JSON files in ADLS Bronze
- Created Databricks Bronze to Silver transformation notebook:
  - `notebooks/01_bronze_to_silver_transform.py`
- Parsed multiline JSON files from Bronze using PySpark
- Cleaned, typed, and deduplicated Bronze records
- Wrote Silver Delta tables for:
  - Sales
  - Inventory
  - Returns
- Created planned Silver to Gold transformation notebook:
  - `notebooks/02_silver_to_gold_transform.py`
- Documented Silver and Gold layer design

### In Progress / Planned

- Execute Silver to Gold transformation
- Create Gold Delta tables for:
  - Sales summary
  - Inventory risk
  - Return analysis
- Build Power BI dashboard from Gold tables
- Add orchestration using Databricks Jobs or Azure Data Factory
- Add production-grade incremental processing using Auto Loader and Delta MERGE
- Add data quality checks
- Add AI-assisted retail operations insights layer



