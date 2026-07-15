# Event Schema Design

We have a retail stream app that simulates live retail systems. It creates sales, inventory, and return events using a defined schema, serializes them as JSON, routes each event type to the correct Azure Event Hub, and continuously publishes messages to simulate real-time retail operations.

## Sales Event

### Description

Generated whenever a customer completes a purchase through a store, website, or mobile application.

### Sample Payload

```json
{
  "event_type": "sales_event",
  "transaction_id": "TXN1001",
  "customer_id": "CUST501",
  "store_id": "STORE015",
  "product_id": "PROD100",
  "product_name": "Apple AirPods Pro",
  "quantity": 2,
  "unit_price": 199.99,
  "total_amount": 399.98,
  "payment_status": "SUCCESS",
  "event_timestamp": "2026-06-22T15:30:00Z"
}
```

## Inventory Event

### Description

Generated whenever inventory levels change due to sales, replenishment, returns, damage, or manual adjustments.

### Sample Payload

```json
{
  "event_type": "inventory_event",
  "inventory_event_id": "INV1001",
  "product_id": "PROD100",
  "store_id": "STORE015",
  "inventory_remaining": 42,
  "inventory_status": "LOW",
  "unit_price": 199.99,
  "event_timestamp": "2026-06-22T15:30:00Z"
}
```

## Producers

### Store POS System

Generates sales events whenever a customer purchases products in a physical store.

### E-commerce Website

Generates sales events from online purchases.

### Mobile Application

Generates sales events from purchases made through the mobile app.

### Inventory Management System

Generates inventory events whenever stock levels change due to replenishment, returns, damages, or adjustments.

## Consumers

### Azure Databricks Streaming

Consumes events from Azure Event Hub and processes them into Bronze, Silver, and Gold layers.

### Inventory Alert Engine

Consumes inventory-related insights and generates stockout alerts.

### Power BI

Consumes Gold-layer business tables for operational dashboards.

### AI Operations Assistant

Consumes business-ready data and answers operational questions in natural language.

## Event Flow

Store POS System
      ↓
Sales Event
      ↓
Azure Event Hub
      ↓
Azure Databricks Streaming
      ↓
Bronze Layer
      ↓
Silver Layer
      ↓
Gold Layer
      ↓
Power BI Dashboard
      ↓
AI Operations Assistant

## Business Questions Supported

### Inventory Questions

- Which products are likely to stock out soon?
- Which stores require replenishment?

### Revenue Questions

- Which stores are underperforming today?
- What products generate the most revenue?

### Demand Questions

- Which products are experiencing unusual demand spikes?
- What products are trending across stores?

### AI-Assisted Questions

- Why are sales down in a specific region?
- What inventory issues are affecting revenue?
- Which stores need immediate attention?