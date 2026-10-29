# Notebook Guide: E-Commerce Medallion Pipeline

This file explains what each notebook does, how they connect, and the practices to follow. For Azure and Unity Catalog setup, see [README.md](./README.md).

## How the notebooks relate

```
 Event Hub (Kafka)                      ADLS Gen2: abfss://ecommerce@rtecommercestrg.dfs.core.windows.net
       |
       v
 [01_bronze] --writes--> /bronze  (+ /bronze/_checkpoint)
                              |
                              v
                        [02_silver] --writes--> /silver (+ /silver/_checkpoint)
                                                     |
                                                     v
                                               [03_gold] --writes--> /gold (+ /gold/_checkpoint)
                                                     |
                                                     v
                                   UC table ecommerce_bricks_7405616949273254.default.gold_orders_managed
                                                     |
                                                     v
                                     [gold_warehouse] SQL query (SQL Warehouse)
```

- Run order: **01_bronze -> 02_silver -> 03_gold**, then the SQL query.
- Each layer streams **incrementally** from the previous layer's Delta folder. The checkpoint stores progress, so each run only processes new data.
- All three use `trigger(availableNow=True)`: each run processes everything new, then stops. This is batch-style streaming, and it is what serverless compute supports.

---

## 01_bronze: raw ingestion

**Purpose:** Read order events from Azure Event Hubs through its Kafka endpoint and land them as-is (parsed JSON) in the bronze Delta folder.

**Input:** Event Hub `ecommerce-hubspace` on namespace `rt-ecommerce-namespace` (port 9093, SASL_SSL/PLAIN).
**Output:** Delta at `/bronze`, checkpoint at `/bronze/_checkpoint`.

**What it does**

1. Defines an explicit schema for the order JSON (`order_id`, `timestamp`, `customer_id`, `product_id`, `category`, `price`, `quantity`, `total_amount`, `city`, `state`, `country`, `latitude`, `longitude`, `delivery_status`).
2. Builds Kafka options (`kafka.bootstrap.servers`, `subscribe`, SASL JAAS config with `$ConnectionString`).
3. `readStream.format('kafka')` -> casts the binary `value` to string -> `from_json` with the schema -> flattens to columns.
4. Writes to Delta in `append` mode with `availableNow`.
5. Cell 2 displays the bronze table for a quick check.

**Key settings**

- `startingOffsets = latest`: on the **first** run (no checkpoint yet), only events sent after the stream starts are read. Use `earliest` to backfill existing events. Once a checkpoint exists, this setting is ignored.
- `failOnDataLoss = false`: keeps running if Event Hub retention has removed old offsets. This is convenient, but data loss is silent.

**Practices / improvements**

- **Security (action required):** the Event Hub connection string, including the SharedAccessKey for `RootManageSharedAccessKey`, is hard-coded in the notebook. Move it to a secret scope (`dbutils.secrets.get`), **regenerate that key in Azure**, and use a Listen-only policy.
- Bronze should be as raw as possible. Think about also keeping Kafka metadata (`partition`, `offset`, `timestamp` as `ingest_time`) and the raw `json` string for replay and debugging.
- `timestamp`, `latitude` and `longitude` are kept as strings here on purpose. Type conversion happens in silver.

---

## 02_silver: clean and enrich

**Purpose:** Turn raw bronze orders into trusted, typed, de-duplicated US orders.

**Input:** Delta stream from `/bronze`.
**Output:** Delta at `/silver`, checkpoint at `/silver/_checkpoint`.

**Transformations**

| Step           | Rule                                                                                      |
| -------------- | ----------------------------------------------------------------------------------------- |
| Type cast      | `timestamp` string -> timestamp (`to_timestamp`)                                          |
| Null handling  | `price` null -> 0.0; `quantity` null -> 1                                                 |
| Recompute      | `total_amount = price * quantity` (ignores the incoming value so it is always consistent) |
| Filter         | `country == 'USA'` and `state IS NOT NULL`                                                |
| De-duplication | 10-minute watermark on `timestamp` + `dropDuplicatesWithinWatermark(['order_id'])`        |

**Practices / improvements**

- `dropDuplicatesWithinWatermark` removes duplicate `order_id`s that arrive within 10 minutes of each other while keeping state small. Duplicates further apart will not be caught.
- Rows with an unparseable `timestamp` become null and are effectively dropped by the watermark. Think about sending invalid rows to a quarantine table.
- Defaulting `price` to 0.0 can understate revenue. Make sure this is the business rule you want, or flag those rows instead.

---

## 03_gold: business aggregates

**Purpose:** Produce sales KPIs per US state per 1-minute window for reporting.

**Input:** Delta stream from `/silver`.
**Output:** Delta at `/gold`, checkpoint at `/gold/_checkpoint`, and the UC table `default.gold_orders_managed`.

**Output columns:** `window_start`, `window_end`, `state`, `total_sales` (sum of `total_amount`), `total_items` (sum of `quantity`).

**What it does**

1. 1-minute watermark on `timestamp`.
2. `groupBy(window('timestamp','1 minute'), 'state')` -> sums.
3. Writes in `append` mode. A window is written only **after** the watermark passes its end, so the newest minute usually appears on the next run.
4. Cell 2 displays the gold Delta folder.
5. Cell 3 (`%sql`) runs `CREATE OR REPLACE TABLE default.gold_orders_managed AS SELECT * FROM delta.<gold path>`.

**Practices / improvements**

- **Cell 3 makes a one-time copy.** The managed table does not update as gold streams. You must re-run cell 3 after each gold run. Better options:
  - write the stream directly to a UC table with `.toTable('ecommerce_bricks_7405616949273254.default.gold_orders')`, or
  - register the gold folder once as an external table (`CREATE TABLE ... LOCATION 'abfss://.../gold'`), so it always shows the latest data.
- Cell 1 does not call `awaitTermination()`, so the next cells can run before the stream finishes. Add `.awaitTermination()` (as bronze and silver do) before reading gold or running cell 3.
- The 1-minute watermark drops events that arrive more than 1 minute late. Make it longer if late data is common.

---

## gold_warehouse (SQL query)

**Purpose:** Let analysts and BI tools read gold through a SQL Warehouse.
**Query:** `SELECT * FROM gold_orders_managed;` (catalog `ecommerce_bricks_7405616949273254`, schema `default`).
This is a good base for dashboards, for example sales by state over time.

---

## Other workspace items

- `Drafts/New Notebook 2026-10-26 12:32:07`: empty. It isn't used by the pipeline, so you can delete it.

---

## General practices for all notebooks

| Topic          | Practice                                                                                                                                                                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Secrets        | Never hard-code keys. Use `dbutils.secrets.get`.                                                                                                                                                                                                        |
| Checkpoints    | One unique checkpoint per stream. Never share them. If a code change breaks compatibility, use a new checkpoint (and reset the target). Folders like `_checkpoint` work inside the table folder, but a separate `/checkpoints/<layer>` path is cleaner. |
| Triggers       | `availableNow=True` on serverless. Schedule with a Lakeflow Job (bronze -> silver -> gold task dependencies).                                                                                                                                           |
| Paths          | Define paths once (a config cell or widgets) so they match across notebooks.                                                                                                                                                                            |
| Storage access | Every path needs a Unity Catalog external location. Do not use storage account keys.                                                                                                                                                                    |
| Imports        | Prefer `import pyspark.sql.functions as F` over `import *` (avoids shadowing Python `sum`, `min`, `max`).                                                                                                                                               |
| Tables         | Prefer Unity Catalog tables (`toTable`) over loose path-based Delta for discovery and governance.                                                                                                                                                       |
| Validation     | Keep a display/count cell after each layer, and check row counts bronze >= silver.                                                                                                                                                                      |
