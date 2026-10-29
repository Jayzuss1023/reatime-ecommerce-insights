# Real-Time E-Commerce Streaming on Azure + Databricks (For Dummies)

This guide walks you through the full setup, start to finish, of a real-time e-commerce pipeline:

```
Order events --> Azure Event Hubs (Kafka endpoint)
             --> Databricks 01_bronze  (raw Delta on ADLS: /bronze)
             --> Databricks 02_silver  (cleaned Delta on ADLS: /silver)
             --> Databricks 03_gold    (aggregated Delta on ADLS: /gold)
             --> Unity Catalog table default.gold_orders_managed
             --> SQL Warehouse query  (gold_warehouse)
```

Notebook details live in [NOTEBOOKS.md](./NOTEBOOKS.md).

---

## 0. Glossary (plain English)

| Term                                  | What it actually is                                                                                                         |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Resource Group (RG)                   | A folder in Azure that holds related resources. Here: `rt-ecommerce-hub`.                                                   |
| Event Hubs Namespace                  | The "server" that hosts one or more event streams. Here: `rt-ecommerce-namespace`.                                          |
| Event Hub                             | One stream/topic inside the namespace. Here: `ecommerce-hubspace`.                                                          |
| ADLS Gen2                             | Azure Data Lake Storage. A storage account with _Hierarchical Namespace_ turned on (real folders). Here: `rtecommercestrg`. |
| Container                             | A top-level bucket inside the storage account. Here: `ecommerce`.                                                           |
| Access Connector for Azure Databricks | A managed identity Databricks uses to read/write your storage. Here: `ecommerce-connector`.                                 |
| Unity Catalog (UC)                    | Databricks' governance layer: who can access which data and storage.                                                        |
| Storage Credential                    | A UC object that wraps the Access Connector identity. Here: `ecommerce-cred`.                                               |
| External Location                     | A UC object that says "this credential may access this storage path".                                                       |
| Medallion (Bronze/Silver/Gold)        | Raw -> Clean -> Business-ready layers.                                                                                      |
| Checkpoint                            | A folder where a stream remembers what it has already processed.                                                            |
| Watermark                             | How long a stream waits for late events before finalizing results.                                                          |

---

## 1. Azure prerequisites

You need an Azure subscription where you can create resources and assign roles (Owner, or Contributor + User Access Administrator).

### 1.1 Create a Resource Group

1. Azure Portal -> **Resource groups** -> **Create**.
2. Name: `rt-ecommerce-hub`. Pick one region and use it for everything below.

### 1.2 Create the Event Hubs Namespace and Event Hub

1. Portal -> **Event Hubs** -> **Create**.
2. Namespace name: `rt-ecommerce-namespace`, RG: `rt-ecommerce-hub`.
3. Pricing tier: **Standard or higher**. The Basic tier does **not** support the Kafka endpoint.
4. Open the namespace -> **+ Event Hub** -> name `ecommerce-hubspace` (partitions: 2-4 is fine for learning).
5. Get a connection string:
   - Best practice: create a **Shared access policy** on the _Event Hub_ (not the namespace) with **Listen** only, e.g. `databricks-listen`.
   - Copy its **Connection string-primary key**.
   - Avoid using `RootManageSharedAccessKey` (it has full Manage rights).

> Kafka facts you will need: bootstrap server = `<namespace>.servicebus.windows.net:9093`, topic = Event Hub name, username = `$ConnectionString`, password = the connection string.

### 1.3 Create the ADLS Gen2 storage account

1. Portal -> **Storage accounts** -> **Create**.
2. Name: `rtecommercestrg`, RG: `rt-ecommerce-hub`, Performance: Standard, Redundancy: LRS is fine for dev.
3. **Advanced tab -> enable Hierarchical namespace** (this is what makes it ADLS Gen2). Required.
4. After creation: **Containers** -> **+ Container** -> `ecommerce`.
5. (Optional) Create folders `bronze`, `silver`, `gold`, `checkpoints`. Spark will also create them on the first write.

### 1.4 Create the Access Connector for Azure Databricks

1. Portal -> search **Access Connector for Azure Databricks** -> **Create**.
2. Name: `ecommerce-connector`, RG: `rt-ecommerce-hub`, same region as the workspace.
3. Copy its **Resource ID** (looks like `/subscriptions/<sub>/resourceGroups/rt-ecommerce-hub/providers/Microsoft.Databricks/accessConnectors/ecommerce-connector`).

### 1.5 Grant the connector access to storage (IAM)

On the **storage account** `rtecommercestrg` -> **Access control (IAM)** -> **Add role assignment** -> Members: _Managed identity_ -> select `ecommerce-connector`. Assign:

| Role                                                                   | Why                                                             |
| ---------------------------------------------------------------------- | --------------------------------------------------------------- |
| Storage Blob Data Contributor                                          | Read/write Delta files. **Required.**                           |
| Storage Queue Data Contributor                                         | Needed for UC _file events_ (managed queues).                   |
| EventGrid EventSubscription Contributor (on the RG or storage account) | Needed for UC _file events_ to create Event Grid subscriptions. |

Role assignments can take 5-10 minutes to take effect.

> If enabling **file events** on an external location fails with a _File Events Resource Provision_ error, it is almost always one of the last two roles missing, or the `Microsoft.EventGrid` resource provider not being registered on the subscription (Subscription -> Resource providers -> register `Microsoft.EventGrid`).

### 1.6 Azure Databricks workspace

1. Portal -> **Azure Databricks** -> **Create** -> **Premium** tier (Unity Catalog requires Premium).
2. Same region as the storage account.
3. Make sure the workspace is attached to a Unity Catalog metastore (new workspaces are attached automatically).

---

## 2. Databricks / Unity Catalog configuration

You can do these steps in **Catalog Explorer -> External data** (UI) or with SQL. You need metastore admin or `CREATE STORAGE CREDENTIAL` / `CREATE EXTERNAL LOCATION` privileges.

### 2.1 Storage Credential

Catalog Explorer -> **External data -> Credentials -> Create credential**:

- Type: **Azure Managed Identity**
- Name: `ecommerce-cred`
- Access connector ID: the Resource ID from step 1.4

### 2.2 External Locations

Every ADLS path that a notebook reads or writes **must** be covered by an external location. Otherwise you get errors like `Failure to initialize configuration for storage account ... fs.azure.account.key`.

Options:

- **Simple:** one location at the container root `abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/` covers bronze, silver, gold and checkpoints.
- **Granular (this project):** one location per layer:

| External location                 | URL                                                             | Credential       |
| --------------------------------- | --------------------------------------------------------------- | ---------------- |
| `ecommerce-excred`                | `abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/bronze` | `ecommerce-cred` |
| `silver`                          | `abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/silver` | `ecommerce-cred` |
| gold location (create if missing) | `abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/gold`   | `ecommerce-cred` |

SQL equivalent:

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS gold
  URL 'abfss://ecommerce@rtecommercestrg.dfs.core.windows.net/gold'
  WITH (STORAGE CREDENTIAL `ecommerce-cred`);

GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION gold TO `<user-or-group>`;
```

Use **Test connection** in the UI after creating each location.

### 2.3 Store the Event Hub secret safely

**Never paste connection strings into notebooks.** Anyone with read access to the notebook (or its revision history) can see it.

Use a Databricks secret scope (Databricks CLI):

```bash
databricks secrets create-scope ecommerce
databricks secrets put-secret ecommerce eventhub-conn-str --string-value "<EVENT_HUB_CONNECTION_STRING>"
```

Replace the whole `<EVENT_HUB_CONNECTION_STRING>` (brackets included) with your real value. Then in the notebook:

```python
event_hub_conn_str = dbutils.secrets.get("ecommerce", "eventhub-conn-str")
```

(Azure Key Vault-backed secret scopes work the same way.)

### 2.4 Catalog / schema for tables

This project registers the final table in catalog `ecommerce_bricks_7405616949273254`, schema `default`. Grant consumers:

```sql
GRANT USE CATALOG ON CATALOG ecommerce_bricks_7405616949273254 TO `<group>`;
GRANT USE SCHEMA, SELECT ON SCHEMA ecommerce_bricks_7405616949273254.default TO `<group>`;
```

### 2.5 Compute

- **Notebooks:** Serverless (or a UC-enabled cluster in Standard/Dedicated access mode). On serverless, streaming must use `.trigger(availableNow=True)`.
- **SQL queries:** a SQL Warehouse (the `gold_warehouse` query uses one).

---

## 3. Run the pipeline end to end

1. **Send events** into `ecommerce-hubspace` (a producer script or the Event Hubs _Data Explorer_ "Send events"). Each event is a JSON order, e.g.:
   ```json
   {
     "order_id": "o-1",
     "timestamp": "2026-10-29T17:13:05",
     "customer_id": "c-9",
     "product_id": "p-3",
     "category": "Electronics",
     "price": 499.0,
     "quantity": 2,
     "total_amount": 998.0,
     "city": "Austin",
     "state": "TX",
     "country": "USA",
     "latitude": "30.26",
     "longitude": "-97.74",
     "delivery_status": "Shipped"
   }
   ```
2. Run **01_bronze** -> raw orders land in `/bronze`.
3. Run **02_silver** -> cleaned, de-duplicated US orders land in `/silver`.
4. Run **03_gold** -> per-state, per-minute sales land in `/gold` and get registered as `default.gold_orders_managed`.
5. Open the **gold_warehouse** query in the SQL Editor -> `SELECT * FROM gold_orders_managed;`
6. To automate it, create a **Lakeflow Job** with three tasks (bronze -> silver -> gold) that depend on each other, scheduled as often as you need.

---

## 4. Troubleshooting cheat sheet

| Symptom                                                        | Likely cause                                                                     | Fix                                                                      |
| -------------------------------------------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `fs.azure.account.key` / `Failure to initialize configuration` | Path not covered by an external location                                         | Create an external location for that path (2.2).                         |
| `403 AuthorizationPermissionMismatch`                          | Connector missing Storage Blob Data Contributor                                  | Add the IAM role (1.5), wait 10 min.                                     |
| File Events Resource Provision failed                          | Missing Queue/EventGrid roles or provider not registered                         | See note in 1.5.                                                         |
| Kafka `TimeoutException` / no data                             | Basic tier, wrong port, firewall, or `startingOffsets=latest` with no new events | Use Standard tier, port 9093, send new events or use `earliest`.         |
| Serverless: trigger not supported                              | Default/processingTime trigger                                                   | Use `.trigger(availableNow=True)`.                                       |
| Gold missing the newest minute                                 | Watermark not yet passed                                                         | Expected; it appears on the next run once newer events arrive.           |
| Stream ignores new logic after code change                     | Old checkpoint state                                                             | Use a new checkpoint folder (and clear target) for incompatible changes. |

---

## 5. Security checklist

- [ ] Event Hub connection string stored in a secret scope, not in code.
- [ ] Any key that was ever pasted into a notebook has been **rotated** in Azure (Shared access policies -> Regenerate key).
- [ ] Listen-only SAS policy used by Databricks.
- [ ] Access to storage only through UC external locations (no account keys in Spark config).
- [ ] Least-privilege grants on external locations and schemas.
