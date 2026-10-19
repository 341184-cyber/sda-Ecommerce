# StreamCart Analytics

**Real-Time E-Commerce Intelligence Dashboard | End-to-End Streaming Analytics**

StreamCart Analytics is an academic prototype that demonstrates how historical e-commerce order data can be transformed into order-level events, streamed through Apache Kafka, stored in MongoDB Atlas, and explored through MongoDB Atlas Charts.

> **Important scope note:** The project uses the public Olist Brazilian E-Commerce dataset from 2016–2018. The producer replays historical records one at a time to simulate event arrival; it is not connected to a live retailer or live checkout system. Dashboard metrics describe the available historical records and should not be interpreted as live production alerts or verified financial savings.

## Table of contents

- [Business problem](#business-problem)
- [Architecture](#architecture)
- [Technology stack](#technology-stack)
- [Repository contents](#repository-contents)
- [Data sources and schema](#data-sources-and-schema)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Run the pipeline](#run-the-pipeline)
- [Dashboard and business metrics](#dashboard-and-business-metrics)
- [Verify the pipeline](#verify-the-pipeline)
- [Troubleshooting](#troubleshooting)
- [Limitations and interpretation notes](#limitations-and-interpretation-notes)
- [Security](#security)
- [Acknowledgements and license](#acknowledgements-and-license)

## Business problem

E-commerce teams need to understand order volume, merchandise value, cancellations, and delivery-related costs across time and customer geographies. If order records are only analyzed in periodic batch reports, changes may not become visible until the next reporting cycle. A streaming-style pipeline can make incoming records available for analysis as they are processed, helping analysts monitor changing patterns and investigate areas that may require operational attention.

This prototype demonstrates that pipeline pattern using a public historical dataset. It is intended to support learning and demonstration of event streaming, data integration, persistence, and dashboard design—not to claim that business users are receiving live transactions from an active retailer.

## Architecture

```text
Olist CSV files
      |
      v
preprocess.py
(join, aggregate, clean and sort records)
      |
      v
data/processed/orders_stream.csv
      |
      v
producer.py
(publishes one JSON order event at a time)
      |
      v
Apache Kafka: ecommerce-orders
      |
      v
consumer.py  <----  database.py
(parse event, add processing timestamp, upsert by order_id)
      |
      v
MongoDB Atlas: ecommerce_streaming.orders
      |
      v
MongoDB Atlas Charts
(monthly trends, status mix and freight analysis)
```

### Event flow

1. `preprocess.py` combines the source tables into a chronological, order-level CSV.
2. `producer.py` reads the processed CSV and sends one JSON message per order to the Kafka topic `ecommerce-orders`. It waits approximately one second between messages to simulate arrivals.
3. `consumer.py` subscribes to the topic, converts `order_purchase_timestamp` to a Python datetime, adds `stream_processed_at`, and writes the record to MongoDB Atlas.
4. The consumer uses an upsert keyed by `order_id`, so replaying a previously processed order updates that document instead of blindly inserting another copy of the same order.
5. MongoDB Atlas Charts uses the stored collection as the dashboard data source.

## Technology stack

- **Python** — preprocessing, event production, and event consumption
- **pandas** — reading, joining, aggregating, and sorting CSV data
- **Apache Kafka** — event-streaming layer
- **Docker Compose** — runs Kafka locally
- **kafka-python** — Kafka producer and consumer clients
- **MongoDB Atlas** — cloud document database
- **PyMongo** — MongoDB connectivity and updates
- **python-dotenv** — loading environment variables from `.env`
- **certifi** — trusted CA certificates for the MongoDB TLS connection
- **MongoDB Atlas Charts** — visualization and dashboarding

## Repository contents

The public repository currently contains the following primary files:

| File | Purpose |
|---|---|
| `preprocess.py` | Reads the four Olist source CSVs, joins them, aggregates item-level values to order level, and writes `data/processed/orders_stream.csv`. |
| `producer.py` | Publishes order records to Kafka topic `ecommerce-orders`. |
| `consumer.py` | Consumes Kafka messages, parses timestamps, adds `stream_processed_at`, and upserts records into MongoDB. |
| `database.py` | Loads MongoDB configuration from `.env`, opens the connection, and exposes a connection test. |
| `docker-compose.yml` | Starts a local single-node Kafka broker. |
| `fix_dates.py` | A utility script related to date conversion. Review the script and back up the collection before running it; it is not required for the normal pipeline. |
| `orders_stream.csv` | A processed order dataset currently present at the repository root. See the path note below. |
| `Real-Time E-Commerce Intelligence Dashboard (1).charts` | Exported Atlas Charts dashboard configuration; it is not a hosted dashboard or a database backup. |

### Important data-path note

The current repository includes `orders_stream.csv` at the repository root, while `producer.py` expects the file at `data/processed/orders_stream.csv`. Also, `preprocess.py` expects the four original Olist CSVs inside `data/raw/`. Set up one of the following paths before running the pipeline:

- **Use the processed CSV already in the repository:** create `data/processed/` and copy the root-level file there, as shown in [Setup](#setup).
- **Rebuild from the original data:** download the four source files from Olist, place them in `data/raw/`, and run `preprocess.py`.

The repository should ideally be updated so the committed file layout, script paths, dependency list, and README remain consistent.

## Data sources and schema

### Source dataset

The project uses the [Olist Brazilian E-Commerce Public Dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). It contains historical order, item, product, and customer-location records from 2016–2018.

Download and place these four files under `data/raw/` if you want to run preprocessing:

```text
data/raw/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_products_dataset.csv
└── olist_customers_dataset.csv
```

The complete dataset includes additional tables, but these are the four used by `preprocess.py`.

### How preprocessing works

- **Orders:** order ID, customer ID, status, purchase timestamp.
- **Order items:** item price and freight value are summed per order; item rows are counted to produce `item_count`.
- **Products:** product category is joined using the first product item found for each order. Therefore, a multi-category order is represented by one category in this prototype.
- **Customers:** customer city and state are joined for geographic analysis.
- Orders without an order ID or usable purchase timestamp are excluded. Missing numeric totals are filled with zero, and missing category values are labelled `Unknown`.

### Order event schema

Each Kafka message is a JSON object containing the following fields:

| Field | Description | Expected type in MongoDB |
|---|---|---|
| `order_id` | Order identifier; used as the upsert key | String |
| `customer_id` | Customer identifier associated with the order | String |
| `order_status` | Recorded order status, such as `delivered` or `canceled` | String |
| `order_purchase_timestamp` | Historical purchase timestamp | Date |
| `total_price` | Sum of item prices for the order; excludes freight | Number |
| `total_freight` | Sum of freight charges for the order | Number |
| `item_count` | Number of item rows associated with the order | Integer |
| `customer_city` | Customer city | String |
| `customer_state` | Customer state abbreviation | String |
| `product_category` | Category mapped from the processed CSV's `product_category_name` column | String |
| `stream_processed_at` | UTC timestamp added by the consumer when it handles the event | Date |

The Olist dataset is Brazilian. Currency-related chart labels should therefore use **Brazilian real (BRL/R$)** unless you explicitly apply and document a currency conversion. `total_price` represents item prices and does not include freight.

### Kafka topic

| Topic | Purpose |
|---|---|
| `ecommerce-orders` | Carries one JSON order-level event per Kafka message from `producer.py` to `consumer.py`. |

The current pipeline defines one topic. It does not define separate topics for payments, shipments, or status-change events.

## Prerequisites

Install the following before starting:

1. **Python 3.10 or newer** (Python 3.11 or 3.12 is a sensible choice for a reproducible academic environment).
2. **Docker Desktop** or Docker Engine with the Docker Compose plugin.
3. A **MongoDB Atlas** cluster, a database user, and network access configured for your machine.
4. The processed CSV already in the repository, or the four original Olist CSV files downloaded from Kaggle.
5. Git, if cloning the repository from GitHub.

You do not need Node.js for the Python pipeline. The `package-lock.json` file in the current repository is not used by the documented Python workflow; it can be removed if no Node.js project is added.

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/341184-cyber/sda-Ecommerce.git
cd sda-Ecommerce
```

### 2. Create and activate a Python virtual environment

**macOS / Linux:**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

**Windows PowerShell:**

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

### 3. Install Python dependencies

The current repository does not include a `requirements.txt`, so install the dependencies explicitly:

```bash
python -m pip install pandas kafka-python pymongo certifi python-dotenv
```

For reproducible submission, add a tested `requirements.txt` with pinned versions after validating the environment used by the team.

### 4. Prepare the processed CSV path

If using the `orders_stream.csv` already committed at the repository root, copy it into the location expected by the producer.

**macOS / Linux:**

```bash
mkdir -p data/processed
cp orders_stream.csv data/processed/orders_stream.csv
```

**Windows PowerShell:**

```powershell
New-Item -ItemType Directory -Force data\processed | Out-Null
Copy-Item orders_stream.csv data\processed\orders_stream.csv
```

Confirm that `data/processed/orders_stream.csv` exists and includes at least these column names:

```text
order_id
customer_id
order_status
order_purchase_timestamp
total_price
total_freight
item_count
customer_city
customer_state
product_category_name
```

If you want to regenerate the file instead, download the four Olist source files and place them under `data/raw/` as listed in [Data sources and schema](#data-sources-and-schema). Then run:

```bash
python preprocess.py
```

The preprocessing script creates `data/processed/` automatically and writes `orders_stream.csv` there.

### 5. Configure MongoDB Atlas

In MongoDB Atlas:

1. Create or select a cluster.
2. Create a database user with read/write access to the target database.
3. Add your computer's IP address to the project's network access allowlist. Avoid allowing access from everywhere unless you understand the security implications.
4. Copy the cluster connection string and replace its username, password, and cluster host with your own values.

Create a file named `.env` in the repository root:

```dotenv
MONGO_URI=mongodb+srv://<username>:<password>@<cluster-host>/?retryWrites=true&w=majority
MONGO_DB=ecommerce_streaming
MONGO_COLLECTION=orders
```

Replace the placeholder values. If your password contains reserved URL characters, URL-encode them in the URI. Never commit `.env` to GitHub.

The application reads these variables:

| Variable | Required? | Default / purpose |
|---|---|---|
| `MONGO_URI` | Yes | MongoDB Atlas connection URI. |
| `MONGO_DB` | No | Defaults to `ecommerce_streaming`. |
| `MONGO_COLLECTION` | No | Defaults to `orders`. |

The expected collection is `ecommerce_streaming.orders` unless you override the optional variables. MongoDB creates the database and collection on the first successful write if they do not already exist.

### 6. Start Kafka

From the repository root, run:

```bash
docker compose up -d
```

Check that the broker is running:

```bash
docker compose ps
docker logs ecommerce-kafka --tail 100
```

The producer and consumer connect to `localhost:9092`, matching the provided Compose configuration.

## Run the pipeline

Run these commands from the repository root. Use **three separate terminal windows** so the consumer can keep listening while the producer sends records.

### Terminal 1 — Start Kafka

If not already running:

```bash
docker compose up -d
```

### Terminal 2 — Start the MongoDB consumer

Activate the virtual environment, then run:

```bash
python consumer.py
```

Expected log text includes a message similar to:

```text
MongoDB Kafka Consumer Started
Kafka topic: ecommerce-orders
Waiting for orders...
```

The consumer continues listening after the producer finishes. Stop it with `Ctrl+C` when you are done.

### Terminal 3 — Start the producer

Activate the same virtual environment and run:

```bash
python producer.py
```

The producer should report the number of orders loaded and print messages as it sends order events. It waits approximately one second between records, so replaying the entire dataset can take a long time. For a short classroom demo, consider adding a documented row limit or faster replay option to the producer rather than waiting for the full dataset.

### Stop Kafka

When the demo is complete:

```bash
docker compose down
```

## Dashboard and business metrics

The repository includes an exported MongoDB Atlas Charts configuration named `Real-Time E-Commerce Intelligence Dashboard (1).charts`. This file is a dashboard configuration export, not a deployed public dashboard and not a MongoDB data backup. You need your own Atlas cluster and a populated collection to view the charts.

Configure or import the dashboard in Atlas Charts so that its data source points to `ecommerce_streaming.orders`. Check the field mappings and aggregations when importing; do not assume all labels or aggregations are correct without inspecting the rendered charts.

Recommended visualizations:

| Visualization | Suggested configuration | Business question |
|---|---|---|
| Monthly revenue trend | Purchase timestamp grouped by month; sum of `total_price` | How does merchandise value vary over time? |
| Monthly order volume | Purchase timestamp grouped by month; distinct count of `order_id` | Are revenue changes associated with changes in order volume? |
| Average order value | Purchase month; mean of `total_price` | Is average merchandise value per order increasing or decreasing? |
| Orders by recorded status | Category `order_status`; distinct count of `order_id` | How are the recorded orders distributed across statuses? |
| Order value by recorded status | Category `order_status`; sum of `total_price` | What merchandise value is associated with each recorded status? |
| Monthly cancellations | Purchase month; distinct count of `order_id`; filter `order_status = canceled` | During which periods are more orders recorded as canceled? |
| Freight by state | Category `customer_state`; sum of `total_freight` | Which states account for the largest aggregate freight amount? |
| Order value versus freight | X=`total_price`; Y=`total_freight`; colour by `customer_state` | How does freight relate to merchandise value across orders and states? |

**Metric definitions matter:** use `SUM(item_count)` to count items, and use a distinct count of `order_id` to count orders. A status-based chart reflects the status recorded in the historical dataset; it does not provide a sequence of status changes. A canceled order's recorded item price is not proof that the amount was paid or lost.

## Verify the pipeline

### Verify the MongoDB connection

With `.env` configured and the virtual environment activated, run:

```bash
python -c "from database import test_connection; print(test_connection())"
```

A successful test should print `MongoDB connection successful!` and the database/collection names, followed by `True`.

### Verify records in MongoDB Atlas

Open **Database → Browse Collections → `ecommerce_streaming` → `orders`** in Atlas. Documents should contain fields such as `order_id`, `order_status`, `total_price`, `order_purchase_timestamp`, and `stream_processed_at`.

You can also use the Atlas query interface to inspect one document. For example:

```javascript
db.orders.findOne()
```

### Verify the data format before streaming

From the repository root, run:

```bash
python -c "import pandas as pd; p='data/processed/orders_stream.csv'; d=pd.read_csv(p); print(d.shape); print(d.columns.tolist()); print(d.head(3).to_string(index=False))"
```

This confirms that the expected CSV path exists and displays its dimensions, columns, and sample rows.

There is no automated test suite or production build configured in the current repository. These are manual smoke checks, not a claim that the project has passed a formal test suite. If adding tests, include repeatable test instructions and run them before submission.

## Troubleshooting

### `FileNotFoundError: data/processed/orders_stream.csv`

The producer looks for the CSV under `data/processed/`. If the file is at the repository root, follow [Setup step 4](#4-prepare-the-processed-csv-path). If rebuilding from raw files, ensure the four input CSVs are under `data/raw/` before running `preprocess.py`.

### `MONGO_URI is not set in the .env file`

Create `.env` in the repository root and set `MONGO_URI`. Make sure the file is named `.env`, not `.env.txt`, and that you launch Python from the project root.

### MongoDB connection timeout or authentication error

Check the cluster URI, username/password, URL encoding of special password characters, the Atlas IP access list, and the database user's permissions. Do not paste credentials into GitHub issues or shared screenshots.

### `NoBrokersAvailable` or a Kafka connection error

Check `docker compose ps` and `docker logs ecommerce-kafka --tail 100`. Wait for Kafka to finish starting, verify port `9092` is available, and confirm both scripts are being run on the host that can reach `localhost:9092`.

### Consumer appears to wait forever

The consumer is designed to keep listening. Start the consumer first, then start the producer in another terminal. If the producer has already completed, the consumer may simply be idle until more messages arrive.

### The document count does not increase after replay

The consumer upserts by `order_id`. Replaying the same order IDs updates existing documents, so the number of documents may remain stable. This is expected for the current idempotent order-level write logic.

### Atlas Charts does not show records or monthly trends

Confirm that the chart data source points to the correct cluster, database, and collection. Make sure `order_purchase_timestamp` is stored as a MongoDB Date rather than a string, and verify the chart's grouping field and aggregation. Refresh the chart after data ingestion. Back up the collection before running `fix_dates.py`; inspect that script before using it.

## Limitations and interpretation notes

- **Historical replay, not a live feed:** records originate from a public 2016–2018 dataset and are replayed to simulate streaming.
- **One event per order:** the current design does not emit a separate event for each order-status transition.
- **Category simplification:** when an order has multiple product categories, preprocessing assigns the category from the first selected order item.
- **Upsert behaviour:** replayed order IDs update existing records rather than creating a new document for every pass.
- **Revenue definition:** `total_price` is the sum of item prices and excludes freight. It should not automatically be described as net revenue, profit, or total customer spend.
- **Cancellation interpretation:** recorded cancellation status is not proof of realized financial loss.
- **Aggregate freight comparisons:** states with more orders may naturally have larger total freight. Consider freight per order or freight as a percentage of merchandise value before inferring efficiency differences.
- **Prototype readiness:** the current repository does not include a pinned Python dependency lock, automated tests, CI workflow, or a hosted Atlas Charts deployment. Those should be added and verified before presenting the project as production-ready.

## Security

- Keep `.env` out of version control. Do not commit database URIs, passwords, API keys, certificates, or tokens.
- Add `.env`, `.venv/`, `venv/`, `__pycache__/`, `.DS_Store`, and generated local data to `.gitignore` as appropriate.
- Use a dedicated MongoDB database user with only the permissions needed for the demo.
- Review screenshots and logs before publishing them to ensure they do not expose connection strings or other credentials.

## Acknowledgements and license

- **Dataset:** [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Review the dataset page and its terms before redistributing source data.
- **Course project:** End-to-End Streaming Analytics Solution.
- **Team:** Group 12.

No project-specific software license is included in the current repository. Unless the team adds a license file, reuse and redistribution of the project's code should not be assumed to be authorized under an open-source license. The dataset may have separate terms from the project code.
