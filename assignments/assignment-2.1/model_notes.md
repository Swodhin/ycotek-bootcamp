# Retail Data Warehouse — Model Notes

## 1. Purpose and design

This model is for Assignment 2.1. It represents a Walmart-like retail business. Since the assignment does not provide a source database, I have used realistic sample-data assumptions to design the warehouse.

The model uses a **snowflake schema**. The product hierarchy is split between `DIM_SUBCLASS` and `DIM_ITEM`, and the location hierarchy is split between `DIM_DISTRICT` and `DIM_LOCN`.

## 2. Tables in the model

### Dimension tables

- `DIM_SUBCLASS` stores subclass, category and department information. It is the parent of `DIM_ITEM`.
- `DIM_ITEM` stores product details and keeps historical versions using SCD Type 2.
- `DIM_DISTRICT` stores district and geographic information. It is the parent of `DIM_LOCN`.
- `DIM_LOCN` stores store/location details and keeps historical versions using SCD Type 2.
- `DIM_DAY` stores calendar dates and related date details. It uses SCD Type 1.
- `DIM_CUSTOMER` stores customer details. It uses SCD Type 1.

### Fact tables

- `FCT_SALES` stores individual sales transaction lines.
- `FCT_INVENTORY` stores inventory quantities for an item and location over an effective period.
- `FCT_PRICE` stores the price of an item at a location over an effective period.

### Aggregate table

- `AGG_DAILY_ITEM_SALES` stores daily sales totals for each item and location.

## 3. Fact table grains

- **FCT_SALES:** One row represents one item on one sales transaction line, for one customer, at one location, on one day.
- **FCT_INVENTORY:** One row represents the inventory position for one item at one location over one effective period.
- **FCT_PRICE:** One row represents the price of one item at one location over one effective period.

## 4. Dimension attributes and SCD types

The tables below classify every dimension column. Descriptive attributes are marked SCD1 or SCD2. Surrogate keys and business identifiers are marked as keys because they identify records rather than describe changing business details. Effective dates and current flags are marked as SCD2 control columns because they manage the versions; they are not descriptive attributes themselves.

### DIM_SUBCLASS

| Attribute | Classification |
|---|---|
| `subclass_sk` | Surrogate key — N/A |
| `subclass_id` | Business key — N/A |
| `subclass_name` | SCD1 |
| `category_name` | SCD1 |
| `department_name` | SCD1 |

**Reason:** If a subclass, category or department label is corrected, the latest value is enough for this model. Keeping every old label is not needed for the reporting requirements assumed here.

### DIM_ITEM

| Attribute | Classification |
|---|---|
| `item_sk` | Surrogate key — N/A |
| `item_id` | Business key — N/A |
| `item_name` | SCD2 |
| `brand` | SCD2 |
| `subclass_sk` | SCD2 — tracks changes to the item's subclass assignment |
| `effective_from` | SCD2 control column |
| `effective_to` | SCD2 control column |
| `is_current` | SCD2 control column |

**Reason:** Product names, brands and subclass assignments can change. Keeping the earlier versions makes it possible to report sales using the product details that applied when the sale happened.

### DIM_DISTRICT

| Attribute | Classification |
|---|---|
| `district_sk` | Surrogate key — N/A |
| `district_id` | Business key — N/A |
| `district_name` | SCD1 |
| `city` | SCD1 |
| `state` | SCD1 |
| `country` | SCD1 |

**Reason:** This model uses the latest district and geographic descriptions. It does not need to keep a history of spelling corrections or label changes for these fields.

### DIM_LOCN

| Attribute | Classification |
|---|---|
| `locn_sk` | Surrogate key — N/A |
| `locn_id` | Business key — N/A |
| `locn_name` | SCD2 |
| `locn_type` | SCD2 |
| `district_sk` | SCD2 — tracks changes to the location's district assignment |
| `effective_from` | SCD2 control column |
| `effective_to` | SCD2 control column |
| `is_current` | SCD2 control column |

**Reason:** A store can be renamed, change type or move to another district. Keeping these versions helps reports show the store and district details that were valid at the time of a transaction.

### DIM_DAY

| Attribute | Classification |
|---|---|
| `day_sk` | Surrogate key — N/A |
| `calendar_date` | SCD1 |
| `day_number` | SCD1 |
| `month_number` | SCD1 |
| `month_name` | SCD1 |
| `quarter_number` | SCD1 |
| `year_number` | SCD1 |
| `day_name` | SCD1 |
| `is_weekend` | SCD1 |

**Reason:** Calendar values are based on the date and normally do not change. If a value needs correcting, the corrected value can replace the old one.

### DIM_CUSTOMER

| Attribute | Classification |
|---|---|
| `customer_sk` | Surrogate key — N/A |
| `customer_id` | Business key — N/A |
| `customer_name` | SCD1 |
| `gender` | SCD1 |
| `city` | SCD1 |
| `customer_segment` | SCD1 |

**Reason:** The model only needs the customer's latest details for its reporting. It does not keep previous versions of customer profiles.

## 5. How SCD Type 2 is handled

`DIM_ITEM` and `DIM_LOCN` use SCD Type 2. When a tracked attribute changes, the old row is given an end date and a new row is inserted with a new surrogate key.

- `effective_from` is the first date the version applies.
- `effective_to` is the last date the version applies, inclusive.
- `is_current` identifies the current version.
- The current version uses `9999-12-31` as its open-ended `effective_to` date.
- A partial unique index makes sure there is no more than one current row for each business key.

When loading a fact, the ETL process should use the dimension version that was valid on the fact's date.

## 6. Keys and relationships

- Each dimension has a surrogate primary key.
- Fact tables use dimension surrogate keys as foreign keys.
- `DIM_SUBCLASS` has a one-to-many relationship with `DIM_ITEM`.
- `DIM_DISTRICT` has a one-to-many relationship with `DIM_LOCN`.
- `FCT_SALES` references `DIM_ITEM`, `DIM_LOCN`, `DIM_DAY` and `DIM_CUSTOMER`.
- `FCT_INVENTORY` references `DIM_ITEM` and `DIM_LOCN`.
- `FCT_PRICE` references `DIM_ITEM` and `DIM_LOCN`.
- `AGG_DAILY_ITEM_SALES` references `DIM_DAY`, `DIM_ITEM` and `DIM_LOCN`.
- Customer is linked only to sales because inventory and price records are not specific to a customer.

## 7. Aggregate table and why this grain was chosen

`AGG_DAILY_ITEM_SALES` has one row per item, per location, per day. It stores `total_quantity_sold`, `total_sales_amount` and `transaction_count`.

I chose this grain because it makes it easier to compare daily sales for each item across different locations. Reports can use the summary instead of calculating totals from all the individual sales lines each time. `transaction_count` is the number of distinct transactions, not the number of transaction lines.

## 8. Assumptions about the source data

1. The assignment does not name an actual source system, so the model is based on synthetic Walmart-like retail data.
2. The source has stable business identifiers for items, locations, customers, districts and subclasses.
3. Each sales record represents one transaction line and includes a transaction ID and line ID.
4. Sales data includes the item, location, date, customer, quantity, unit price, discount and final sales amount.
5. Inventory data is recorded by item and location, with effective start and end dates.
6. Price data is recorded by item and location, with effective start and end dates.
7. Effective end dates are inclusive. `9999-12-31` means that the period is still open.
8. An item belongs to one subclass during each SCD2 version, and a location belongs to one district during each SCD2 version.
9. Customer details use SCD1 and are only linked to sales.
10. The calendar table is populated from 2020-01-01 to 2035-12-31. The range can be extended if the source data needs more dates.
11. `sales_amount` is treated as the final line amount after discounts. The ETL process should check or calculate it consistently using the source values.
12. The aggregate table is built from `FCT_SALES`. Its transaction count is calculated using distinct transaction IDs for each item, location and day.

## 9. Implementation notes

- PostgreSQL identity columns generate the surrogate keys.
- Fact tables have their own surrogate primary keys and unique constraints based on their business grain.
- Partial unique indexes enforce one current SCD2 row per item or location business key.
- The SQL file creates the schema, tables, constraints and calendar seed. Loading source records, applying SCD2 changes and refreshing the aggregate table would be handled by ETL processes.
