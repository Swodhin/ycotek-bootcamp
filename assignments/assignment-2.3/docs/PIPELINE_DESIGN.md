\# Assignment 2.3 – Pipeline Design Document



\## 1. Introduction



This document describes the design and workflow of the retail data engineering pipeline developed using Apache Airflow, Docker, and Snowflake.



The pipeline processes five consecutive days of retail source data and organizes it into Bronze, Silver, and Gold layers.



\## 2. Source Data



The pipeline processes the following datasets:



\- Item Master

\- POS Sales

\- Inventory Snapshots

\- Store Master

\- Store-Region Mapping



The input files cover the business dates October 1–5, 2026. Store Master also includes an earlier snapshot dated September 24, 2026.



\## 3. Pipeline Architecture



\### Bronze Layer – Raw Ingestion



The Bronze layer stores the raw source data in Snowflake.



\- Files are detected using Airflow FileSensor tasks.

\- Files are uploaded to a Snowflake stage.

\- COPY INTO loads the files into Bronze raw tables.

\- Source file names and row numbers are retained for traceability.

\- Existing rows for a source file are removed before that file is reloaded.



\### Silver Layer – Data Cleaning and Validation



The Silver layer transforms raw data into standardized datasets.



\- Validates required fields and business rules.

\- Identifies missing item IDs.

\- Identifies negative sales quantities.

\- Identifies sales referencing missing items.

\- Identifies duplicate transaction lines.

\- Stores invalid records in a quarantine table with their source information and rejection reason.

\- Makes valid records available for Gold processing.



\### Gold Layer – Dimensions, Facts, and Reports



The Gold layer contains dimensional and analytical tables.



\*\*Dimensions\*\*

\- DIM\_DATE

\- DIM\_ITEM

\- DIM\_LOCN



\*\*Fact tables\*\*

\- FCT\_SALES

\- FCT\_INVENTORY



\*\*Reports\*\*

\- Daily sales by item

\- Daily sales by department

\- Daily sales by store

\- Daily sales by district

\- Daily sales by region

\- Daily inventory



\## 4. Airflow Workflow



The DAG is named `assignment\_2\_3\_pipeline`.



Its main task sequence is:



1\. Load file specifications from configuration.

2\. Set up Bronze tables and Snowflake objects.

3\. Set up Silver tables.

4\. Wait for source files using FileSensor tasks.

5\. Dynamically map ingestion tasks across source files.

6\. Transform and validate Bronze data into Silver.

7\. Generate a quarantine summary.

8\. Set up Gold tables.

9\. Transform Silver data into Gold dimensions and facts.

10\. Reconcile Silver and Gold sales totals.



The workflow uses task dependencies to ensure that each layer is processed in the correct order.



\## 5. Historical Data Management



\### SCD Type 2



SCD Type 2 preserves historical versions instead of overwriting changed records.



\- DIM\_ITEM contains SCD2 comparison logic for item attributes.

\- DIM\_LOCN preserves historical store versions.

\- Store 1101 demonstrates a district change from 101 to 102 on October 1, 2026.



\### SCD Type 1



SCD Type 1 updates an attribute in place without preserving its previous value.



\- STORE\_NAME10 in DIM\_LOCN uses SCD1 update logic.



\### Inventory History



Inventory snapshots are retained for each business date.



The fact table uses EFFECTIVE\_FROM, EFFECTIVE\_TO, and IS\_CURRENT to identify historical and current snapshot records.



\## 6. Error Handling and Quarantine



Invalid records are not silently discarded.



The quarantine table records:

\- Dataset name

\- Source file

\- Source row number

\- Rejection reason

\- Record data

\- Quarantine timestamp



The Day 3 POS file contains intentional data-quality defects, which are detected and reported by the pipeline.



\## 7. Reconciliation



The pipeline compares daily net sales totals between Silver POS sales and the Gold sales fact table.



The reconciliation was verified for all five dates, October 1–5, 2026.



The difference was zero for every date.



\## 8. Idempotency



The pipeline was rerun using the same source data.



Before-and-after table counts were captured to verify that repeated execution does not create duplicate records or change the expected final counts.



\## 9. Automated Tests



Three automated tests were executed using Pytest:



1\. DAG parsing test.

2\. DAG dependency cycle test.

3\. Task retry configuration test.



All three tests passed.



\## 10. Conclusion



The pipeline demonstrates a complete retail data engineering workflow using Airflow and Snowflake.



It supports raw ingestion, data cleaning, quarantine handling, dimensional modeling, historical inventory tracking, reporting, reconciliation, and repeatable execution.



