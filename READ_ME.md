# PySpark BIA — BigMart Sales Data Engineering

A hands-on PySpark notebook covering data ingestion, schema management, transformations, aggregations, joins, window functions, UDFs, and multi-format data writing — built on the BigMart Sales dataset in Databricks.

## Dataset
`BigMart Sales.csv` — retail sales data with item-level and outlet-level attributes (Item_Identifier, Item_Weight, Item_Fat_Content, Item_Visibility, Item_Type, Item_MRP, Outlet_Identifier, Outlet_Establishment_Year, Outlet_Size, Outlet_Location_Type, Outlet_Type, Item_Outlet_Sales).

---

## 1. Data Reading
- Loaded CSV into a Spark DataFrame using `spark.read.format("csv")` with `header` and `inferSchema` options.

## 2. Schema Definition
Three approaches to enforce schema instead of relying on inference:
- **DDL-formatted schema string** — schema defined as a SQL-like string and passed via `.schema()`.
- **StructType() schema** — schema defined programmatically using `StructType` and `StructField` objects for explicit type control.
- **printSchema()** — used throughout to inspect column names and data types after each schema approach.

## 3. Core Transformations

| Transformation | Purpose |
|---|---|
| `select()` | Projects specific columns from the DataFrame |
| `alias()` | Renames a column *within* a select/expression (non-persistent) |
| `withColumnRenamed()` | Renames a column across the whole DataFrame, keeping all others intact |
| `withColumn()` | Adds a new column or modifies an existing one in the original DataFrame |
| `filter()` | Row-level filtering using single and multiple conditions (`&`, `isin()`, `isNull()`) |
| `regexp_replace()` | Replaces/cleans values within a column (e.g., standardizing category labels) |
| Type casting | Converts column data types (e.g., string to double/int) using `.cast()` |
| `orderBy()` / `sort()` | Single and multi-column sorting, ascending and descending |
| `limit()` | Restricts the number of rows returned |
| `drop()` | Removes one or more columns |
| `dropDuplicates()` | Removes duplicate rows, including column-specific duplicate checks |
| `dropna()` / `fillna()` | Handles missing/null values — dropping specific-column nulls and filling defaults to avoid downstream risk |
| `union()` | Combines two DataFrames row-wise (including a demonstration of `unionByName()` behavior) |

## 4. String & Date Functions
- **String functions** — column-level string manipulation (e.g., case conversion, trimming).
- **Date functions** — extracting and formatting date components.
- **datediff()** — calculates the difference between two dates.

## 5. Advanced Transformations

| Function | Purpose |
|---|---|
| `split()` | Splits a string column into an array, with indexing to extract specific elements |
| `groupBy()` | Aggregates data by one or more columns |
| `collect_list()` | Aggregates values into a list per group |
| `pivot()` | Reshapes data by turning row values into columns |
| `when().otherwise()` | Conditional (if-else style) column logic |

## 6. Joins
- Inner join between two DataFrames on a common key (`dept_id`), demonstrating standard join syntax and behavior differences between join key naming conventions.

## 7. Window Functions
- **`rank()`** — assigns ranks within partitions ordered by a column.
- **Running/cumulative sum** — calculated using `sum().over(Window.orderBy(...).rowsBetween(...))` with unbounded preceding/following frame boundaries.

## 8. User-Defined Functions (UDFs)
- Custom Python function (`myfun`) registered as a Spark UDF using `udf()` and applied to a column via `withColumn()`.

## 9. Data Writing
Demonstrates all standard Spark write modes and formats:

| Mode/Format | Behavior |
|---|---|
| `mode('append')` | Adds new data to existing output without deleting old data |
| `mode('overwrite')` | Replaces existing output entirely |
| `mode('error')` | Default — fails if the target already exists |
| `mode('ignore')` | Skips the write silently if the target already exists |
| `format('csv')` | Writes output as CSV |
| `format('parquet')` | Writes output as columnar Parquet format |
| `format('delta')` + `saveAsTable()` | Writes as a **managed Delta table** registered in the catalog (required for `saveAsTable` in Databricks/Unity Catalog) |

## 10. Spark SQL
- **`createTempView()`** — registers the DataFrame as a temporary SQL view.
- **`%sql` magic cell** — queries the view directly using SQL syntax inside the notebook.
- **`spark.sql()`** — runs SQL queries from Python and captures results back into a DataFrame for further processing.

---

## Key Takeaways
This notebook is a structured walkthrough of the PySpark DataFrame API — from ingestion and schema enforcement, through single/multi-column transformations, null handling, joins, and window functions, to writing data in multiple formats and modes. It reflects practical, interview-relevant PySpark patterns commonly used in real-world ETL pipelines.

## Tech Stack
`PySpark` · `Databricks` · `Delta Lake` · `Spark SQL`
