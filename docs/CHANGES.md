## Changes that have to be done to the queries in order to run them in Spark SQL

**NOTE**: these changes assume that the queries were generated using the `netezza` dialect. If you are using a different dialect, you may need to make additional changes.

| Original | Replacement |
|----------|-------------|
|`cast('<date_str>' as date) + <n> days`|`date_add(cast('<date_str>' as date), <n>)`|
|`cast('<date_str>' as date) - <n> days`|`date_sub(cast('<date_str>' as date), <n>)`|
|Originally, Spark SQL does not support double quoted alias as identifiers. For example: `SELECT c_customer_id AS "Customer ID" FROM customer` will throw an error. | To fix this, you can either use backticks instead of double quotes, or enable ANSI mode in Spark SQL by setting the configuration `spark.sql.ansi.enable` to `true` (see [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)).|
|Queries 14, 23, 24 and 39 have two statements each.|We need to run each query separately, as Spark SQL does not support multiple statements in a single query.|
|Query 30 uses the column `c_last_review_date_sk`, which does not exist in any part of the schema.|Reeplace every occurrence of `c_last_review_date_sk` with `c_last_review_date`.|

(cast('1998-08-04' as date) +  30 days)
(date_add(cast('1998-08-04' as date), 30))