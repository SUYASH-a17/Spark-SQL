# Spark-SQL
Learning Spark SQL
### Importing Spark & Starting a sparksession
```
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkSQL1').getOrCreate()
```
### Data Loading
```
file_path = 'abcd.csv'
df1 = spark.read.format("csv").option("header","true").load(file_path)
df1
```
## Sparksql1 file
In Spark DF method1 explains the dataframe methods in SparkSQL
Loading and working on csv & json files and many other sql operations
```
df1.show()   # To show the DF in a nice format 
df1.conut()  # Count of number of rows in DF
df1.columns  # Count of number of columns in DF

df3_sample = df3.sample(withReplacement = False, fraction = 0.1)  # sample method randomly selects subset of data from the DF fraction=0.1 means 10% from the total data, withReplacement parameter and the false value means each row can be selected only once.
df3_sample.show()

df3_sort = df3_sample.sort("event_datetime")  # sorts the columns in ascending order
df3_sort.show()
```
SQL keywords
```
df1.filter(df1["location_id"]=='loc0').count()                  # filter by matching the column and the values required and return the rows
df1.groupBy("location_id").count().show()                       # group the DF by "location_id"
df1.orderBy("location_id").show()                               # order the DF by the values in "location_id"
df1.groupBy("location_id").agg({"temp_celcius":"mean"}).show()  # group the DF by "location_id" and calc the mean value of "temp_celcius"
df1.groupBy("location_id").agg({"temp_celcius":"max"}).show()   # group the DF by "location_id" and calc the max value of "temp_celcius"
df1.groupBy("location_id").agg({"temp_celcius":"mean"}).orderBy("location_id").show()  # group the DF by "location_id" and calc the mean value of "temp_celcius"  and order by "location_id"
```

## SparkSQL2 file
Here we continue with our SparkSQL methods on dataframes

```
s_df.createOrReplaceTempView("utilization")            #The createOrReplaceTempView method is used to create a temporary view of a DataFrame that you can query using SQL
s_sql = spark.sql("SELECT * FROM utilization LIMIT 10") s_sql.count()  #s_sql will contain the first 10 rows from the utilization table
s_sql = spark.sql("SELECT event_datetime AS e_dt FROM utilization LIMIT 10") s_sql.show()  # s_sql will contain the e_dt as the first 10 rows from the utilization table
```

## SparkSQL3 file
More Sparksql on Dataframes and tables using df methods

## Spark dup file 
dropping dup and null values using spark sql and df methods like fillna and dropna

## Spark EDA file
Exploratory Data Analysis with SparkSQL

## Spark Timeseries & Window function
Using SparkSQL window function implemented timeseries analysis 

### The OVER Clause in SQL

The `OVER` clause is a powerful SQL feature used with window functions to perform calculations across a set of rows related to the current row.

### Functionality
It enables aggregations, distributions, rankings, and offset comparisons within specified partitions of data.

### Syntax
The basic structure is:

```sql
function_name() OVER (
    [PARTITION BY column(s)]
    [ORDER BY column(s)]
    [ROWS or RANGE clause]
)
```

### PARTITION BY Clause
The `PARTITION BY` clause in SQL divides the result set into partitions based on the specified `partition_column(s)`. It allows you to perform calculations or apply window functions separately for each partition while retaining all rows in the result set.

#### Key Points about `PARTITION BY`:
- It creates subsets of data based on the values in the `partition_column(s)`.
- Window functions or aggregations are applied independently to each partition.
- Unlike `GROUP BY`, it preserves all rows in the output, adding calculated values as new columns.
- It's commonly used with window functions like `AVG()`, `SUM()`, `RANK()`, and `ROW_NUMBER()`.

#### Example
```sql
SELECT employee_id, department_id, salary,
       AVG(salary) OVER (PARTITION BY department_id) AS avg_department_salary
FROM employees;
```

This query calculates the average salary for each department while still showing individual employee data. The result is partitioned by `department_id`, and the `AVG()` function is applied separately to each partition.


## Spark ML file
Used vectorassembler and implemented Kmeans model

## Spark ML2 file
Used vectorassembler and implemented Linear Regression model
