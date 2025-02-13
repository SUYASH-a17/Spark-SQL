# Spark-SQL
Learning Spark SQL

## Spark_df_meth_1 file
In Spark DF method1 explains the dataframe methods in SparkSQL
Loading and working on csv & json files and 

## SparkSQL2 file
Here we continue with our SparkSQL methods on dataframes

## SparkSQL3 file
More Sparksql on Dataframes and tables using df methods

## Spark dup file 
dropping dup and null values using spark sql and df methods like fillna and dropna

## Spark EDA file
Exploratory Data Analysis with SparkSQL

## Spark Timeseries & Window function
Using SparkSQL window function implemented timeseries analysis 

## The OVER Clause in SQL

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
