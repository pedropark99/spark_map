# `are_of_type()`

## Description

Map all columns of your Spark DataFrame that fit a certain data type (string, double, integer, etc.). This function is one of several existing mapping functions (read the article [**"Building the mapping"**](https://github.com/pedropark99/spark_map/blob/main/doc/english/articles/building-mapping.md)).

## Arguments

- `arg_type`: a *string* containing the name of the data type you want to search for (see the available values ​​in the **"Details and Examples"** section below);

## Details and examples

Therefore, `are_of_type()` is used to define which columns `spark_map()` will apply the given function to. To use this function, you must provide one of the following values:

- `"string"`: for columns of type `pyspark.sql.types.StringType()`;
- `"int"`: for columns of type `pyspark.sql.types.IntegerType()`;
- `"double"`: for columns of type `pyspark.sql.types.DoubleType()`;
- `"date"`: for columns of type `pyspark.sql.types.DateType()`;
- `"datetime"`: for columns of type `pyspark.sql.types.TimestampType()`;

This means that `are_of_type()` accepts only one of the above values. If you provide a *string* that is not included in the above list, a `ValueError` is automatically raised by the function, as shown below:

```python
are_of_type("str")
```

```python
ValueError: You must choose one of the following values: 'string', 'int', 'double', 'date', 'datetime'
```

In essence, `are_of_type()` uses your Spark DataFrame schema to determine which columns belong to the data type you have determined. Notice in the example below, that the column named `"date"` is mapped by `spark_map()`, even though this column is clearly a date column. This happens, because Spark is interpreting this column by the type `pyspark.sql.types.StringType()`, not by `pyspark.sql.types.DateType()`.

```python
data = [
  ("2022-03-01", "Luke", 36981),
  ("2022-02-15", "Anne", 31000),
  ("2022-03-12", "Bishop", 31281)
]

sales = spark.createDataFrame(data, ['date', 'name', 'value'])

spark_map(sales, are_of_type("string"), F.max).show()
```

```
Selected columns by `spark_map()`: date, name

+----------+----+
|      date|name|
+----------+----+
|2022-03-12|Luke|
+----------+----+
```