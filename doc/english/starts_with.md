# `starts_with()`

## Description

Map all columns of your Spark DataFrame whose name starts with a specific text. This function is one of several existing mapping functions (read the article [**"Building the mapping"**](https://github.com/pedropark99/spark_map/blob/main/doc/english/articles/building-mapping.md)).

## Arguments

- `text`: a *string* containing the text you want to search for;

## Details and examples

Therefore, `starts_with()` is used to define which columns `spark_map()` will apply the given function to. This function performs the inverse process of `ends_with()`, that is, it searches for all columns whose name starts with a specific text. Therefore, with the expression `starts_with("Score")`, `starts_with()` will map all columns whose name starts with the text `"Score"`.

During the mapping process, an exact *match* between the searched *strings* is always used. As a result, an expression like `starts_with("Sales")` is not able to map columns like `"sales_brazil"`, `"sales_colombia"` and `"sales_eua"`, however it is able to map columns like `"Sales_france "` and `"Sales_russia"`. If you need to be more flexible in your mapping, you'll likely want to use the `matches()` function instead of `starts_with()`.

```python
data = [
  (2022, 1, 12300, 41000, 36981),
  (2022, 2, 19120, 21300, 31000),
  (2022, 3, 18380, 11500, 31281)
]

sales = spark.createDataFrame(data, ['year', 'month', 'Sales_france', 'sales_brazil', 'Sales_russia'])

spark_map(sales, starts_with('Sales'), F.mean).show()
```

```
Selected columns by `spark_map()`: Sales_france, Sales_russia

+------------+------------------+
|Sales_france|      Sales_russia|
+------------+------------------+
|     16600.0|33087.333333333336|
+------------+------------------+
```