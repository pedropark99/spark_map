# `ends_with()`

## Description

Map all columns of your Spark DataFrame whose name ends with a specific text. This function is one of several existing mapping functions (read the article [**"Building the mapping"**](https://github.com/pedropark99/spark_map/blob/main/doc/english/articles/building-mapping.md)).

## Arguments

- `text`: a *string* containing the text you want to search for;

## Details and examples

Therefore, `ends_with()` is used to define which columns `spark_map()` will apply the given function to. This function performs the inverse process of `starts_with()`, i.e. it searches for all columns whose name ends with a specific text. So, with the expression `ends_with("Score")`, `ends_with()` will map all columns whose name ends with the text `"Score"`.

During the mapping process, an exact match between the searched *strings* is always used. As a result, an expression like `ends_with("Sales")` is not able to map columns like `"brazil_sales"`, `"colombia_sales"` and `"eua_sales"`, however it is able to map columns like `"france_Sales "` and `"russia_Sales"`. If you need to be more flexible in your mapping, you'll likely want to use the `matches()` function instead of `ends_with()`.

```python
data = [
  (2022, 1, 12300, 41000, 36981),
  (2022, 2, 19120, 21300, 31000),
  (2022, 3, 18380, 11500, 31281)
]

sales = spark.createDataFrame(data, ['year', 'month', 'france_Sales', 'brazil_sales', 'russia_Sales'])

spark_map(sales, ends_with('Sales'), F.mean).show()
```

```
Selected columns by `spark_map()`: france_Sales, russia_Sales

+------------+------------------+
|france_Sales|      russia_Sales|
+------------+------------------+
|     16600.0|33087.333333333336|
+------------+------------------+
```