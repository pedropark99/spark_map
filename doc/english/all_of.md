# `all_of()`

## Description

Map all columns of your Spark DataFrame whose name is included in a string list. This function is one of several existing mapping functions (read the article [**"Building the mapping"**](https://github.com/pedropark99/spark_map/blob/main/doc/english/articles/building-mapping.md)).

## Arguments

- `list_cols`: a list of *strings* containing the names of the columns you want to map;

## Details and examples

Therefore, `all_of()` is used to define which columns `spark_map()` will apply the given function to. You can use this function, when you want to allow a set of columns to be mapped, but for some reason you don't know in advance if all these columns (or a part of them) will be available in your Spark DataFrame.

You must give `all_of()` a list of *strings*. Each *string* represents the name of a column that can be mapped. As an example, the expression `all_of(['sales_france', 'sales_brazil', 'sales_colombia'])` allows columns named `"sales_france"`, `"sales_brazil"` and `"sales_colombia"` to be mapped by `spark_map()`. However, `spark_map()` doesn't necessarily need to find all these columns at once. That is, `all_of()` makes these columns "optional", so `spark_map()` can find all three columns, or only two, or even just one of these columns. See the example below:

```python
data = [
  (2022, 1, 12300, 41000, 36981),
  (2022, 2, 19120, 21300, 31000),
  (2022, 3, 18380, 11500, 31281)
]

sales = spark.createDataFrame(data, ['year', 'month', 'sales_france', 'sales_brazil', 'sales_russia'])

spark_map(
    sales,
    all_of(['sales_france', 'sales_brazil', 'sales_colombia']),
    F.mean
  )\
  .show()
```

```
Selected columns by `spark_map()`: sales_france, sales_brazil

+------------+------------+
|sales_france|sales_brazil|
+------------+------------+
|     16600.0|     24600.0|
+------------+------------+
```

However, it is worth noting that `spark_map()` **must find at least one of the columns defined** in `all_of()`. If it doesn't, `spark_map()` will raise a `KeyError` warning that no column could be found with the mapping you defined.

```python
spark_map(sales, all_of(['sales_italy']), F.mean).show()
```

```python
KeyError: '`spark_map()` did not find any column that matches your mapping!'
```