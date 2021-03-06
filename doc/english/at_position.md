# `at_position()`

## Description

Map the columns of your Spark DataFrame based on their numeric indexes (1st, 2nd, 3rd column, etc.). This function is one of several existing mapping functions (read the article [**"Building the mapping"**](https://github.com/pedropark99/spark_map/blob/main/doc/english/articles/building-mapping.md)).

## Arguments

- `*indexes`: the column indexes (separated by commas);
- `zero_index`: boolean value (`True` or `False`) indicating whether the indexes provided in `*indexes` are zero-based or not (read the **"Details"** section below). By default, this argument is set to `False`;

## Details and examples

Therefore, `at_position()` is used to define which columns `spark_map()` will apply the given function to. To use this function, you supply the numeric indexes, separated by commas, that represent the columns you want to map in `spark_map()`.

The `zero_index` argument is optional, and determines whether the given column indexes will be based on a zero-start index system, or on a one-start index system. Python uses a zero-start index system, so the value 0 represents the first value of an object, while 1 represents the second value of an object, and so on.

But, the `zero_index` argument is set by default to `False`. Because of this, the `at_position()` function always initially works with an index system starting at one. So, in the expression `at_position(3, 4, 5)`, the `at_position()` function will map the 3rd, 4th and 5th columns of your Spark DataFrame. However, if you want to override this behavior, and use Python's default index system (starting at zero), just set this argument to `True`. In the example below, `at_position()` will map to the 2nd, 3rd and 4th column of the `sales` DataFrame.

```python
data = [
  (2022, 1, 12300, 41000, 36981),
  (2022, 2, 19120, 21300, 31000),
  (2022, 3, 18380, 11500, 31281)
]

sales = spark.createDataFrame(data, ['year', 'month', 'france_Sales', 'brazil_sales', 'russia_Sales'])

spark_map(sales, at_position(1, 2, 3, zero_index = True), F.mean).show()
```

```
Selected columns by `spark_map()`: month, france_Sales, brazil_sales

+-----+------------+------------+
|month|france_Sales|brazil_sales|
+-----+------------+------------+
|  2.0|     16600.0|     24600.0|
+-----+------------+------------+
```

When providing a zero index, you should always set the `zero_index` argument to `True`. When the `zero_index` argument is set to `False`, `at_position()` will automatically subtract 1 from all indexes. So an index equal to zero becomes an index equal to -1, and negative indexes are not allowed by `at_position()`. See the example below:

```python
at_position(0, 2, 4)
```

```python
ValueError: One (or more) of the provided indexes are negative! Did you provided a zero index, and not set the `zero_index` argument to True?
```

Furthermore, any duplicate index is automatically eliminated by `at_position()`. See the example below in which the indexes 1 and 4 are repeated during the call to `at_position()`, but they are automatically eliminated in the function result.

```python
at_position(1, 1, 2, 3, 4, 4, 5)
```

```python
{'fun': '__at_position', 'val': (0, 1, 2, 3, 4)}
```

Furthermore, the indexes given to `at_position()` must not be inside a list, if you make this mistake, the function will raise a `ValueError`, as shown below:

```python
at_position([4, 5, 6])
```
```python
ValueError: Did you provided your column indexes inside a list? You should not encapsulate these indexes inside a list. For example, if you want to select 1?? and 3?? columns, just do `at_position(1, 3)` instead of `at_position([1, 3])`.
```

Column indexes are a required argument. So, if you don't provide any index, `at_position()` will necessarily raise a `ValueError`, as shown below:


```python
at_position(zero_index = True)
```
```python
ValueError: You did not provided any index for `at_position()` to search
```