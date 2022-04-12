# Building the mapping

You need to provide a mapping to `spark_map()`. This mapping defines which columns `spark_map()` should apply the function given in the `function` argument. You can build this *mapping* using one of the mapping functions, which are as follows:

- `at_position()`: maps the columns that are in certain positions (1st column, 2nd column, 3rd column, etc.);
- `starts_with()`: maps columns whose name starts with a specific *string*;
- `ends_with()`: maps columns whose name ends with a specific *string*;
- `matches()`: maps columns whose name matches a regular expression;
- `are_of_type()`: maps columns that belong to a specific data type (*string*, *integer*, *double*, etc.);
- `all_of()`: maps all columns that are included within a specific list;

As a first example, you can use the `at_position()` function whenever you want to select columns by position. So if you want to select the first, second, third and fourth column, you give the respective indices of those columns to the `at_position()` function.

On the other hand, you may need to use another method to map the columns you are interested in. For example, the `students` DataFrame has 4 Score columns (`Score1`, `Score2`, `Score3` and `Score4`), and we have two obvious ways to map all these columns. One way is using the `starts_with()` function, and another is using the `matches()` function. Both options below bring the same results.

```python
d = [
  (12114, 'Anne', 21, 1.56, 8, 9, 10, 9, 'Economics', 'SC'),
  (13007, 'Adrian', 23, 1.82, 6, 6, 8, 7, 'Economics', 'SC'),
  (10045, 'George', 29, 1.77, 10, 9, 10, 7, 'Law', 'SC'),
  (12459, 'Adeline', 26, 1.61, 8, 6, 7, 7, 'Law', 'SC'),
  (10190, 'Mayla', 22, 1.67, 7, 7, 7, 9, 'Design', 'AR'),
  (11552, 'Daniel', 24, 1.75, 9, 9, 10, 9, 'Design', 'AR')
]

columns = [
  'StudentID', 'Name', 'Age', 'Heigth', 'Score1',
  'Score2', 'Score3', 'Score4', 'Course', 'Department'
] 

students = spark.createDataFrame(d, columns)

spark_map(students, starts_with("Score"), F.sum).show(truncate = False)
```

```python
spark_map(students, matches("(.+)[0-9]$"), F.sum).show(truncate = False)
```


Basically, the mapping is just a small description containing the algorithm that should be used to find the columns and the value that will be passed on to this algorithm. As an example, the result of the expression `at_position(3, 4, 5)` is a small `dict`, containing two elements (`fun` and `val`). The `fun` element defines the function/algorithm to be used to find the columns, and the `val` element stores the value that will be passed to this function/algorithm.

```python
at_position(3, 4, 5)
```
```python
{'fun': '__at_position', 'val': (3, 4, 5)}
```

The result of the expression `matches('^Score')` is quite similar. However, unlike the previous example that uses an internal function called `__at_position`, this time, the algorithm to be used is what is stored in a function called `__matches`, and `'^Score'` is the value that will be passed to that function.

```python
matches('^Score')
```
```python
{'fun': '__matches', 'val': '^Score'}
```

## Creating your own mapping method

This means that you could **implement your own mapping algorithm**, and, give `spark_map()` a `dict` containing the name of the function that contains that algorithm, and the value that should be passed to that function (the `fun` and `val` elements). Every mapping function should have three arguments: 1) an arbitrary value for the algorithm; 2) the column names of the spark DataFrame as a list of strings (basically, the result of `pyspark.sql.DataFrame.columns`); 3) the schema of the spark DataFrame (this is an object of class `StructType`, or, basically, the result of `pyspark.sql.DataFrame.schema`). 

Your mapping function should always have these three arguments, even if it does not use all of them. For example, the function below maps the column that is in a specific position at the alphabetic order. This `alphabetic_order()` function uses only the `index` and `cols` arguments, even if it receives three arguments. Other requirement is the return value of your mapping function. It should always return a list of strings that contains the names of the columns that were mapped by the function. If your mapping function does not find any column during its search, the function should return an empty list.

```python
def alphabetic_order(index, cols: list, schema: StructType):
    cols.sort()
    return cols[index]
```

To demonstrate this function, lets take the `sales.sales_per_country` spark DataFrame as an example. The list of columns from this DataFrame are exposed below:

```python
sales = spark.table('sales.sales_per_country')
print(sales.columns)
```

```python
['year', 'month', 'country', 'idstore', 'totalsales']
```

Now, using the `alphabetic_order()` inside `spark_map()`:

```python
spark_map(sales, {'fun' = 'alphabetic_order', 'val' = 2}, F.max)
```

```
Selected columns by `spark_map()`: idstore

+--------+
| idstore|
+--------+
|    2300|
+--------+
```



## Take care when using custom mapping functions

However, it is worth noting that if you try to use in your mapping a function that does not exist (ie a function that has not yet been defined in your session), you will get a `KeyError` as a result. Notice in the example below, where I try to use a function called `some_mapping_function()` with the value `'some_value'` to map the columns. Because `spark_map()` doesn't find any function named `some_mapping_function()` defined in my session, a `KeyError` ends up being raised. So, if you face this error when using `spark_map()`, investigate if you have correctly defined the function you want to use in your mapping.

```python
spark_map(students, {'fun': 'some_mapping_function', 'val': 'some_value'}, F.sum)
```

```python
KeyError: 'some_mapping_function'
```

## If your mapping doesn't find any columns

On the other hand, `spark_map()` will also raise a `KeyError`, in case the function you are using in your mapping doesn't find any columns in your DataFrame. However, in this case, `spark_map()` sends a clear message that no columns were found using the mapping you defined. As an example, we could reproduce this error, when trying to map in the DataFrame `students`, all the columns that start with the *string* `'april'`. A `KeyError` is raised in this case because there is no column in the `students` table whose name starts with the word "april".

```python
spark_map(students, starts_with('april'), F.sum)
```
```python
KeyError: `spark_map()` did not find any column that matches your mapping!
```
