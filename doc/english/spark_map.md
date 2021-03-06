# `spark_map()`
## Description

With `spark_map()` you are able to apply a function over multiple columns of a Spark DataFrame. In short, `spark_map()` takes a Spark DataFrame as *input* and returns a new Spark DataFrame (aggregated by the function you provided) as *output*.

## Arguments

- `table`: a Spark DataFrame or a grouped DataFrame (i.e. `pyspark.sql.DataFrame` or `pyspark.sql.GroupedData`);
- `mapping`: the mapping that defines the columns where you want to apply `function` (read the article [**"Building the mapping"**](https://github.com/pedropark99/spark_map/blob/main/doc/english/articles/building-mapping.md));
- `function`: the function you want to apply to each column defined in `mapping`;


## Details and examples

As an example, consider the `students` DataFrame below:

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
  'StudentID', 'Name', 'Age', 'Height', 'Score1',
  'Score2', 'Score3', 'Score4', 'Course', 'Department'
]

students = spark.createDataFrame(d, columns)
students.show(truncate = False)
```

```
+---------+-------+---+------+------+------+------+------+---------+----------+
|StudentID|Name   |Age|Height|Score1|Score2|Score3|Score4|Course   |Department|
+---------+-------+---+------+------+------+------+------+---------+----------+
|12114    |Anne   |21 |1.56  |8     |9     |10    |9     |Economics|SC        |
|13007    |Adrian |23 |1.82  |6     |6     |8     |7     |Economics|SC        |
|10045    |George |29 |1.77  |10    |9     |10    |7     |Law      |SC        |
|12459    |Adeline|26 |1.61  |8     |6     |7     |7     |Law      |SC        |
|10190    |Mayla  |22 |1.67  |7     |7     |7     |9     |Design   |AR        |
|11552    |Daniel |24 |1.75  |9     |9     |10    |9     |Design   |AR        |
+---------+-------+---+------+------+------+------+------+---------+----------+
```

Suppose you want to calculate the average of the third, fourth and fifth columns of this DataFrame `students`. The `spark_map()` function allows you to perform this calculation in an extremely simple and clear way, as shown below:

```python
import pyspark.sql.functions as F
spark_map(students, at_position(3, 4, 5), F.mean).show(truncate = False)
```
```
Selected columns by `spark_map()`: Age, Height, Score1

+------------------+------------------+------+
|Age               |Height            |Score1|
+------------------+------------------+------+
|24.166666666666668|1.6966666666666665|8.0   |
+------------------+------------------+------+
```

If you want your calculation to be applied by group, just provide the grouped table to `spark_map()`. For example, suppose you wanted to calculate the same averages as in the example above, but within each department:

```python
import pyspark.sql.functions as F
by_department = students.groupBy('Department')
spark_map(by_department, at_position(3, 4, 5), F.mean).show()
```

```
Selected columns by `spark_map()`: Age, Height, Score1

+----------+-----+------------------+------+
|Department|  Age|            Height|Score1|
+----------+-----+------------------+------+
|        SC|24.75|1.6900000000000002|   8.0|
|        AR| 23.0|              1.71|   8.0|
+----------+-----+------------------+------+
```


## You define the calculation and `spark_map()` distributes it

All `spark_map()` does is apply any function to a set of columns in your DataFrame. And this function can be any function, as long as it is an aggregator function (that is, a function that can be used inside the `pyspark.sql.DataFrame.agg()` and `pyspark.sql.GroupedData.agg()` methods). As long as your function meets this requirement, you can define whatever calculation formula you want, and use `spark_map()` to spread that calculation over multiple columns.

As an example, suppose you needed to use a little inference to test whether the average of the various Student Scores significantly deviates from 6, through the statistic produced by a *t* test:

```python
def t_test(x, value_test = 6):
  return ( F.mean(x) - F.lit(value_test) ) / ( F.stddev(x) / F.sqrt(F.count(x)) )

results = spark_map(students, starts_with("Score"), t_test)
results.show(truncate = False)
```

```
Selected columns by `spark_map()`: Score1, Score2, Score3, Score4

+-----------------+------------------+-----------------+----------------+
|Score1           |Score2            |Score3           |Score4          |
+-----------------+------------------+-----------------+----------------+
|3.464101615137754|2.7116307227332026|4.338609156373122|4.47213595499958|
+-----------------+------------------+-----------------+----------------+
```