# `spark_map()`
## Overview
This repository store a Pyspark implementation of `map()` function for spark DataFrames. With `spark_map()` you are able to apply a function over multiple columns of a Spark DataFrame. In other words, with `spark_map()`, you can avoid building long `.agg()` expressions that basically applies the same Pyspark function over multiple columns.

## Documentation
Documentation of `spark_map()` and its partners, are available as Markdown files (`.md`) in [english](https://github.com/pedropark99/spark_map/tree/main/doc/english) and in [portuguese](https://github.com/pedropark99/spark_map/tree/main/doc/portuguese).

## A simple example
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

Suppose you want to calculate the average of the third, fourth and fifth columns of this DataFrame students. The `spark_map()` function allows you to perform this calculation in an extremely simple and clear way, as shown below:

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



