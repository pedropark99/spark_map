# `matches()`

## Description

Map all columns of your Spark DataFrame that fit into a regular expression. This function is one of several existing mapping functions (read the article [**"Building the mapping"**](https://github.com/pedropark99/spark_map/blob/main/doc/english/articles/building-mapping.md)).

## Arguments

- `regex`: a *string* (preferably a *raw string*) containing the regular expression to be used;

## Details and examples

Therefore, `matches()` is used to define which columns `spark_map()` will apply the given function to. To use this function, you supply a *raw string* containing the regular expression you want to use. **It is extremely important** that you provide your expression within a *raw string* rather than a traditional *string*, especially if your expression includes special characters like TABs or new lines (`'\t'` or `'\n'`). In Python, *raw strings* are constructed by placing an `'r'` before the quotes in our *string*. Therefore, the expression `r'raw string'` represents a *raw string*, while `'string'` represents a traditional *string*.

It is worth noting that the regular expression provided will be passed to the `re.match()` method, and will be applied to the name of each column of your Spark DataFrame. With that in mind, if your regular expression can't find any column, it's interesting that you investigate your error through the `re.match()` method. For example, suppose you have the DataFrame `pop` below. Suppose also that you want to map all columns that contain the string `'male'` somewhere. Note that no columns were found by `spark_map()`.

```python
data = [
  ('Brazil', 74077777, 86581634, 96536269, 74925448, 88208705, 99177368),
  ('Colombia', 16315306, 19427307, 22159658, 16787263, 20202658, 23063041),
  ('Russia', 69265950, 68593139, 66249411, 78703457, 78003730, 76600057)
]

pop = spark.createDataFrame(
  data,
  ['country', 'pop_male_1990', 'pop_male_2000', 'pop_male_2010',
   'pop_female_1990', 'pop_female_2000', 'pop_female_2010']
)

spark_map(pop, matches(r'male'), F.max).show()
```
```python
KeyError: '`spark_map()` did not found any column that matches your mapping!'
```

To investigate what is going wrong in this case, it is useful to separate the name of a column that should have been found and apply `re.match()` in isolation to that column. Note below that the result of the expression `re.match(r'male', name)` is `None`. This means that the regular expression `'male'` does not generate a match with the text `pop_male_1990`.

```python
name = 'pop_male_1990'
print(re.match(r'male', name))
```
```python
None
```


By testing various combinations and delving deeper into the problem, you may eventually find that the expression `'male'` is wrong as it represents an **exact match** with the text `'male'`. That is, with this expression, `re.match()` is able to find only the text `'male'` and nothing else. We can fix this problem by allowing an arbitrary number of characters to be found around the text `'male'`. For this, we circumvent `'male'` with the mini-expression `'(.+)'`, as shown below:


```python
name = 'pop_male_1990'
print(re.match(r'(.+)male(.+)', name))
```
```python
<re.Match object; span=(0, 13), match='pop_male_1990'>
```

Now that we've tested this new regular expression in `re.match()` we can return to the `matches()` function. Notice below that this time all the expected columns are found.


```python
spark_map(pop, matches(r'(.+)male(.+)'), F.max).show()
```

```
Selected columns by `spark_map()`: pop_male_1990, pop_male_2000, pop_male_2010, pop_female_1990, pop_female_2000, pop_female_2010

+-------------+-------------+-------------+---------------+---------------+---------------+
|pop_male_1990|pop_male_2000|pop_male_2010|pop_female_1990|pop_female_2000|pop_female_2010|
+-------------+-------------+-------------+---------------+---------------+---------------+
|     74077777|     86581634|     96536269|       78703457|       88208705|       99177368|
+-------------+-------------+-------------+---------------+---------------+---------------+
```