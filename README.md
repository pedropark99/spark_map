

<!-- badges: start -->
[![pytest](https://github.com/pedropark99/spark_map/actions/workflows/pytest.yml/badge.svg)](https://github.com/pedropark99/spark_map/actions/workflows/pytest.yml)
<!-- badges: end -->

# Overview

`spark_map` is a python package that offers some tools that help you to apply a function over multiple columns of Apache Spark DataFrames, using `pyspark`. The package offers two main functions (or "two main methods") to distribute your calculations, which are `spark_map()` and `spark_across()`. Furthermore, the package offers several methods to map (or select) the columns to which you want to apply your calculations (these methods are called *mapping methods* in the package).

# Installation

To get the latest version of `spark_map` at PyPI, use:

```bash
pip install spark_map
```

# Documentation

The full documentation for `spark_map` package is available at the [website of the package](https://pedropark99.github.io/spark_map/). To access it, just use the `Function Reference` and `Articles` menus located at the top navigation bar of the website.



# What problem `spark_map` solves?

When you work a lot with data pipelines using Apache Spark and `pyspark`, at some day, you might find yourself writing a very long `agg()` statement to aggregate multiple columns of my Spark DataFrame with the same function, like this one below:

```python
from pyspark.sql.functions import sum, column
aggregates = (
    spark.table('cards.detailed_sales_per_user')
        .groupBy('day')
        .agg(
            sum(column('cards_lite')).alias('cards_lite'),
            sum(column('cards_silver')).alias('cards_silver'),
            sum(column('cards_gold')).alias('cards_gold'),
            sum(column('cards_premium')).alias('cards_premium'),
            sum(column('cards_enterprise')).alias('cards_enterprise'),
            sum(column('cards_business')).alias('cards_business')
        )
)
```
The problem with this code is that: it is not elegant; and it is error-prone. Because it involves copy and paste, and very subtle changes in each line. Following the golden rule of DRY (*do not repeat yourself*), we need a better way to write this code. That is the exact problem that `spark_map` solves for you!

When you want to apply the same function (like `sum()`) over multiple columns of a Spark DataFrame (like `spark.table('cards.detailed_sales_per_user')`) that might be grouped by a variable (like `day`), you can use the `spark_map` package, to declare this operation in a much better, elegant and concise way, by using the `spark_map()` function.

```python
from spark_map.functions import spark_map
from spark_map.mapping import starts_with
grouped_by_day = spark.table('cards.detailed_sales_per_user')\
    .groupBy('day')

aggregates = spark_map(grouped_by_day, starts_with('cards'), sum)
```

# How `spark_map()` works ?

The `spark_map()` function receives three inputs, which are `table` (i.e. the Spark DataFrame you want to use), `mapping` (i.e. a "mapping" that describes which columns you want to apply your function), and `function` (i.e. the function you want to apply to each column in the Spark DataFrame).

In short, the `starts_with('cards')` section in the above example tells `spark_map()` that you want to apply the input function on all columns of `grouped_by_day` whose name starts with the text `'cards'`. In other words, all `spark_map()` does is to apply the function you want (in the above example this function is `sum()`) to whatever column it finds in the input DataFrame which fits in the description of your mapping method.

You can use different mapping methods to select the columns of your DataFrame, and the package offers several built-in methods which can be very useful for you, which are available through the `spark_map.mapping` module of the package. You can select columns based on:

- `at_position()`: their position (i.e. 3rd, 4th and 5th columns);
- `matches()`: a regex to which their match;
- `are_of_type()`: the type of data their store (i.e. all columns of type `int`);
- `starts_with()` and `ends_with()`: its name starting or ending with a particular pattern;
- `all_of()`: its name being inside a specific list of options;

# Check the documentation for more examples and details

The [website](https://pedropark99.github.io/spark_map) have documentation for all functions of the package. If you have any trouble to understand or to find examples, is a good idea to check the [Function Reference](https://pedropark99.github.io/spark_map/reference-en.html) of the package, to see examples and more details about how each function works.

To understand how the mapping methods works, and how you can create your own mapping method, a good place to start is to read the article [Building the mapping](https://pedropark99.github.io/spark_map/english/articles/building-mapping.html) available at the website of the package.
