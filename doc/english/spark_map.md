# `spark_map()`
## Overview

With `spark_map()`, you can apply a function to a set of columns in a spark DataFrame. It works both for normal spark DataFrame, and, for grouped spark DataFrame (in other words, for a `pyspark.sql.DataFrame.groupBy()`). The function receives a spark DataFrame as input, and returns a new aggregated spark DataFrame as output. For example, to apply the pyspark `mean()` function, to the third, fourth and fifth columns of the `lima.vweventtracks` DataFrame, you do:

```python
import pyspark.sql.functions as F
tb = spark.table('lima.vweventtracks')
spark_map(tb, at_position(3,4,5), F.mean)
```

## Arguments

- `table`: a spark DataFrame or a grouped DataFrame (i.e. `pyspark.sql.DataFrame` or `pyspark.sql.GroupedData`);
- `mapping`: a `dict` containing the mapping that defines the columns where you want to apply the `function` (this mapping is defined by one of the many mapping functions, see **"Building the mapping"** section below);
- `function`: the function that you want to apply on each column defined in the `mapping`;