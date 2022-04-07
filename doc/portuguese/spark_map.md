# `spark_map()`
## Introdução

Com `spark_map()` você pode aplicar uma função a um conjunto de colunas em um Spark DataFrame. Essa função aceita tanto um Spark DataFrame normal quanto um Spark DataFrame agrupado (em outras palavras, `pyspark.sql.DataFrame.groupBy()`). A função recebe um Spark DataFrame como *input* e retorna um novo Spark DataFrame agregado como *output*. Por exemplo, para aplicar a função pyspark `mean()`, à terceira, quarta e quinta colunas do DataFrame `lima.vweventtracks`, você faria:

```python
import pyspark.sql.functions as F
tb = spark.table('lima.vweventtracks')
spark_map(tb, at_position(3, 4, 5), F.mean)
```

## Argumentos

- `table`: um Spark DataFrame ou um DataFrame agrupado (i.e. `pyspark.sql.DataFrame` ou `pyspark.sql.GroupedData`);
- `mapping`: um `dict` contendo o mapeamento que define as colunas onde você deseja aplicar `function` (este mapeamento é construído por uma das muitas funções de mapeamento disponíveis, veja a seção **"Construindo o mapeamento"** abaixo);
- `function`: a função que você deseja aplicar em cada coluna definida no `mapping`;