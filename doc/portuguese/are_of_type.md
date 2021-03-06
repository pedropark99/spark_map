# `are_of_type()`

## Descrição

Mapear todas as colunas de seu Spark DataFrame que se encaixam em um determinado tipo de dado (string, double, integer, etc.). Essa função é uma das várias funções de mapeamento existentes (leia o artigo [**"Construindo o mapeamento"**](https://github.com/pedropark99/spark_map/blob/main/doc/portuguese/artigos/construindo-mapeamento.md)). 

## Argumentos

- `arg_type`: uma *string* contendo o nome do tipo de dado que você deseja pesquisar (veja os valores disponíveis na seção **"Detalhes e exemplos"** abaixo);

## Detalhes e exemplos

Portanto, `are_of_type()` é utilizada para definir quais são as colunas sobre as quais `spark_map()` vai aplicar a função fornecida. Para utilizar essa função, você deve fornecer um dos seguintes valores:

- `"string"`: para colunas do tipo `pyspark.sql.types.StringType()`;
- `"int"`: para colunas do tipo `pyspark.sql.types.IntegerType()`;
- `"double"`: para colunas do tipo `pyspark.sql.types.DoubleType()`;
- `"date"`: para colunas do tipo `pyspark.sql.types.DateType()`;
- `"datetime"`: para colunas do tipo `pyspark.sql.types.TimestampType()`;

Ou seja, `are_of_type()` somente um dos valores acima. Caso você forneça uma *string* que não esteja inclusa nos valores acima, um `ValueError` é automaticamente acionado pela função, como demonstrado abaixo:

```python
are_of_type("str")
```

```python
ValueError: You must choose one of the following values: 'string', 'int', 'double', 'date', 'datetime'
```

No fundo, `are_of_type()` utiliza o schema de seu Spark DataFrame para determinar quais colunas pertencem ao tipo de dado que você determinou. Repare no exemplo abaixo, que a coluna chamada `"date"` é mapeada por `spark_map()`, mesmo que essa coluna seja claramente uma coluna de datas. Tal fato ocorre, pois Spark está interpretando essa coluna pelo tipo `pyspark.sql.types.StringType()`, e não `pyspark.sql.types.DateType()`.

```python
dados = [
  ("2022-03-01", "Luke", 36981),
  ("2022-02-15", "Anne", 31000),
  ("2022-03-12", "Bishop", 31281)
]

sales = spark.createDataFrame(dados, ['date', 'name', 'value'])

spark_map(sales, are_of_type("string"), F.max).show()
```

```
Selected columns by `spark_map()`: date, name

+----------+----+
|      date|name|
+----------+----+
|2022-03-12|Luke|
+----------+----+
```
