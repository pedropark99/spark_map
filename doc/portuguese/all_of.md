# `all_of()`

## Descrição

Mapear todas as colunas de seu Spark DataFrame cujo nome esteja incluso em uma lista de strings. Essa função é uma das várias funções de mapeamento existentes (leia o artigo [**"Construindo o mapeamento"**](https://github.com/pedropark99/spark_map/blob/main/doc/portuguese/artigos/construindo-mapeamento.md)). 

## Argumentos

- `list_cols`: uma lista de *strings* contendo os nomes das colunas que você deseja mapear; 

## Detalhes e exemplos

Portanto, `all_of()` é utilizada para definir quais são as colunas sobre as quais `spark_map()` vai aplicar a função fornecida. Você pode utilizar essa função, quando você deseja permitir que um conjunto de colunas seja mapeada, porém, por algum motivo, você não sabe de antemão se todas essas colunas (ou uma parte delas) estará disponível em seu Spark DataFrame. 

Você deve fornecer à `all_of()` uma lista de *strings*. Cada *string* representa o nome de uma coluna que pode ser mapeada. Como exemplo, a expressão `all_of(['sales_france', 'sales_brazil', 'sales_colombia'])` permite que as colunas chamadas `"sales_france"`, `"sales_brazil"` e `"sales_colombia"` sejam mapeadas por `spark_map()`. Porém, não necessariamente `spark_map()` precisa encontrar todas essas colunas de uma vez só. Ou seja, `all_of()` torna essas colunas "opcionais", logo, `spark_map()` pode encontrar as três colunas, ou, apenas duas, ou ainda, apenas uma dessas colunas. Veja o exemplo abaixo:

```python
dados = [
  (2022, 1, 12300, 41000, 36981),
  (2022, 2, 19120, 21300, 31000),
  (2022, 3, 18380, 11500, 31281)
]

sales = spark.createDataFrame(dados, ['year', 'month', 'sales_france', 'sales_brazil', 'sales_russia'])

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

Contudo, vale destacar que, `spark_map()` **precisa encontrar pelo menos uma das colunas definidas** em `all_of()`. Caso isso não ocorra, `spark_map()` vai levantar um `KeyError` avisando que nenhuma coluna pode ser encontrada com o mapeamento que você definiu.

```python
spark_map(sales, all_of(['sales_italy']), F.mean).show()
```

```python
KeyError: '`spark_map()` did not found any column that matches your mapping!'
```
