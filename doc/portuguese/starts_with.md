# `starts_with()`

## Descrição

Mapear todas as colunas de seu Spark DataFrame cujo o nome se inicia por um texto específico. Essa função é uma das várias funções de mapeamento existentes (leia o artigo [**"Construindo o mapeamento"**](https://github.com/pedropark99/spark_map/blob/main/doc/portuguese/artigos/construindo-mapeamento.md)). 

## Argumentos

- `text`: uma *string* contendo o texto pelo qual você deseja pesquisar;

## Detalhes e exemplos

Portanto, `starts_with()` é utilizada para definir quais são as colunas sobre as quais `spark_map()` vai aplicar a função fornecida. Esta função realiza o processo inverso de `ends_with()`, isto é, ela pesquisa por todas as colunas cujo o nome se inicia por um texto específico. Sendo assim, com a expressão `starts_with("Score")`, `starts_with()` vai mapear todas as colunas cujo o nome se inicia pelo texto `"Score"`. 

Durante o processo de mapeamento, é utilizado sempre um *match* exato entre os *strings* pesquisados. Como resultado, uma expressão como `starts_with("Sales")` não é capaz de mapear colunas como `"sales_brazil"`, `"sales_colombia"` e `"sales_eua"`, porém, é capaz de mapear colunas como `"Sales_france"` e `"Sales_russia"`. Se você precisa ser mais flexível em seu mapeamento, é provável que você deseja utilizar a função `matches()` ao invés de `starts_with()`.

```python
dados = [
  (2022, 1, 12300, 41000, 36981),
  (2022, 2, 19120, 21300, 31000),
  (2022, 3, 18380, 11500, 31281)
]

sales = spark.createDataFrame(dados, ['year', 'month', 'Sales_france', 'sales_brazil', 'Sales_russia'])

spark_map(sales, starts_with('Sales'), F.mean).show()
```

```
Selected columns by `spark_map()`: Sales_france, Sales_russia

+------------+------------------+
|Sales_france|      Sales_russia|
+------------+------------------+
|     16600.0|33087.333333333336|
+------------+------------------+
```