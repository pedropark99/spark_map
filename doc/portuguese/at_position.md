# `at_position()`

## Descrição

Mapear as colunas de seu Spark DataFrame baseado em seus índices numéricos (1°, 2°, 3° coluna, etc.). Essa função é uma das várias funções de mapeamento existentes (leia o artigo [**"Construindo o mapeamento"**](https://github.com/pedropark99/spark_map/blob/main/doc/portuguese/artigos/construindo-mapeamento.md)). 

## Argumentos

- `*indexes`: os índices das colunas (separados por vírgulas);
- `zero_index`: valor booleano (`True` ou `False`) indicando se os índices fornecidos em `*indexes` são baseados em zero ou não (leia a seção de **"Detalhes"** abaixo). Por padrão, esse argumento é setado para `False`;

## Detalhes e exemplos

Portanto, `at_position()` é utilizada para definir quais são as colunas sobre as quais `spark_map()` vai aplicar a função fornecida. Para utilizar essa função, você fornece os índices numéricos, separados por vírgulas, que representam as colunas que você deseja mapear em `spark_map()`. 

O argumento `zero_index` é opcional, e determina se os índices das colunas fornecidos serão baseados em um sistema de índices iniciado em zero, ou, em um sistema inciado em um. A linguagem Python utiliza um sistema de índices iniciado em zero, logo, o valor 0 representa o primeiro valor de um objeto, enquanto 1, o segundo valor de um objeto, e assim por diante. 

Em contrapartida, o argumento `zero_index` é configurado por padrão para `False`. Por causa disso, a função `at_position()` trabalha sempre inicialmente com um sistema de índices iniciado em um. Logo, na expressão `at_position(3, 4, 5)`, a função `at_position()` vai mapear a 3°, 4° e 5° colunas de seu Spark DataFrame. Porém, caso você queira sobrepor esse comportamento, e, utilizar o sistema de índices padrão do Python (iniciado em zero), basta configurar esse argumento para `True`. No exemplo abaixo, `at_position()` vai mapear a 2°, 3° e 4° coluna do DataFrame `sales`.

```python
dados = [
  (2022, 1, 12300, 41000, 36981),
  (2022, 2, 19120, 21300, 31000),
  (2022, 3, 18380, 11500, 31281)
]

sales = spark.createDataFrame(dados, ['year', 'month', 'france_Sales', 'brazil_sales', 'russia_Sales'])

spark_map(sales, at_position(1, 2, 3, zero_index = True), F.mean).show()
```

```
Selected columns by `spark_map()`: month, france_Sales, brazil_sales

+-----+------------+------------+
|month|france_Sales|brazil_sales|
+-----+------------+------------+
|  2.0|     16600.0|     24600.0|
+-----+------------+------------+
```

Ao fornecer um índice zero, você sempre deve configurar o argumento `zero_index` para `True`. Quando o argumento `zero_index` está setado para `False`, `at_position()` vai automaticamente subtrair 1 de todos os índices. Logo, um índice igual a zero, se torna um índice igual a -1, e índices negativos não são permitidos por `at_position()`. Veja o exemplo abaixo:

```python
at_position(0, 2, 4)
```

```python
ValueError: 'One (or more) of the provided indexes are negative! Did you provided a zero index, and not set the `zero_index` argument to True?'
```

Além disso, todo e qualquer índice duplicado é automaticamente eliminado por `at_position()`. Veja o exemplo abaixo em que, os índices 1 e 4 estão repetidos durante a chamada à `at_position()`, mas, são automaticamente eliminados no resultado da função.

```python
at_position(1, 1, 2, 3, 4, 4, 5)
```

```python
{'fun': '__at_position', 'val': (0, 1, 2, 3, 4)}
```

Para mais, os índices fornecidos à `at_position()` não devem estar dentro de uma lista, caso você cometa esse erro, a função vai levantar um `ValueError`, como demonstrado abaixo:

```python
at_position([4, 5, 6])
```
```python
ValueError: 'Did you provided your column indexes inside a list? You should not encapsulate these indexes inside a list. For example, if you want to select 1° and 3° columns, just do `at_position(1, 3)` instead of `at_position([1, 3])`'.
```

Os índices das colunas são um argumento obrigatório. Logo, caso você não forneça nenhum índice, `at_position()` vai obrigatoriamente levantar um `ValueError`, como demonstrado abaixo:


```python
at_position(zero_index = True)
```
```python
ValueError: 'You did not provided any index for `at_position()` to search'.
```

