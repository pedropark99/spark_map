
# Construindo o mapeamento

Você precisa fornecer um mapeamento (ou *mapping*) para a função `spark_map()`. Esse mapeamento define quais são as colunas que `spark_map()` deve aplicar a função fornecida no argumento `function`. Você pode construir esse *mapping* utilizando uma das funções de mapeamento, que são as seguintes:

- `at_position()`: mapeia as colunas que estão em certas posições (1° coluna, 2° coluna, 3° coluna, etc.);
- `starts_with()`: mapeia as colunas cujo nome começa por uma *string* específica;
- `ends_with()`: mapeia as colunas cujo nome termina por uma *string* específica;
- `matches()`: mapeia as colunas cujo nome se encaixa em uma expressão regular;
- `are_of_type()`: mapeia as colunas que pertencem a um tipo de dado específico (*string*, *integer*, *double*, etc.);
- `all_of()`: mapeia todas as colunas que estão inclusas dentro de uma lista específica;

Como um primeiro exemplo, você pode utilizar a função `at_position()` sempre que você deseja selecionar as colunas por posição. Portanto, se você deseja selecionar a primeira, segunda, terceira e quarta coluna, você fornece os respectivos índices dessas colunas à função `at_position()`. 

Por outro lado, você talvez precise utilizar um outro método para mapear as colunas que você está interessado. Por exemplo, o DataFrame `students` possui 4 colunas de Scores (`Score1`, `Score2`, `Score3` e `Score4`), e temos duas formas óbvias de mapearmos todas essas colunas. Uma forma é utilizando a função `starts_with()`, e outra, através da função `matches()`. Ambas as opções abaixo trazem os mesmos resultados.

```python
spark_map(students, starts_with("Score"), F.sum).show(truncate = False)
```

```python
spark_map(students, matches("[0-9]$"), F.sum).show(truncate = False)
```


No fundo, o mapeamento é apenas uma pequena descrição contendo o algoritmo que deve ser utilizado para encontrar as colunas e o valor que será repassado a este algoritmo. Como exemplo, o resultado da expressão `at_position(3, 4, 5)` é um pequeno `dict`, contendo dois elementos (`fun` e `val`). O elemento `fun` define a função/algoritmo a ser utilizado para encontrar as colunas, e o elemento `val` guarda o valor que será repassado para essa função/algoritmo.

```python
at_position(3, 4, 5)
```
```python
{'fun': '__at_position', 'val': (3, 4, 5)}
```

O resultado da expressão `matches('^Score')` é bastante similar. Porém, diferente do exemplo anterior que utiliza uma função interna chamada `__at_position`, dessa vez, o algoritmo a ser utilizado é o que está armazenado em uma função chamada `__matches`, e `'^Score'` é o valor que será repassado a essa função.

```python
matches('^Score')
```
```python
{'fun': '__matches', 'val': '^Score'}
```

## Criando o seu próprio método de mapeamento

Isso significa que você poderia **implementar o seu próprio algoritmo de mapeamento**, e, fornecer à `spark_map()` um `dict` contendo o nome da função que contém esse algoritmo e, o valor que deve ser repassado para essa função (os elementos `fun` e `val`). Toda função de mapeamento deve ter três argumentos: 1) um valor arbitrário para o algoritmo; 2) os nomes das colunas do Spark DataFrame como uma lista de strings (basicamente, o resultado de `pyspark.sql.DataFrame.columns`); 3) o esquema (ou *schema*) do Spark DataFrame (este é um objeto da classe `StructType`, ou, basicamente, o resultado de `pyspark.sql.DataFrame.schema`).

Sua função de mapeamento deve sempre ter esses três argumentos, mesmo que ela não use todos eles. Por exemplo, a função abaixo mapeia a coluna que está em uma posição específica na ordem alfabética. Esta função `alphabetic_order()` usa apenas os argumentos `index` e `cols`, mesmo que ela receba três argumentos. Outro requisito é o valor de retorno de sua função de mapeamento. Sua função de mapeamento deve sempre retornar uma lista de *strings*, a qual contém os nomes das colunas que foram mapeadas pela função. Caso a sua função de mapeamento não encontre nenhuma coluna durante a sua pesquisa, ela deve retornar uma lista vazia.

``` python
def alphabetic_order(index, cols: list, schema: StructType):
    cols.sort()
    return cols[index]
```

Para demonstrar essa função, vamos usar o DataFrame spark `sales.sales_per_country` como exemplo. A lista de colunas deste DataFrame está exposta abaixo:

```python
sales = spark.table('sales.sales_per_country')
print(sales.columns)
```

```python
['year', 'month', 'country', 'idstore', 'totalsales']
```

Agora, usando o `alphabetic_order()` dentro de `spark_map()`:

```python
spark_map(sales, {'fun' = 'alphabetic_order', 'val' = 2}, F.max)
```

```
Selected columns by `spark_map()`: idstore

+--------+
| idstore|
+--------+
|    2300|
+--------+
```


## Tome cuidado ao utilizar funções de mapeamento personalizadas

Contudo, vale destacar que, se você tentar utilizar em seu mapeamento uma função que não existe (isto é, uma função que ainda não foi definida em sua sessão), você terá como resultado um `KeyError`. Repare no exemplo abaixo, em que tento utilizar uma função chamada `some_mapping_function()` com o valor `'some_value'` para mapear as colunas. Pelo fato de `spark_map()` não encontrar nenhuma função chamada `some_mapping_function()` definida em minha sessão, um `KeyError` acaba sendo levantado. Portanto, se você enfrentar esse erro ao utilizar `spark_map()`, investigue se você definiu corretamente a função que você deseja utilizar em seu mapeamento.

```python
spark_map(students, {'fun': 'some_mapping_function', 'val': 'some_value'}, F.sum)
```

```python
KeyError: 'some_mapping_function'
```

## Caso o seu mapeamento não encontre nenhuma coluna

Por outro lado, `spark_map()` também vai levantar um `KeyError`, caso a função que você esteja utilizando em seu mapeamento não encontre nenhuma coluna em seu DataFrame. Porém, nesse caso, `spark_map()` emite uma mensagem clara de que nenhuma coluna foi encontrada utilizando o mapeamento que você definiu. Como exemplo, poderíamos reproduzir esse erro, ao tentar mapear no DataFrame `students`, todas as colunas que começam pela *string* `'april'`. Um `KeyError` é levantado nesse caso pois não existe nenhuma coluna na tabela `students`, cujo nome começe pela palavra "april".

```python
spark_map(students, starts_with('april'), F.sum)
```
```python
KeyError: `spark_map()` did not found any column that matches your mapping!
```

