# `spark_map()`
## Introdução

Com `spark_map()` você é capaz de aplicar uma função sobre múltiplas colunas de um Spark DataFrame. Em resumo, `spark_map()` recebe um Spark DataFrame como *input* e retorna um novo Spark DataFrame (agregado pela função que você forneceu) como *output*. Como exemplo, considere o DataFrame `students` abaixo:

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
  'StudentID', 'Name', 'Age', 'Heigth', 'Score1',
  'Score2', 'Score3', 'Score4', 'Course', 'Department'
] 

students = spark.createDataFrame(d, columns)
students.show(truncate = False)
```

```
+---------+-------+---+------+------+------+------+------+---------+----------+
|StudentID|Name   |Age|Heigth|Score1|Score2|Score3|Score4|Course   |Department|
+---------+-------+---+------+------+------+------+------+---------+----------+
|12114    |Anne   |21 |1.56  |8     |9     |10    |9     |Economics|SC        |
|13007    |Adrian |23 |1.82  |6     |6     |8     |7     |Economics|SC        |
|10045    |George |29 |1.77  |10    |9     |10    |7     |Law      |SC        |
|12459    |Adeline|26 |1.61  |8     |6     |7     |7     |Law      |SC        |
|10190    |Mayla  |22 |1.67  |7     |7     |7     |9     |Design   |AR        |
|11552    |Daniel |24 |1.75  |9     |9     |10    |9     |Design   |AR        |
+---------+-------+---+------+------+------+------+------+---------+----------+
```

Suponha que você deseja calcular a média da terceira, quarta e quinta coluna desse DataFrame `students`. A função `spark_map()` te permite realizar esse cálculo de maneira extremamente simples e clara, como demonstrado abaixo:

```python
import pyspark.sql.functions as F
spark_map(students, at_position(3, 4, 5), F.mean).show(truncate = False)
```
```
Selected columns by `spark_map()`: Heigth, Score1, Score2

+------------------+------+-----------------+
|Heigth            |Score1|Score2           |
+------------------+------+-----------------+
|1.6966666666666665|8.0   |7.666666666666667|
+------------------+------+-----------------+
```

Se você deseja que seu cálculo seja aplicado por grupo, basta fornecer a tabela já agrupada para `spark_map()`. Por exemplo, suponha que você desejasse calcular as mesmas médias do exemplo acima, porém, dentro de cada departamento:

```python
import pyspark.sql.functions as F
by_department = students.groupBy('Department')
spark_map(by_department, at_position(3, 4, 5), F.mean).show()
```

```
Selected columns by `spark_map()`: Heigth, Score1, Score2

+----------+------------------+------+------+
|Department|            Heigth|Score1|Score2|
+----------+------------------+------+------+
|        AR|              1.71|   8.0|   8.0|
|        SC|1.6900000000000002|   8.0|   7.5|
+----------+------------------+------+------+
```

## Argumentos

- `table`: um Spark DataFrame ou um DataFrame agrupado (i.e. `pyspark.sql.DataFrame` ou `pyspark.sql.GroupedData`);
- `mapping`: o mapeamento que define as colunas onde você deseja aplicar `function` (veja a seção **"Construindo o mapeamento"** abaixo);
- `function`: a função que você deseja aplicar em cada coluna definida no `mapping`;


## Você define o cálculo e `spark_map()` distribui ele

Tudo que `spark_map()` faz é aplicar uma função qualquer sobre um conjunto de colunas de seu DataFrame. E essa função pode ser qualquer função, desde que seja uma função agregadora (isto é, uma função que pode ser utilizada dentro dos métodos `pyspark.sql.DataFrame.agg()` e `pyspark.sql.GroupedData.agg()`). Desde que sua função atenda esse requisito, você pode definir a fórmula de cálculo que quiser, e, utilizar `spark_map()` para distribuir esse cálculo ao longo de várias colunas.

Como exemplo, suponha você precisasse utilizar um pouco de inferência para testar se a média dos vários Scores dos estudantes se distancia significativamente de 6, através da estatística produzida por um teste *t*:

```python
def t_test(x, value_test = 6):
  return ( F.mean(x) - F.lit(value_test) ) / ( F.stddev(x) / F.sqrt(F.count(x)) )

results = spark_map(students, starts_with("Score"), t_test)
results.show(truncate = False)
```

```
Selected columns by `spark_map()`: Score1, Score2, Score3, Score4

+-----------------+------------------+-----------------+----------------+
|           Score1|            Score2|           Score3|          Score4|
+-----------------+------------------+-----------------+----------------+
|3.464101615137754|2.7116307227332026|4.338609156373122|4.47213595499958|
+-----------------+------------------+-----------------+----------------+
```


## Construindo o mapeamento

Você precisa fornecer um mapeamento (ou *mapping*) para a função `spark_map()`. Esse mapeamento define quais são as colunas que `spark_map()` deve aplicar a função fornecida no argumento `function`. Você pode construir esse *mapping* utilizando uma das funções de mapeamento, que são as seguintes:

- `at_position()`: mapeia as colunas que estão em certas posições (1° coluna, 2° coluna, 3° coluna, etc.);
- `starts_with()`: mapeia as colunas cujo nome começa por uma *string* específica;
- `ends_with()`: mapeia as colunas cujo nome termina por uma *string* específica;
- `matches()`: mapeia as colunas cujo nome se encaixa em uma expressão regular;
- `are_of_type()`: mapeia as colunas que pertencem a um tipo de dado específico (*string*, *integer*, *double*, etc.);
- `all_of()`: mapeia todas as colunas que estão inclusas dentro de uma lista específica;

Como um primeiro exemplo, você pode utilizar a função `at_position()` sempre que você deseja selecionar as colunas por posição. Portanto, se você deseja


No fundo, o mapeamento é apenas uma pequena descrição contendo o algoritmo que deve ser utilizado para selecionar as colunas e o valor que será repassado a este algoritmo. Como exemplo, o resultado da expressão `at_position(3, 4, 5)` é um pequeno `dict`, contendo dois elementos (`fun` e `val`). O elemento `fun` define a função/algoritmo a ser utilizado para selecionar a coluna, e o elemento `val` guarda o valor que será repassado para essa função/algoritmo.

```python
at_position(3, 4, 5)
```
```python
{'fun': '__at_position', 'val': (3, 4, 5)}
```

O resultado da expressão `matches('^Score')` é bastante similar. Porém, diferente do exemplo anterior que utiliza uma função interna chamada `__at_position`, dessa vez, o algoritmo a ser utilizado é o que está armazenado em uma função chamada `__matches`, e `'^Score'` é o valor que essa função vai utilizar.

```python
matches('^Score')
```
```python
{'fun': '__matches', 'val': '^Score'}
```

Isso significa que você poderia **implementar o seu próprio algoritmo de mapeamento**, e, fornecer à `spark_map()` um `dict` contendo o nome da função que contém esse algoritmo e, o valor que deve ser repassado para essa função (os elementos `fun` e `val`). Contudo, vale destacar que, se você tentar utilizar uma função que não existe (isto é, uma função que ainda não foi definida), você terá como resultado um `KeyError`. 

Repare no exemplo abaixo, em que tento utilizar uma função chamada `some_mapping_function()` com o valor `'some_value'` para mapear as colunas. Pelo fato de `spark_map()` não encontrar nenhuma função chamada `some_mapping_function()` definida em minha sessão, um `KeyError` acaba sendo levantado. Portanto, se você enfrentar esse erro ao utilizar `spark_map()`, investigue se você definiu corretamente o seu mapeamento e a função que você deseja utilizar.

```python
spark_map(students, {'fun': 'some_mapping_function', 'val': 'some_value'}, F.sum)
```

```python
KeyError: 'some_mapping_function'
```


