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
- `mapping`: um `dict` contendo o mapeamento que define as colunas onde você deseja aplicar `function` (este mapeamento é construído por uma das muitas funções de mapeamento disponíveis, veja a seção **"Construindo o mapeamento"** abaixo);
- `function`: a função que você deseja aplicar em cada coluna definida no `mapping`;


## Construindo o mapeamento

Você precisa fornecer um mapeamento (ou *mapping*) para a função `spark_map()`. Esse mapeamento define quais são as colunas que `spark_map()` deve aplicar a função fornecida no argumento `function`. Você pode construir esse mapeamento através das funções de mapeamento, que são as seguintes:

- `at_position()`: mapeia as colunas que estão em certas posições (1° coluna, 2° coluna, 3° coluna, etc.);
- `starts_with()`: mapeia as colunas cujo nome começa por uma *string* específica;
- `ends_with()`: mapeia as colunas cujo nome termina por uma *string* específica;
- `matches()`: mapeia as colunas cujo nome se encaixa em uma expressão regular;
- `are_of_type()`: mapeia as colunas que pertencem a um tipo de dado específico (*string*, *integer*, *double*, etc.);
- `all_of()`: mapeia todas as colunas que estão inclusas dentro de uma lista específica;






