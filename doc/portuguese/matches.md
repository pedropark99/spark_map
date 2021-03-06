# `matches()`

## Descrição

Mapear todas as colunas de seu Spark DataFrame que se encaixam em uma expressão regular. Essa função é uma das várias funções de mapeamento existentes (leia o artigo [**"Construindo o mapeamento"**](https://github.com/pedropark99/spark_map/blob/main/doc/portuguese/artigos/construindo-mapeamento.md)). 

## Argumentos

- `regex`: uma *string* (de preferência, uma *raw string*) contendo a expressão regular a ser utilizada;

## Detalhes e exemplos

Portanto, `matches()` é utilizada para definir quais são as colunas sobre as quais `spark_map()` vai aplicar a função fornecida. Para utilizar essa função, você fornece uma *raw string* contendo a expressão regular que você deseja utilizar. **É extremamente importante** que você forneça sua expressão dentro de uma *raw string*, ao invés de uma *string* tradicional, especialmente se sua expressão inclui caracteres especiais como TABs ou *new lines* (`'\t'` ou `'\n'`). No Python, *raw strings* são construídas ao posicionarmos um `'r'` antes das aspas de nossa *string*. Portanto, a expressão `r'raw string'` representa uma *raw string*, enquanto `'string'`, representa uma *string* tradicional.

Vale destacar que, a expressão regular fornecida será repassada para o método `re.match()`, e será aplicada sobre o nome de cada uma das colunas de seu Spark DataFrame. Tendo isso em mente, caso a sua expressão regular não consiga encontrar nenhuma coluna, é interessante que você investigue o seu erro através do método `re.match()`. Por exemplo, suponha que você tenha o DataFrame `pop` abaixo. Suponha também que você deseja selecionar todas as colunas que contém a sequência `'male'` em algum lugar. Perceba que, nenhuma coluna foi encontrada por `spark_map()`.

```python
dados = [
  ('Brazil', 74077777, 86581634, 96536269, 74925448, 88208705, 99177368),
  ('Colombia', 16315306, 19427307, 22159658, 16787263, 20202658, 23063041),
  ('Russia', 69265950, 68593139, 66249411, 78703457, 78003730, 76600057)
]

pop = spark.createDataFrame(
  dados,
  ['country', 'pop_male_1990', 'pop_male_2000', 'pop_male_2010', 
   'pop_female_1990', 'pop_female_2000', 'pop_female_2010']
)

spark_map(pop, matches(r'male'), F.max).show()
```
```python
KeyError: '`spark_map()` did not found any column that matches your mapping!'
```

Para investigar o que está ocorrendo de errado nesse caso, é útil separarmos o nome de uma coluna que deveria ter sido encontrada e, aplicarmos `re.match()` de forma isolada sobre essa coluna. Perceba abaixo que, o resultado da expressão `re.match(r'male', nome)` é `None`. Isso significa que a expressão regular `'male'` de fato não gera um *"match"* com o texto `pop_male_1990`. 

```python
nome = 'pop_male_1990'
print(re.match(r'male', nome))
```
```python
None
```

Ao testar várias combinações e investigar mais a fundo o problema, você eventualmente pode entender que a expressão `'male'` está errada pois ela representa um *"match"* exato com o texto `'male'`. Ou seja, com essa expressão, `re.match()` é capaz de encontrar apenas o texto `'male'` e nada mais. Podemos corrigir esse problema, ao permitirmos que um número arbitrário de caracteres seja encontrado ao redor do texto `'male'`. Para isso, contornamos `'male'` com a mini-expressão `'(.+)'`, como demonstrado abaixo:


```python
nome = 'pop_male_1990'
print(re.match(r'(.+)male(.+)', nome))
```

Agora que testamos essa nova expressão regular em `re.match()` podemos retornar à função `matches()`. Perceba abaixo que, dessa vez, todas as colunas esperadas são encontradas.


```python
spark_map(pop, matches(r'(.+)male(.+)'), F.max).show()
```

```
Selected columns by `spark_map()`: pop_male_1990, pop_male_2000, pop_male_2010, pop_female_1990, pop_female_2000, pop_female_2010

+-------------+-------------+-------------+---------------+---------------+---------------+
|pop_male_1990|pop_male_2000|pop_male_2010|pop_female_1990|pop_female_2000|pop_female_2010|
+-------------+-------------+-------------+---------------+---------------+---------------+
|     74077777|     86581634|     96536269|       78703457|       88208705|       99177368|
+-------------+-------------+-------------+---------------+---------------+---------------+
```











