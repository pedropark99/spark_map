# `matches()`

## Descrição

Mapear todas as colunas de seu Spark DataFrame que se encaixam em uma expressão regular. Essa função é uma das várias funções de mapeamento existentes (leia o artigo [**"Construindo o mapeamento"**](https://github.com/pedropark99/spark_map/blob/main/doc/portuguese/artigos/construindo-mapeamento.md)). 

## Argumentos

- `regex`: uma *string* contendo a expressão regular a ser utilizada;

## Detalhes e exemplos

Portanto, `matches()` é utilizada para definir quais são as colunas sobre as quais `spark_map()` vai aplicar a função fornecida. Para utilizar essa função, você fornece uma *string* contendo a expressão regular que você deseja utilizar. Essa expressão regular será posteriormente repassada para o método `re.compile()`, e, será aplicada sobre o nome de cada uma das colunas de seu Spark DataFrame. Todos os nomes que derem *"match"* com essa expressão regular, serão coletados e repassados para `spark_map()`.