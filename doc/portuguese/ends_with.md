# `ends_with()`

## Descrição

Mapear todas as colunas de seu Spark DataFrame cujo o nome termina por um texto específico. Essa função é uma das várias funções de mapeamento existentes (leia o artigo [**"Construindo o mapeamento"**](https://github.com/pedropark99/spark_map/blob/main/doc/portuguese/artigos/construindo-mapeamento.md)). 

## Argumentos

- `text`: uma *string* contendo o texto pelo qual você deseja pesquisar;

## Detalhes e exemplos

Portanto, `ends_with()` é utilizada para definir quais são as colunas sobre as quais `spark_map()` vai aplicar a função fornecida. Esta função realiza o processo inverso de `starts_with()`, isto é, ela pesquisa por todas as colunas cujo o nome termina por um texto específico. Sendo assim, com a expressão `ends_with("Score")`, `ends_with()` vai mapear todas as colunas cujo o nome termina pelo texto `"Score"`.

Durante o processo de mapeamento, é utilizado sempre um *match* exato entre os *strings* pesquisados. Como resultado, uma expressão como `ends_with("Sales")` não é capaz de mapear colunas como `"brazil_sales"`, `"colombia_sales"` e `"eua_sales"`, porém, é capaz de mapear colunas como `"france_Sales"` e `"russia_Sales"`. Se você precisa ser mais flexível em seu mapeamento, é provável que você deseja utilizar a função `matches()` ao invés de `ends_with()`.