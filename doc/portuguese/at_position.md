# `at_position()`

## Descrição

Mapear as colunas baseado em seus índices numéricos (1°, 2°, 3° coluna, etc.). Essa função é uma das várias funções de mapeamento existentes (leia o artigo [**"Construindo o mapeamento"**](https://github.com/pedropark99/spark_map/blob/main/doc/portuguese/artigos/construindo-mapeamento.md)). 

## Argumentos

- `*indexes`: os índices das colunas (separados por vírgulas);
- `zero_index`: valor booleano (`True` ou `False`) indicando se os índices fornecidos em `*indexes` são baseados em zero ou não (leia a seção de **"Detalhes"** abaixo). Por padrão, esse argumento é setado para `False`;

## Detalhes

Portanto, `at_position()` é utilizada para definir quais são as colunas sobre as quais `spark_map()` vai aplicar a função fornecida. Os índices fornecidos podem ser.