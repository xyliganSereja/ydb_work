# INDEX

Конструкция `INDEX` используется для определения {% if concept_secondary_index %}[вторичного индекса]({{ concept_secondary_index }}){% else %}вторичного индекса{% endif %} {% if backend_name == "YDB" and oss == true %}для [строковых](../../../../concepts/datamodel/table.md#row-oriented-tables) таблиц{% else %}на таблице{% endif %}:

```yql
CREATE TABLE `<table_name>` (
  ...
    INDEX `<index_name>`
    [GLOBAL|LOCAL]
    [UNIQUE]
    [SYNC|ASYNC]
    [USING <index_type>]
    ON ( <index_columns> )
    [COVER ( <cover_columns> )]
    [WITH ( <parameter_name> = <parameter_value>[, ...])]
  [,   ...]
)
```

где:

{% include [index_grammar_explanation.md](../_includes/index_grammar_explanation.md) %}

**Ограничения по типам таблиц.** Обычные глобальные вторичные индексы (`GLOBAL`) и векторные индексы сейчас поддерживаются только для [строковых таблиц](../../../../concepts/datamodel/table.md#row-oriented-tables). Для [колоночных таблиц](../../../../concepts/datamodel/table.md#column-oriented-tables) общая функциональность вторичных индексов (включая GLOBAL) пока в разработке; при этом в `CREATE TABLE` уже можно задавать **локальные Bloom skip индексы**: `INDEX ... LOCAL USING bloom_filter` или `INDEX ... LOCAL USING bloom_ngram_filter`. Параметры и типы индексов см. в [ALTER TABLE ADD INDEX — Локальные Bloom skip индексы](../alter_table/indexes.md#local-bloom-column).

## Пример

```yql
CREATE TABLE my_table (
    a Uint64,
    b Uint64,
    c Utf8,
    d Date,
    INDEX idx_d GLOBAL ON (d),
    INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c),
    PRIMARY KEY (a)
)
```
