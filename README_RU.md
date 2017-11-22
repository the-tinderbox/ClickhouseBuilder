# Clickhouse Query Builder
[![Build Status](https://travis-ci.org/the-tinderbox/ClickhouseBuilder.svg?branch=master)](https://travis-ci.org/the-tinderbox/ClickhouseBuilder) [![Coverage Status](https://coveralls.io/repos/github/the-tinderbox/ClickhouseBuilder/badge.svg?branch=master)](https://coveralls.io/github/the-tinderbox/ClickhouseBuilder?branch=master)

# Requirements

`php 7.1`

# Install

Composer

```bash
composer require the-tinderbox/clickhouse-builder
```

# Using
Для того, что бы использовать билдер необходимо передать в конструктор клиент `the-tinderbox/clickhouse-php-client`.

```php
$server = new Tinderbox\Clickhouse\Server('127.0.0.1', '8123', 'default', 'user', 'pass');
$client = new Tinderbox\Clickhouse\Client($server);
$builder = new Builder($client);
```
После этого можно писать запросы и выполнять их.

### Select columns

```php
$builder->select('column', 'column2', 'column3 as alias');
$builder->select(['column', 'column2', 'column3 as alias']);
$builder->select(['column', 'column2', 'column3' => 'alias']);
```

Все эти вызове будут преобразованы в такой запрос:

```sql
SELECT `column`, `column2`, `column3` AS `alias` 
```

Так же в качестве колонки может быть передано замыкание `Closure`. В таком случае в замыкание будет передан инстанс
`Column` которые можно настроить как угодно, используя различные функции в виде суммы, умножения и пр. Так же можно
использовать подзапросы

**Пример с функциями**
 
```php
$builder->select(function ($column) {
    $column->name('time')->sumIf('time', '>', 10); 
});
```

Конечный запрос будет таким:

```sql
SELECT sumIf(`time`, time > 10) 
```

**Пример с подзапросом**

```php
$builder->select(function ($column) {
    $column->as('alias')
    ->query()
    ->select('column')
    ->from('table'); 
});
```

Конечный запрос будет таким:

```sql
SELECT  (SELECT `column` FROM `table) as `alias`
```

Подобный результат можно получить другими способовами:

```php
$1 = $builder->select(function ($column) {
     $column->as('alias')
            ->query(function ($query) {
                $query->select('column')->from('table');
            })
});

$2 = $builder->select(function ($column) {
        $subQuery = $builder->select('column')->from('table');
        
        $column->as('alias')
                ->query($subQuery); 
});
```

**Внимание! Функции на колонки пока в разработке и лучше воздержаться от их использования в продакшене.**

### From

Указание таблицы для выборки данных

```php
$builder->select('column')->from('table', 'alias');
```

Преобразует запрос в такой вид: 

```sql
SELECT `column` FROM `table` as `alias` 
```

Так же можно передать замыкание `Closure` или инстанс `Builder` для использования подзапроса

```php
$builder->from(function ($from) {
    $from->query()->select('column')->from('table');
});
```

Конечный запрос будет таким:

```sql
SELECT * FROM (SELECT `column` FROM `table`) 
```

или

```php
$builder->from(function ($from) {
    $from->query(function ($query) {
        $query->select('column')->from('table');
    });
});
```

или

```php
$builder->from(function ($from) {
    $from->query($builder->select('column')->from('table'));
});
```

или

```php
$builder->from($builder->select('column')->from('table'));
```

Все эти способы сформируют запрос как описано выше.

### Sample coefficient

Указание коэффициента для семплирования.

```php
$builder->select('column')->from('table')->sample(0.1);
```

Сформирует такой запрос:

```sql
SELECT `column` FROM `table` SAMPLE 0.1 
```

### Joins

Джоины таблиц и подзапросов

```php
$builder->from('table')->join('another_table', 'any', 'left', ['column1', 'column2'], true);
```

Сформирует такой запрос:

```sql
SELECT * FROM `table` GLOBAL ANY LEFT JOIN `another_table` USING `column1`, `column2` 
```

Для того что бы в качестве таблицы использовать результат подзапроса можно передать замыкание `Closure` или 
инстанс `Builder`

```php
$builder->from('table')->join(function ($query) {
    $query->select('column1', 'column2')->from('table2');
}, 'any', 'left', ['column1', 'column2']);

$builder->from('table')->join($builder->select('column1', 'column2')->from('table2'), 'any', 'left', ['column1', 'column2']);
```

Результатом будет такой запрос:

```sql
SELECT * FROM `table` ANY LEFT JOIN (SELECT `column1`, `column2` FROM `table2`) USING `column1`, `column2` 
```

Для упрощения использования джоинов есть несколько функций-синонимов с предопределенными параметрами джоина.
 
```php
$builder->from('table')->anyLeftJoin('table', ['column']);
$builder->from('table')->allLeftJoin('table', ['column']);
$builder->from('table')->allInnerJoin('table', ['column']);
$builder->from('table')->anyInnerJoin('table', ['column']);

$buulder->from('table')->leftJoin('table', 'any', ['column']);
$buulder->from('table')->innerJoin('table', 'all', ['column']);
```

### Temporary tables usage

Бывают случаи когда вам требуется отфильтровать данные, например пользователей по их идентификаторам, но кол-во
идентификаторов исчисляется миллионами. Вы можете разместить их в локальном файле и использовать его в качестве
временной таблицы на сервере.

```php
/*
 * Добавим файл с идентификаторами пользователей как таблицу _users
 * Так же мы должны указать структуру таблицы. В примере ниже
 * структура таблицы будет ['UInt64']
 */
$builder->addFile('users.csv', '_users', ['UInt64']);
$builder->select(raw('count()'))->from('clicks')->whereIn('userId', new Identifier('_users'));
```

Конечный запрос будет таким:

```sql
SELECT count() FROM `clicks` WHERE `userId` IN `_users`
```

**Если вы хотите, что бы таблицы подхватывались автоматически, то вам следует вызывать `addFile` до того как вы вызываете `whereIn`.**

Локальные файлы можно так же использовать в `whereIn`, `prewhereIn`, `havingIn` и `join` методах билдера.

### Prewhere, where, having

Условия фильтрации данных prewhere, where и having. Все примеры описаны для where, но prewhere и having имеют
точно такой же набор функций.

```php
$builder->from('table')->where('column', '=', 'value');
$builder->from('table')->where('column', 'value');
```

Преобразует в такой запрос:

```sql
SELECT * FROM `table` WHERE `column` = 'value' 
```

Все строки оборачиваются в одинарные ковычки.
Если не передан оператор, то будет использован оператор равенства `=`.
Если не передан оператор и значение является массивом, то будет использован оператор `IN`.

```php
$builder->from('table')->where(function ($query) {
    $query->where('column1', 'value')->where('column2', 'value');
});
```

Преобразует в такой запрос:

```sql
SELECT * FROM `table` WHERE (`column1` = 'value' AND `column2` = 'value') 
```

Если первым аргументом было передано замыкание `Closure`, то все where внутри этого замыкания будут обернуты
в скобки как группа условий.

Но если на инстанс `$query` внутри замыкания будет выполнен метод `from`, то группа этих условий превратится в подзапрос.

```php
$builder->from('table')->where(function ($query) {
    $query->select('column')->from('table');
})
```

```sql
SELECT * FROM `table` WHERE (SELECT `column` FROM `table`) 
```

Такое же поведение будет и у параметра `$value` за исключением оборачивания в скобки. Любое замыкание переданное в
`$value` будет преобразовано в подзапрос.

```php
$builder->from('table')->where('column', 'IN', function ($query) {
    $query->select('column')->from('table');
});
```

Преобразует в такой запрос:

```sql
SELECT * FROM `table` WHERE `column` IN (SELECT `column` FROM `table`) 
```

Так же можно использовать предподготовленное условие в виде `TwoElementsLogicExpression`, но лучше не использовать этот
способ написания условий.

Так же как и у джоинов существует множество функций-хелперов для where, prewhere и having.

```php
$builder->where();
$builder->orWhere();

$builder->whereRaw();
$builer->orWhereRaw();

$builder->whereIn();
$builder->orWhereIn();

$builder->whereGlobalIn();
$builder->orWhereGlobalIn();

$builder->whereGlobalNotIn();
$builder->orWhereGlobalNotIn();

$builder->whereNotIn();
$builder->orWhereNotIn();

$builder->whereBetween();
$builder->orWhereBetween();

$builder->whereNotBetween();
$builder->orWhereNotBetween();

$builder->whereBetweenColumns();
$builder->orWhereBetweenColumns();

$builder->whereNotBetweenColumns();
$builder->orWhereNotBetweenColumns();
```

Так же есть метод, который выполняет where по значению из внешнего словаря.

```php
$builder->whereDict('dict', 'attribute', 'key', '=', 'value');
```

Преобразует запрос в такой:

```sql
SELECT dictGetString('dict', 'attribute', 'key') as `attribute` WHERE `attribute` = 'value' 
```

В качестве ключа можно передать массив и тогда он будет преобразован в кортеж `tuple`.
По умолчанию все строковые идентификаторы будут обернуты одинарными ковычками. Если вы хотите передать
название колонки в кортеж, то можно использовать такой пример:

```php
$builder->whereDict('dict', 'attribute', [new Identifier('column'), 'string value'], '=', 'value');
```

В таком случае запрос будет выглядеть так:

```sql
SELECT dictGetString('dict', 'attribute', tuple(`column`, 'string value')) as `attribute` WHERE `attribute` = 'value' 
```

### Group By

Ничего сложного.

```php
$builder->from('table')->select('column', raw('count()')->groupBy('attribute');
```

Сформирует запрос:

```sql
SELECT `column`, count() FROM `table` GROUP BY `attribute`
```

Для аргумента `$column` поведение идентично методу `select`.

### Order By

Сортировка результата.

```php
$builder->from('table')->orderBy('column', 'asc', 'fr');
```

*В примере выше, третий аргумент является опциональным*

```sql
SELECT *  FROM `table` ORDER BY `column` ASC COLLATE 'fr'
```

Так же для удобства использования есть методы-хелперы:

```php
$builder->orderByAsc('column');
$builder->orderByDesc('column');
```

Для аргумента `$column` поведение идентично методу `select`.
 
### Limit

Лимиты. В Clickhouse есть два типа лимитов: limit и limit n by.

Limit n by:
 
```php
$builder->from('table')->limitBy(1, 'column1', 'column2');
```

Преобразует запрос в такой вид:

```sql
SELECT * FROM `table` LIMIT 1 BY `column1`, `column2` 
```

Обычный лимит:

```php
$builder->from('table')->limit(10, 100);
```

Преобразует запрос в такой вид:

```sql
SELECT * FROM `table` LIMIT 100, 10 
```

### Union ALL

В метод `unionAll` можно передать замыкание `Closure` или инстанс `Builder`. В случае с замыканием, в качестве аргумента
будет передан инстанс `Builder` который можно настроить.

```php
$builder->from('table')->unionAll(function($query) {
    $query->select('column1')->from('table');
})->unionAll($builder->select('column2')->from('table'));
```

Преобразует запрос в такой вид:

```sql
SELECT * FROM `table` UNION ALL SELECT `column1` FROM `table` UNION ALL SELECT `column2` FROM `table` 
```

### Performing request and getting result.

После того как запрос сфомрирован, вам необходимо выполнить метод `get()` что бы отправить запрос на сервер и получить
результат.

Так же есть возможность выполнения асинхронных запросов. Работает практически как `unionAll`.

```php
$builder->from('table')->asyncWithQuery(function($query) {
    $query->from('table');
});

$builder->from('table')->asyncWithQuery($builder->from('table'));
$builder->from('table')->asyncWithQuery()->from('table');
```

Эти вызовы выполняют один и тот же сценарий. Два запроса будут выполнены асинхронно. Если вызвать метод
`get()` на билдер с переданными асинхронными запросами, то в качестве результата вернется массив где каждый элемент
будет результатом каждого отдельного запроса. Порядок результатов соотетствует порядку запросов.

### Integrations

#### Laravel

Есть возможность использовать текущий билдер в Laravel/Lumen.

**Laravel**

В `config/app.php` добавить:

```php
    'providers' => [
        ...
        \Tinderbox\ClickhouseBuilder\Integrations\Laravel\ClickhouseServiceProvider::class,
        ...
    ]
```

**Lumen**

В `bootstrap/app.php` добавить:

```php
$app->register(\Tinderbox\ClickhouseBuilder\Integrations\Laravel\ClickhouseServiceProvider::class);
```

Подключение настраивается через `config/database.php`.

**По умолчанию используется http транспорт, но вы можете указать любой другой через опцию `transport`.**

Пример с использованием одного сервера:

```php
'connections' => [
    'clickhouse' => [
        'driver' => 'clickhouse',
        'host' => '',
        'port' => '',
        'database' => '',
        'username' => '',
        'password' => '',
        'options' => [
            'timeout' => 10,
            'protocol' => 'https'
        ]
    ]
]
```

Пример с использованием кластера:

```php
'connections' => [
    'clickhouse' => [
        'driver' => 'clickhouse',
        'cluster' => [
            'server-1' => [
                'host' => '',
                'port' => '',
                'database' => '',
                'username' => '',
                'password' => '',
                'options' => [
                    'timeout' => 10,
                    'protocol' => 'https'
                ]
            ],
            'server-2' => [
                'host' => '',
                'port' => '',
                'database' => '',
                'username' => '',
                'password' => '',
                'options' => [
                    'timeout' => 10,
                    'protocol' => 'https'
                ]
            ]
        ]
    ]
]
```

Выбор сервера для выполнения запроса:

```php
DB::connection('clickhouse')->using('server-2')->select(...);
```
