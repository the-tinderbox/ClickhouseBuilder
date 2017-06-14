# Clickhouse Query Builder
[![Build Status](https://travis-ci.org/the-tinderbox/ClickhouseBuilder.svg?branch=master)](https://travis-ci.org/the-tinderbox/ClickhouseBuilder) [![Coverage Status](https://coveralls.io/repos/github/the-tinderbox/ClickhouseBuilder/badge.svg?branch=master)](https://coveralls.io/github/the-tinderbox/ClickhouseBuilder?branch=master)

# Requirements

`php 7.1`

# Install

Via composer

```bash
composer require the-tinderbox/clickhouse-builder
```

# Usage
For working query builder we must previously instantiate and pass in constructor `the-tinderbox/clickhouse-php-client`.

```php
$server = new Tinderbox\Clickhouse\Server('127.0.0.1', '8123', 'default', 'user', 'pass');
$client = new Tinderbox\Clickhouse\Client($server);
$builder = new Builder($client);
```
After that we can build and perform sql queries.

### Select columns

```php
$builder->select('column', 'column2', 'column3 as alias');
$builder->select(['column', 'column2', 'column3 as alias']);
$builder->select(['column', 'column2', 'column3' => 'alias']);
```

All this calls will be transformed into next sql:

```sql
SELECT `column`, `column2`, `column3` AS `alias` 
```

Also, as a column we can pass closure. In this case in closure will be passed instance of Column class, inside which we
can setup column how we want. This can be useful for difficult expressions with many functions, subqueries and etc.
 
```php
$builder->select(function ($column) {
    $column->name('time')->sumIf('time', '>', 10); 
});
```
Will be compiled in:

```sql
SELECT sumIf(`time`, time > 10) 
```

```php
$builder->select(function ($column) {
    $column->as('alias') //or ->name('alias') in this case
    ->query()
    ->select('column')
    ->from('table'); 
});
```
Will be compiled in:

```sql
SELECT  (SELECT `column` FROM `table) as `alias`
```

Same behavior can be also achieved by any of the following approaches:

```php
$1 = $builder->select(function ($column) {
         $column->as('alias') //or ->name('alias') in this case
            ->query(function ($query) {
                $query->select('column')->from('table');
            })
});
$2 = $builder->select(function ($column) {
         $column->as('alias') //or ->name('alias') in this case
            ->query($builder->select('column')->from('table')); 
});
```

**Notice! Functions on columns is not stable and under development.**

### From

```php
$builder->select('column')->from('table', 'alias');
```
Produce the following query:

```sql
SELECT `column` FROM `table` as `alias` 
```

Also can be passed closure or builder as argument for performing sub query.

```php
$builder->from(function ($from) {
    $from->query()->select('column')->from('table');
});
```

```sql
SELECT * FROM (SELECT `column` FROM `table`) 
```

or

```php
$builder->from(function ($from) {
    $from->query(function ($query) {
        $query->select('column')->from('table');
    });
});
```

or

```php
$builder->from(function ($from) {
    $from->query($builder->select('column')->from('table'));
});
```

or

```php
$builder->from($builder->select('column')->from('table'));
```

It is all variants of the same sql query which was listed above.

### Sample coefficient

```php
$builder->select('column')->from('table')->sample(0.1);
```

```sql
SELECT `column` FROM `table` SAMPLE 0.1 
```

I think there no need for additional words)

### Joins

```php
$builder->from('table')->join('another_table', 'any', 'left', ['column1', 'column2'], true);
```

```sql
SELECT * FROM `table` GLOBAL ANY LEFT JOIN `another_table` USING `column1`, `column2` 
```

For performing subquery as first argument you can pass closure or builder.

```php
$builder->from('table')->join(function ($query) {
    $query->select('column1', 'column2')->from('table2');
}, 'any', 'left', ['column1', 'column2']);

$builder->from('table')->join($builder->select('column1', 'column2')->from('table2'), 'any', 'left', ['column1', 'column2']);
```

```sql
SELECT * FROM `table` ANY LEFT JOIN (SELECT `column1`, `column2` FROM `table2`) USING `column1`, `column2` 
```

Also there are many helper functions with hardcoded arguments, like strict or type and they combinations.
 
```php
$builder->from('table')->anyLeftJoin('table', ['column']);
$builder->from('table')->allLeftJoin('table', ['column']);
$builder->from('table')->allInnerJoin('table', ['column']);
$builder->from('table')->anyInnerJoin('table', ['column']);

$buulder->from('table')->leftJoin('table', 'any', ['column']);
$buulder->from('table')->innerJoin('table', 'all', ['column']);
```

### Prewhere, where, having
All example will be about where, but same behavior also is for prewhere and having. 

```php
$builder->from('table')->where('column', '=', 'value');
$builder->from('table')->where('column', 'value');
```

```sql
SELECT * FROM `table` WHERE `column` = 'value' 
```

All string values will be wrapped with single quotes.
If operator is not provided `=` will be used.
If operator is not provided and value is an array, then `IN` will be used. 

```php
$builder->from('table')->where(function ($query) {
    $query->where('column1', 'value')->where('column2', 'value');
});
```

```sql
SELECT * FROM `table` WHERE (`column1` = 'value' AND `column2` = 'value') 
```

If in the first argument was passed closure, then all wheres statements from inside will be wrapped with parenthesis.
But if on that builder (inside closure) will be specified `from` then it will be transformed into subquery.

```php
$builder->from('table')->where(function ($query) {
    $query->select('column')->from('table');
})
```

```sql
SELECT * FROM `table` WHERE (SELECT `column` FROM `table`) 
```

Almost same is for value parameter, except wrapping into parenthesis.
Any closure or builder instance passed as value will be converted into subquery.

```php
$builder->from('table')->where('column', 'IN', function ($query) {
    $query->select('column')->from('table');
});
```

```sql
SELECT * FROM `table` WHERE `column` IN (SELECT `column` FROM `table`) 
```

Also you can pass internal representation of this statement and it will be used. I will no talk about this with deeper
explanation because its not preferable way to use this.

Like joins there are many helpers with hardcoded parameters.

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

Also there is method to make where by dictionary:

```php
$builder->whereDict('dict', 'attribute', 'key', '=', 'value');
```

```sql
SELECT dictGetString('dict', 'attribute', 'key') as `attribute` WHERE `attribute` = 'value' 
```

If you want to use complex key, you may pass an array as `$key`, then array will be converted to tuple. By default all strings will be escaped by single quotes, but you may pass an `Identifier` instance to pass for example column name:

```php
$builder->whereDict('dict', 'attribute', [new Identifier('column'), 'string value'], '=', 'value');
```

Will produce:

```sql
SELECT dictGetString('dict', 'attribute', tuple(`column`, 'string value')) as `attribute` WHERE `attribute` = 'value' 
```

### Group By

Works like select.

```php
$builder->from('table')->select('column', raw('count()')->groupBy('attribute');
```

Final query will be like:

```sql
SELECT `column`, count() FROM `table` GROUP BY `attribute`
```

### Order By

```php
$builder->from('table')->orderBy('column', 'asc', 'fr');
```

*In the example above, third argument is optional*

```sql
SELECT *  FROM `table` ORDER BY `column` ASC COLLATE 'fr'
```

Aliases:

```php
$builder->orderByAsc('column');
$builder->orderByDesc('column');
```

For column there are same behaviour like in select method.
 
### Limit

There are two types of limit. Limit and limit n by.
 
Limit n by:
 
```php
$builder->from('table')->limitBy(1, 'column1', 'column2');
```

Will produce:

```sql
SELECT * FROM `table` LIMIT 1 BY `column1`, `column2` 
```
 
Simple limit:
 
```php
$builder->from('table')->limit(10, 100);
```

Will produce:

```sql
SELECT * FROM `table` LIMIT 100, 10 
```

```php
$builder->from('table')->limit(10, 100);
```
`` SELECT * FROM `table` LIMIT 100, 10 ``

### Union ALL
In `unionAll` method can be passed closure or builder instance. In case of closure inside will be passed
builder instance.

```php
$builder->from('table')->unionAll(function($query) {
    $query->select('column1')->from('table');
})->unionAll($builder->select('column2')->from('table'));
```

```sql
SELECT * FROM `table` UNION ALL SELECT `column1` FROM `table` UNION ALL SELECT `column2` FROM `table` 
```

### Performing request and getting result.

After building request you must call `get()` method for sending request to the server.
Also there has opportunity to make asynchronous requests. Its works almost like `unitAll`.

```php
$builder->from('table')->asyncWithQuery(function($query) {
    $query->from('table');
});
$builder->from('table')->asyncWithQuery($builder->from('table'));
$builder->from('table')->asyncWithQuery()->from('table');
```
This callings will produce the same behavior. Two queries which will be executed asynchronous.
Now, if you call `get()` method, as result will be returned array, where numeric index correspond to the result of
request with this number.

### Integrations

#### Laravel

Есть возможность использовать текущий билдер в Laravel/Lumen.

**Laravel**

In `config/app.php` add:

```php
    'providers' => [
        ...
        \Tinderbox\ClickhouseBuilder\Integrations\Laravel\ClickhouseServiceProvider::class,
        ...
    ]
```

**Lumen**

In `bootstrap/app.php` add:

```php
$app->register(\Tinderbox\ClickhouseBuilder\Integrations\Laravel\ClickhouseServiceProvider::class);
```

Connection configures via `config/database.php`.

Example with alone server:

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

Example with cluster:

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

Choose server from cluster to perform request:

```php
DB::connection('clickhouse')->using('server-2')->select(...);
```
