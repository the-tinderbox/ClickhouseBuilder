<?php

namespace Tinderbox\ClickhouseBuilder;

use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery as m;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Client;
use Tinderbox\Clickhouse\Common\File;
use Tinderbox\Clickhouse\Common\FileFromString;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\Clickhouse\Query;
use Tinderbox\Clickhouse\Server;
use Tinderbox\Clickhouse\ServerProvider;
use Tinderbox\ClickhouseBuilder\Query\Builder;
use Tinderbox\ClickhouseBuilder\Query\Column;
use Tinderbox\ClickhouseBuilder\Query\Enums\Operator;
use Tinderbox\ClickhouseBuilder\Query\From;
use Tinderbox\ClickhouseBuilder\Query\JoinClause;
use Tinderbox\ClickhouseBuilder\Query\TwoElementsLogicExpression;

class BuilderTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function getBuilder(): Builder
    {
        return new Builder(m::mock(Client::class));
    }

    public function test_select()
    {
        $builder = $this->getBuilder();

        $builder->select('column'); //1

        $this->assertEquals('SELECT `column`', $builder->toSql());

        $builder->select('column as alias'); //2

        $this->assertEquals('SELECT `column` AS `alias`', $builder->toSql());

        $builder->select('firstColumn', 'secondColumn'); //3

        $this->assertEquals('SELECT `firstColumn`, `secondColumn`', $builder->toSql());

        $builder->select('firstColumn as firstAlias', 'secondColumn as secondAlias'); //4

        $this->assertEquals('SELECT `firstColumn` AS `firstAlias`, `secondColumn` AS `secondAlias`', $builder->toSql());

        $builder->select([function ($column) {
            $column->name(function ($col) {
                $col->name('a')->plus('b');
            })->multiple('c');
        }]);
        $this->assertEquals('SELECT (`a` + \'b\') * \'c\'', $builder->toSql());

        $builder->select(['column']); //1

        $this->assertEquals('SELECT `column`', $builder->toSql());

        $builder->select(['column' => 'alias']); //2

        $this->assertEquals('SELECT `column` AS `alias`', $builder->toSql());

        $builder->select(['firstColumn', 'secondColumn']); //3

        $this->assertEquals('SELECT `firstColumn`, `secondColumn`', $builder->toSql());

        $builder->select(['firstColumn' => 'firstAlias', 'secondColumn' => 'secondAlias']); //4

        $this->assertEquals('SELECT `firstColumn` AS `firstAlias`, `secondColumn` AS `secondAlias`', $builder->toSql());

        $builder->select('firstColumn')->addSelect('secondColumn');

        $this->assertEquals('SELECT `firstColumn`, `secondColumn`', $builder->toSql());

        $builder->select('firstColumn as firstAlias')->addSelect('secondColumn as secondAlias');

        $this->assertEquals('SELECT `firstColumn` AS `firstAlias`, `secondColumn` AS `secondAlias`', $builder->toSql());

        $builder = $this->getBuilder()->table('table');
        $builder->addSelect();

        $this->assertEquals('SELECT * FROM `table`', $builder->toSql());

        $builder = $this->getBuilder()->select(['a' => $builder]);
        $this->assertEquals('SELECT (SELECT * FROM `table`) AS `a`', $builder->toSql());

        $builder = $this->getBuilder()->from($this->getBuilder()->table('table'));
        $this->assertEquals('SELECT * FROM (SELECT * FROM `table`)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->sample(0.3);
        $this->assertEquals('SELECT * FROM `table` SAMPLE 0.3', $builder->toSql());
    }

    public function test_select_column_closure()
    {
        $builder = $this->getBuilder();

        $builder->select(['column' => function ($column) {
            $this->assertInstanceOf(Column::class, $column);
            $column->name('myColumn')->as('myAlias');
        }]);

        $this->assertEquals('SELECT `myColumn` AS `myAlias`', $builder->toSql());

        $builder->select(['column' => function ($column) {
            $this->assertInstanceOf(Column::class, $column);
        }]);

        $this->assertEquals('SELECT `column`', $builder->toSql());

        $builder->select(function ($column) {
            $this->assertInstanceOf(Column::class, $column);
        });

        $this->assertEquals('SELECT', $builder->toSql());

        $builder->select(function ($column) {
            $this->assertInstanceOf(Column::class, $column);
            $column->name('myColumn')->as('myAlias');
        });

        $this->assertEquals('SELECT `myColumn` AS `myAlias`', $builder->toSql());

        $builder->select([function ($column) {
            $this->assertInstanceOf(Column::class, $column);
            $column->name('myColumn')->as('myAlias');
        }]);

        $this->assertEquals('SELECT `myColumn` AS `myAlias`', $builder->toSql());

        $builder->select('firstColumn')->addSelect(function ($column) {
            $this->assertInstanceOf(Column::class, $column);
            $column->name('secondColumn')->as('secondAlias');
        });

        $this->assertEquals('SELECT `firstColumn`, `secondColumn` AS `secondAlias`', $builder->toSql());

        $builder->select('firstColumn')->addSelect([function ($column) {
            $this->assertInstanceOf(Column::class, $column);
            $column->name('secondColumn')->as('secondAlias');
        }]);

        $this->assertEquals('SELECT `firstColumn`, `secondColumn` AS `secondAlias`', $builder->toSql());

        $builder->select('firstColumn')->addSelect(['column' => function ($column) {
            $this->assertInstanceOf(Column::class, $column);
            $column->name('secondColumn')->as('secondAlias');
        }]);

        $this->assertEquals('SELECT `firstColumn`, `secondColumn` AS `secondAlias`', $builder->toSql());

        $builder->select('firstColumn')->addSelect(['column' => function ($column) {
            $this->assertInstanceOf(Column::class, $column);
            $column->as('secondAlias');
        }]);

        $this->assertEquals('SELECT `firstColumn`, `column` AS `secondAlias`', $builder->toSql());
    }

    public function test_select_sub_query_in_closure()
    {
        $builder = $this->getBuilder();

        $builder->select(function ($column) {
            $column->query(function ($builder) {
                $this->assertInstanceOf(Builder::class, $builder);
                $builder->select('column')->from('table');
            })->as('value');
        });

        $this->assertEquals('SELECT (SELECT `column` FROM `table`) AS `value`', $builder->toSql());

        $twoLevelSubRequestInSelect = 'SELECT (SELECT (SELECT `columnOnSecondLevel` FROM `secondLevelTable`) AS `secondLevel` FROM `firstLevelTable`) AS `firstLevel` FROM `mainLevelTable`';

        // 2 level select sub request callback hell. Just continue reading)
        $builder->select(function ($column) {
            $column->as('firstLevel')->query(function ($builder) {
                $this->assertInstanceOf(Builder::class, $builder);

                $builder->select([function ($column) {
                    $column->as('secondLevel')->query(function ($builder) {
                        $builder->select('columnOnSecondLevel')->from('secondLevelTable');
                    });
                }])->from('firstLevelTable');
            });
        })->from('mainLevelTable');

        $this->assertEquals($twoLevelSubRequestInSelect, $builder->toSql());

        //same 2 level sub select request. Flatten and more readable version of the same query.
        $builder->select(function (Column $column) {
            $column
                ->as('firstLevel')
                ->query()
                ->select(function (Column $column) {
                    $column
                        ->as('secondLevel')
                        ->query()
                        ->select('columnOnSecondLevel')->from('secondLevelTable');
                })
                ->from('firstLevelTable');
        })->from('mainLevelTable');

        $this->assertEquals($twoLevelSubRequestInSelect, $builder->toSql());
    }

    public function test_from_simple()
    {
        $builder = $this->getBuilder();

        $builder->from('table');
        $this->assertEquals('SELECT * FROM `table`', $builder->toSql());

        $builder->select('value')->from('table');
        $this->assertEquals('SELECT `value` FROM `table`', $builder->toSql());
        $builder->select(); //just to flush column

        $builder->from('table as t');
        $this->assertEquals('SELECT * FROM `table` AS `t`', $builder->toSql());

        $builder->from('table2')->alias('2');
        $this->assertEquals('SELECT * FROM `table2` AS `2`', $builder->toSql());

        $builder->from('table3')->as('3');
        $this->assertEquals('SELECT * FROM `table3` AS `3`', $builder->toSql());

        $builder->from('table4')->as('4')->final();
        $this->assertEquals('SELECT * FROM `table4` AS `4` FINAL', $builder->toSql());

        $builder->from('table', 'alias');
        $this->assertEquals('SELECT * FROM `table` AS `alias`', $builder->toSql());

        $builder->from('table', 'alias', true);
        $this->assertEquals('SELECT * FROM `table` AS `alias` FINAL', $builder->toSql());
    }

    public function test_from_with_closure()
    {
        $builder = $this->getBuilder();

        $builder->from(function ($from) {
            $this->assertInstanceOf(From::class, $from);
        });

        $builder->from(function ($from) {
            $from->table('table');
        });
        $this->assertEquals('SELECT * FROM `table`', $builder->toSql());

        $builder->from(function ($from) {
            $from->table('table')->as('t');
        });
        $this->assertEquals('SELECT * FROM `table` AS `t`', $builder->toSql());

        $builder->from(function ($from) {
            $from->table('table')->as('t')->final();
        });
        $this->assertEquals('SELECT * FROM `table` AS `t` FINAL', $builder->toSql());

        $builder->from(function ($from) {
            $from->remote('expression', 'database', 'table');
        });
        $this->assertEquals('SELECT * FROM remote(\'expression\', database, table)', $builder->toSql());

        $builder->from(function ($from) {
            $from->remote('expression', 'database', 'table')->as('alias');
        });
        $this->assertEquals('SELECT * FROM remote(\'expression\', database, table) AS `alias`', $builder->toSql());

        $builder->from(function ($from) {
            $from->merge('database', 'regexp');
        });
        $this->assertEquals('SELECT * FROM merge(database, \'regexp\')', $builder->toSql());
    }

    public function test_from_sub_query()
    {
        $builder = $this->getBuilder();

        $builder->from(function ($from) {
            $from->query()->select('column')->from('table');
        });
        $this->assertEquals('SELECT * FROM (SELECT `column` FROM `table`)', $builder->toSql());

        $builder->from(function ($from) {
            $from->query()->select('column')->from('table')->as('t');
        })->as('subQueryFrom');
        $this->assertEquals('SELECT * FROM (SELECT `column` FROM `table` AS `t`) AS `subQueryFrom`', $builder->toSql());

        $builder->from(function ($from) {
            $from->as('subQueryFrom')->query()->select('column')->from('table');
        });
        $this->assertEquals('SELECT * FROM (SELECT `column` FROM `table`) AS `subQueryFrom`', $builder->toSql());

        $builder->from(function ($from) {
            $from->query(function ($query) {
                $query->select('column')->from('table');
            })->as('subQuery');
        });
        $this->assertEquals('SELECT * FROM (SELECT `column` FROM `table`) AS `subQuery`', $builder->toSql());

        $builder->from(function ($from) {
            $from->query()->from(function ($from) {
                $from->query()->select('column')->from('table');
            });
        });
        $this->assertEquals('SELECT * FROM (SELECT * FROM (SELECT `column` FROM `table`))', $builder->toSql());
    }

    public function test_join_simple()
    {
        $builder = $this->getBuilder();
        $builder->from('table')->join('table2', 'any', 'left', ['column']);
        $this->assertEquals('SELECT * FROM `table` ANY LEFT JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->join('table2', 'any', 'inner', ['column', 'column2']);
        $this->assertEquals('SELECT * FROM `table` ANY INNER JOIN `table2` USING `column`, `column2`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->join('table2', 'all', 'left', ['column']);
        $this->assertEquals('SELECT * FROM `table` ALL LEFT JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->join('table2', 'any', 'left', ['column']);
        $this->assertEquals('SELECT * FROM `table` ANY LEFT JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->join('table2', 'any', 'left', ['column'], 'global');
        $this->assertEquals('SELECT * FROM `table` GLOBAL ANY LEFT JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->join('table2', 'any', 'left', ['column'], 'global', 'table3');
        $this->assertEquals('SELECT * FROM `table` GLOBAL ANY LEFT JOIN `table2` AS `table3` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->join('table2 as table3', 'any', 'left', ['column'], 'global', 'table4');
        $this->assertEquals('SELECT * FROM `table` GLOBAL ANY LEFT JOIN `table2` AS `table3` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->join('table2 as table3 as table', 'any', 'left', ['column'], 'global', 'table4');
        $this->assertEquals('SELECT * FROM `table` GLOBAL ANY LEFT JOIN `table2` AS `table3` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->leftJoin('table2', 'all', ['column']);
        $this->assertEquals('SELECT * FROM `table` ALL LEFT JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->leftJoin('table2', 'any', ['column']);
        $this->assertEquals('SELECT * FROM `table` ANY LEFT JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->innerJoin('table2', 'all', ['column']);
        $this->assertEquals('SELECT * FROM `table` ALL INNER JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->innerJoin('table2', 'any', ['column']);
        $this->assertEquals('SELECT * FROM `table` ANY INNER JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->anyLeftJoin('table2', ['column']);
        $this->assertEquals('SELECT * FROM `table` ANY LEFT JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->anyInnerJoin('table2', ['column']);
        $this->assertEquals('SELECT * FROM `table` ANY INNER JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->allLeftJoin('table2', ['column']);
        $this->assertEquals('SELECT * FROM `table` ALL LEFT JOIN `table2` USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->allInnerJoin('table2', ['column']);
        $this->assertEquals('SELECT * FROM `table` ALL INNER JOIN `table2` USING `column`', $builder->toSql());
    }

    public function test_join_with_closure()
    {
        $builder = $this->getBuilder();
        $builder->from('table')->anyLeftJoin(function ($join) {
            $this->assertInstanceOf(JoinClause::class, $join);
        });

        $builder = $this->getBuilder();
        $builder->from('table')->allLeftJoin(function ($join) {
            $join->table('table2')->using(['column'])->addUsing('column2');
        });
        $this->assertEquals('SELECT * FROM `table` ALL LEFT JOIN `table2` USING `column`, `column2`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->anyInnerJoin(function ($join) {
            $join->table('table2')->using('column');
        }, ['column2']);
        $this->assertEquals('SELECT * FROM `table` ANY INNER JOIN `table2` USING `column`, `column2`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->allInnerJoin(function ($join) {
            $join->query()->select('column')->from('table');
            $join->addUsing('column', 'column2');
        });
        $this->assertEquals('SELECT * FROM `table` ALL INNER JOIN (SELECT `column` FROM `table`) USING `column`, `column2`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->allInnerJoin(function ($join) {
            $join->query()->select('column')->from(function ($from) {
                $from->query()->from('table2');
            });
        }, ['column', 'column2']);
        $this->assertEquals('SELECT * FROM `table` ALL INNER JOIN (SELECT `column` FROM (SELECT * FROM `table2`)) USING `column`, `column2`', $builder->toSql());

        $builder = $this->getBuilder()->anyLeftJoin($this->getBuilder()->from('table'), ['column']);
        $this->assertEquals('SELECT * ANY LEFT JOIN (SELECT * FROM `table`) USING `column`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table')->join(function ($join) {
            $join->query()->select('column1', 'column2')->from('table2');
        }, 'any', 'left', ['column1', 'column2']);
        $this->assertEquals('SELECT * FROM `table` ANY LEFT JOIN (SELECT `column1`, `column2` FROM `table2`) USING `column1`, `column2`', $builder->toSql());
    }

    public function test_preWheres()
    {
        $builder = $this->getBuilder();

        $builder->select('column')->from('table')->preWhere('column', '=', 10);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `column` = 10', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->preWhere(function ($query) {
            $query->preWhere('column1', '=', 1)->preWhere('column2', '=', 2);
        })->preWhere('column3', '=', 2);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE (`column1` = 1 AND `column2` = 2) AND `column3` = 2', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->preWhere(function ($query) {
            $query->select('column2')->from('table2'); //if table provided, query will be converted in sub query
        });
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE (SELECT `column2` FROM `table2`)', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->preWhere(function ($query) {
            $query->select(10);
        }, '=', 10);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE (SELECT 10) = 10', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->preWhere('column', [1, 2, 'string']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `column` IN (1, 2, \'string\')', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->preWhere('column', 1)->orPreWhere('column2', 3);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `column` = 1 OR `column2` = 3', $builder->toSql());

        $builder = $this->getBuilder()->select('column')->from('table')->preWhereRaw('column = 2');
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE column = 2', $builder->toSql());

        $builder = $this->getBuilder()->select('column')->from('table')->preWhereRaw('column = 2')->orPreWhereRaw('column2 = 3');
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE column = 2 OR column2 = 3', $builder->toSql());
    }

    public function test_preWheres_in()
    {
        $builder = $this->getBuilder();

        $builder = $builder->newQuery()->select('column')->from('table')->preWhereIn('column', ['string', 1, 2, 3]);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `column` IN (\'string\', 1, 2, 3)', $builder->toSql());

        $builder = $builder->newQuery();
        $builder->addFile(new TempTable('_numbers', '', ['number' => 'UInt64']))->select('column')->from('table')->preWhereIn('column', '_numbers');

        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `column` IN `_numbers`', $builder->toSql());

        $builder = $builder->newQuery()->from('table')->preWhereIn(function ($query) {
            $query->from('table2');
        }, ['string', 1, 2, 3]);

        $this->assertEquals('SELECT * FROM `table` PREWHERE (SELECT * FROM `table2`) IN (\'string\', 1, 2, 3)', $builder->toSql());

        $builder = $builder->newQuery()->from('table')->preWhereIn(function ($query) {
            $query->preWhere('column', 1);
        }, ['string', 1, 2, 3]);

        $this->assertEquals('SELECT * FROM `table` PREWHERE (`column` = 1) IN (\'string\', 1, 2, 3)', $builder->toSql());

        $builder = $builder->newQuery()->from('table')->preWhereIn('column', function ($query) {
            $query->from('another_table')->preWhere('column', 1);
        });

        $this->assertEquals('SELECT * FROM `table` PREWHERE `column` IN (SELECT * FROM `another_table` PREWHERE `column` = 1)', $builder->toSql());

        $builder = $builder->newQuery()->from('table')->preWhereIn('column', [1, 2, 3, 'string'])->orPreWhereIn('column2', [1, 2, 4, 'string2']);
        $this->assertEquals('SELECT * FROM `table` PREWHERE `column` IN (1, 2, 3, \'string\') OR `column2` IN (1, 2, 4, \'string2\')', $builder->toSql());

        $builder = $builder->newQuery()->from('table')->preWhereIn('column', [1, 2, 3, 'string'])->orPreWhereNotIn('column2', [1, 2, 4, 'string2']);
        $this->assertEquals('SELECT * FROM `table` PREWHERE `column` IN (1, 2, 3, \'string\') OR `column2` NOT IN (1, 2, 4, \'string2\')', $builder->toSql());
    }

    public function test_preWhere_between()
    {
        $builder = $this->getBuilder();

        $builder = $builder->newQuery()->select('column')->from('table')->preWhereBetween('column', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `column` BETWEEN \'first\' AND \'second\'', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->preWhereBetween(function ($query) {
            $query->from('table');
        }, ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE (SELECT * FROM `table`) BETWEEN \'first\' AND \'second\'', $builder->toSql());

        $builder = $builder->newQuery()
            ->select('column')
            ->from('table')
            ->preWhereBetween('column', ['first', 'second'])->orPreWhereBetween('column2', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `column` BETWEEN \'first\' AND \'second\' OR `column2` BETWEEN \'first\' AND \'second\'', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->preWhereBetweenColumns('column', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `column` BETWEEN `first` AND `second`', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->prewhere('col', '=', 'a')->orPreWhereBetweenColumns('column', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `col` = \'a\' OR `column` BETWEEN `first` AND `second`', $builder->toSql());
    }

    public function test_preWhere_not_between()
    {
        $builder = $this->getBuilder();

        $builder = $builder->newQuery()->select('column')->from('table')->preWhereNotBetween('column', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE NOT ( `column` BETWEEN \'first\' AND \'second\' )', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->preWhereNotBetween(function ($query) {
            $query->from('table');
        }, ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE NOT ( (SELECT * FROM `table`) BETWEEN \'first\' AND \'second\' )', $builder->toSql());

        $builder = $builder->newQuery()
            ->select('column')
            ->from('table')
            ->preWhereBetween('column', ['first', 'second'])->orPreWhereNotBetween('column2', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `column` BETWEEN \'first\' AND \'second\' OR NOT ( `column2` BETWEEN \'first\' AND \'second\' )', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->preWhereNotBetweenColumns('column', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE NOT ( `column` BETWEEN `first` AND `second` )', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->prewhere('col', '=', 'a')->orPreWhereNotBetweenColumns('column', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` PREWHERE `col` = \'a\' OR NOT ( `column` BETWEEN `first` AND `second` )', $builder->toSql());
    }

    public function test_wheres_basic()
    {
        $builder = $this->getBuilder()->select('column')->from('table')->where('column', '=', 1);
        $this->assertEquals('SELECT `column` FROM `table` WHERE `column` = 1', $builder->toSql());

        $builder = $this->getBuilder()->select('column')->from('table')->where('column', '=', 0);
        $this->assertEquals('SELECT `column` FROM `table` WHERE `column` = 0', $builder->toSql());

        $builder = $this->getBuilder()->select('column')->from('table')->where('column', 1);
        $this->assertEquals('SELECT `column` FROM `table` WHERE `column` = 1', $builder->toSql());

        $builder = $this->getBuilder()->select('column')->from('table')->where('column', [1, 'value']);
        $this->assertEquals('SELECT `column` FROM `table` WHERE `column` IN (1, \'value\')', $builder->toSql());

        $builder = $this->getBuilder()->select('column')->from('table')->where(function ($query) {
            $query->where('column1', 2)->where('column2', '>', 3);
        });
        $this->assertEquals('SELECT `column` FROM `table` WHERE (`column1` = 2 AND `column2` > 3)', $builder->toSql());

        $builder = $this->getBuilder()->select('column')->from('table')->whereRaw('column = 2');
        $this->assertEquals('SELECT `column` FROM `table` WHERE column = 2', $builder->toSql());

        $builder = $this->getBuilder()->select('column')->from('table')->whereRaw('column = 2')->orWhereRaw('column2 = 1');
        $this->assertEquals('SELECT `column` FROM `table` WHERE column = 2 OR column2 = 1', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->where('column', function ($query) {
            $query->select('column')->from('table');
        });
        $this->assertEquals('SELECT * FROM `table` WHERE `column` = (SELECT `column` FROM `table`)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->where('column', 1)->orWhere('column2', 3);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` = 1 OR `column2` = 3', $builder->toSql());

        $builder = $this->getBuilder();
        $element = new TwoElementsLogicExpression($builder);
        $element->firstElement('a');
        $element->operator('=');
        $element->secondElement('b');
        $element->concatOperator(Operator:: OR);

        $builder->where($element);
        $this->assertEquals('SELECT * WHERE \'a\' = \'b\'', $builder->toSql());

        $builder = $this->getBuilder();
        $element = new TwoElementsLogicExpression($builder);
        $element->firstElement('a');
        $element->operator('=');
        $element->secondElement('b');
        $element->concatOperator(Operator:: OR);

        $element2 = new TwoElementsLogicExpression($builder);
        $element2->firstElement('c');
        $element2->operator('=');
        $element2->secondElement('d');
        $element2->concatOperator(Operator:: OR);

        $builder->where($element, '=', $element2);
        $this->assertEquals('SELECT * WHERE \'a\' = \'b\' = \'c\' = \'d\'', $builder->toSql());

        $builder = $this->getBuilder()->where($this->getBuilder()->select('column')->from('table'), '=', 'a');
        $this->assertEquals('SELECT * WHERE (SELECT `column` FROM `table`) = \'a\'', $builder->toSql());
    }

    public function test_where_ins()
    {
        $builder = $this->getBuilder()->from('table')->whereIn('column', [1, 'string']);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` IN (1, \'string\')', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereIn('column', function ($query) {
            $query->from('table')->preWhere('column', 2);
        });
        $this->assertEquals('SELECT * FROM `table` WHERE `column` IN (SELECT * FROM `table` PREWHERE `column` = 2)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereIn('column', [1, 2, 3])->orWhereIn('column2', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` IN (1, 2, 3) OR `column2` IN (1, 2, 3)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereNotIn('column', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` NOT IN (1, 2, 3)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->where('column', 'value')->orWhereNotIn('column', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` = \'value\' OR `column` NOT IN (1, 2, 3)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereGlobalIn('column', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` GLOBAL IN (1, 2, 3)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereGlobalIn('column', [1, 2, 3])->orWhereGlobalIn('column2', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` GLOBAL IN (1, 2, 3) OR `column2` GLOBAL IN (1, 2, 3)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereGlobalNotIn('column', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` GLOBAL NOT IN (1, 2, 3)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereGlobalNotIn('column', [1, 2, 3])->orWhereGlobalNotIn('column2', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` GLOBAL NOT IN (1, 2, 3) OR `column2` GLOBAL NOT IN (1, 2, 3)', $builder->toSql());

        $builder = $builder->newQuery();
        $builder->addFile(new TempTable('_numbers', '', ['number' => 'UInt64']))->select('column')->from('table')->whereIn('column', '_numbers');

        $this->assertEquals('SELECT `column` FROM `table` WHERE `column` IN `_numbers`', $builder->toSql());

        $builder = $builder->newQuery();
        $builder->addFile(new TempTable('_numbers', '', ['number' => 'UInt64']))->select('column')->from('table')->whereGlobalIn('column', '_numbers');

        $this->assertEquals('SELECT `column` FROM `table` WHERE `column` GLOBAL IN `_numbers`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereIn('column', []);
        $this->assertEquals('SELECT * FROM `table` WHERE 0 = 1', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereNotIn('column', []);
        $this->assertEquals('SELECT * FROM `table`', $builder->toSql());
    }

    public function test_where_between()
    {
        $builder = $this->getBuilder()->from('table')->whereBetween('column', [1, 'string']);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` BETWEEN 1 AND \'string\'', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereBetween(function ($query) {
            $query->from('table');
        }, [1, 2]);
        $this->assertEquals('SELECT * FROM `table` WHERE (SELECT * FROM `table`) BETWEEN 1 AND 2', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereNotBetween('column', [1, 'string']);
        $this->assertEquals('SELECT * FROM `table` WHERE NOT ( `column` BETWEEN 1 AND \'string\' )', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->where('col', '=', 'a')->orWhereNotBetween('column', [1, 'string']);
        $this->assertEquals('SELECT * FROM `table` WHERE `col` = \'a\' OR NOT ( `column` BETWEEN 1 AND \'string\' )', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->where('column', 1)->orWhereBetween('column2', [1, 2]);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` = 1 OR `column2` BETWEEN 1 AND 2', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereBetweenColumns('column', ['column1', 'column2']);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` BETWEEN `column1` AND `column2`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->where('column', 1)->orWhereBetweenColumns('column', ['column1', 'column2']);
        $this->assertEquals('SELECT * FROM `table` WHERE `column` = 1 OR `column` BETWEEN `column1` AND `column2`', $builder->toSql());
    }

    public function test_where_dict()
    {
        $builder = $this->getBuilder()->from('table')->whereDict('dictionary', 'attribute', 'key', 'value');
        $this->assertEquals("SELECT dictGetString('dictionary', 'attribute', 'key') as `attribute` FROM `table` WHERE `attribute` = 'value'", $builder->toSql());

        $builder = $this->getBuilder()->from('table')->whereDict('dictionary', 'attribute', ['key', 1], '!=', 'value');
        $this->assertEquals("SELECT dictGetString('dictionary', 'attribute', tuple('key', 1)) as `attribute` FROM `table` WHERE `attribute` != 'value'", $builder->toSql());

        $builder = $this->getBuilder()
            ->from('table')
            ->whereDict('dictionary', 'attribute', ['key', 1], '!=', 'value')
            ->orWhereDict('dictionary2', 'attribute2', 5, 5);

        $this->assertEquals("SELECT dictGetString('dictionary', 'attribute', tuple('key', 1)) as `attribute`, dictGetString('dictionary2', 'attribute2', 5) as `attribute2` FROM `table` WHERE `attribute` != 'value' OR `attribute2` = 5", $builder->toSql());
    }

    public function test_count()
    {
        $builder = $this->getBuilder()->from('table')->select('column1', 'column2', 'column3')->orderBy('column1')->limit(10)->getCountQuery();
        $this->assertEquals('SELECT count() as `count` FROM `table`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->select('column1', 'column2', 'column3')->groupBy('column2')->orderBy('column1')->limit(10)->getCountQuery();
        $this->assertEquals('SELECT count() as `count` FROM `table` GROUP BY `column2` ORDER BY `column1` ASC', $builder->toSql());
    }

    public function test_group_by()
    {
        $builder = $this->getBuilder()->from('table')->groupBy('column');
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column', 'column2');
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column`, `column2`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy(['column', 'column2' => 'a']);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column`, `column2`', $builder->toSql());
    }

    public function test_add_group_by()
    {
        $builder = $this->getBuilder()->from('table')->groupBy('column')->addGroupBy('column2', 'column3');
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column`, `column2`, `column3`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->addGroupBy(['column', 'column2' => 'a']);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column`, `column2`', $builder->toSql());
    }

    public function test_havings()
    {
        $builder = $this->getBuilder()->from('table')->groupBy('column')->having('column', 1);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING `column` = 1', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->havingBetween('column', [1, 2]);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING `column` BETWEEN 1 AND 2', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->havingBetween('column', [1, 2])->orHavingBetween('column2', [3, 4]);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING `column` BETWEEN 1 AND 2 OR `column2` BETWEEN 3 AND 4', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->havingNotBetween('column', [1, 2]);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING NOT ( `column` BETWEEN 1 AND 2 )', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->havingBetween('column', [1, 2])->orHavingNotBetween('column2', [3, 4]);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING `column` BETWEEN 1 AND 2 OR NOT ( `column2` BETWEEN 3 AND 4 )', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->havingBetweenColumns('column', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` HAVING `column` BETWEEN `first` AND `second`', $builder->toSql());

        $builder = $builder->newQuery()->select('column')->from('table')->having('col', '=', 'a')->orHavingBetweenColumns('column', ['first', 'second']);
        $this->assertEquals('SELECT `column` FROM `table` HAVING `col` = \'a\' OR `column` BETWEEN `first` AND `second`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->having(function ($query) {
            $query->having('column1', 1)->orHaving('column2', 2);
        })->having('column3', 3);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING (`column1` = 1 OR `column2` = 2) AND `column3` = 3', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->having('column', 1)->orHaving('column', 3);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING `column` = 1 OR `column` = 3', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->havingIn('column', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING `column` IN (1, 2, 3)', $builder->toSql());

        $builder = $builder->newQuery();
        $builder->addFile(new TempTable('_numbers', '', ['number' => 'UInt64']))->select('column')->from('table')->havingIn('column', '_numbers');

        $this->assertEquals('SELECT `column` FROM `table` HAVING `column` IN `_numbers`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->havingNotIn('column', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING `column` NOT IN (1, 2, 3)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->having('a', '=', 'b')->orHavingNotIn('column', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING `a` = \'b\' OR `column` NOT IN (1, 2, 3)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->groupBy('column')->having('a', '=', 'b')->orHavingIn('column', [1, 2, 3]);
        $this->assertEquals('SELECT * FROM `table` GROUP BY `column` HAVING `a` = \'b\' OR `column` IN (1, 2, 3)', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->havingRaw('a = b');
        $this->assertEquals('SELECT * FROM `table` HAVING a = b', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->having('a', '=', 'b')->orHavingRaw('a = b');
        $this->assertEquals('SELECT * FROM `table` HAVING `a` = \'b\' OR a = b', $builder->toSql());
    }

    public function test_order()
    {
        $builder = $this->getBuilder()->from('table')->orderByAsc('column');
        $this->assertEquals('SELECT * FROM `table` ORDER BY `column` ASC', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->orderByDesc('column');
        $this->assertEquals('SELECT * FROM `table` ORDER BY `column` DESC', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->orderByDesc('column', 'ru');
        $this->assertEquals("SELECT * FROM `table` ORDER BY `column` DESC COLLATE 'ru'", $builder->toSql());

        $builder = $this->getBuilder()->from('table')->orderByDesc('column', 'ru')->orderByAsc('column2');
        $this->assertEquals("SELECT * FROM `table` ORDER BY `column` DESC COLLATE 'ru', `column2` ASC", $builder->toSql());

        $builder = $this->getBuilder()->from('table')->orderByRaw('column ASC');
        $this->assertEquals('SELECT * FROM `table` ORDER BY column ASC', $builder->toSql());
    }

    public function test_limit_by()
    {
        $builder = $this->getBuilder()->from('table')->limitBy(1, 'column1', 'column2');
        $this->assertEquals('SELECT * FROM `table` LIMIT 1 BY `column1`, `column2`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->limitBy(1, 'column1');
        $this->assertEquals('SELECT * FROM `table` LIMIT 1 BY `column1`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->takeBy(1, 'column1', 'column2');
        $this->assertEquals('SELECT * FROM `table` LIMIT 1 BY `column1`, `column2`', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->takeBy(1, 'column1');
        $this->assertEquals('SELECT * FROM `table` LIMIT 1 BY `column1`', $builder->toSql());
    }

    public function test_limit()
    {
        $builder = $this->getBuilder()->from('table')->limit(1);
        $this->assertEquals('SELECT * FROM `table` LIMIT 1', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->limit(1, 2);
        $this->assertEquals('SELECT * FROM `table` LIMIT 2, 1', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->take(1);
        $this->assertEquals('SELECT * FROM `table` LIMIT 1', $builder->toSql());

        $builder = $this->getBuilder()->from('table')->take(1, 2);
        $this->assertEquals('SELECT * FROM `table` LIMIT 2, 1', $builder->toSql());
    }

    public function test_unionAll()
    {
        $builder = $this->getBuilder()->from('table')->unionAll(function ($query) {
            $query->select('column')->from('table2')->where('column3', 5);
        })->unionAll($this->getBuilder()->select('column5', 'column6')->from('table3'));

        $this->assertEquals('SELECT * FROM `table` UNION ALL SELECT `column` FROM `table2` WHERE `column3` = 5 UNION ALL SELECT `column5`, `column6` FROM `table3`', $builder->toSql());

        $this->expectException(\InvalidArgumentException::class);

        $builder->unionAll('a');
    }

    public function test_readOne_and_read()
    {
        $server = new Server('127.0.0.1');
        $client = new Client((new ServerProvider())->addServer($server));

        $client->write([
            ['query' => 'drop table if exists default.builder_test'],
            ['query' => 'create table if not exists default.builder_test (number UInt64) engine = Memory'],
        ], 1);

        $builder = new Builder($client);
        $result = $builder
            ->table('system.tables')
            ->where('database', '=', 'default')
            ->where('name', '=', 'builder_test')->get();

        $this->assertEquals(1, count($result->rows), 'Correctly returns result of query');

        $client->write([
            ['query' => 'drop table if exists default.builder_test'],
            ['query' => 'drop table if exists default.builder_test2'],
            ['query' => 'create table if not exists default.builder_test (number UInt64) engine = Memory'],
            ['query' => 'create table if not exists default.builder_test2 (number UInt64) engine = Memory'],
        ], 1);

        $builder = new Builder($client);

        $result = $builder
            ->table('system.tables')
            ->where('database', '=', 'default')
            ->where('name', '=', 'builder_test')
            ->asyncWithQuery(function ($builder) {
                $builder
                    ->table('system.tables')
                    ->where('database', '=', 'default')
                    ->where('name', '=', 'builder_test2');
            })->get();

        $this->assertTrue(count($result[0]->rows) && count($result[0]->rows), 'Correctly returns result of query');

        $client->write([
            ['query' => 'drop table if exists default.builder_test'],
        ], 1);

        $query = $builder->newQuery()->from(raw('numbers(0,10)'));
        $query->asyncWithQuery()->table(raw('numbers(10,10)'));

        $result = $query->get();

        $this->assertEquals(2, count($result));
        $this->assertEquals(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'], array_column($result[0]->rows, 'number'));
        $this->assertEquals(['10', '11', '12', '13', '14', '15', '16', '17', '18', '19'], array_column($result[1]->rows, 'number'));

        $this->expectException(\InvalidArgumentException::class);

        $this->getBuilder()->from('table1')->asyncWithQuery('string')->get();
    }

    public function test_insert()
    {
        $server = new Server('127.0.0.1');
        $client = new Client((new ServerProvider())->addServer($server));

        $client->write([
            ['query' => 'drop table if exists default.builder_test'],
            ['query' => 'create table if not exists default.builder_test (number UInt64, string String) engine = Memory'],
        ], 1);

        $builder = new Builder($client);

        $builder->table('builder_test')->insert([[
            'number' => 1,
            'string' => 'value1',
        ], [
            'number' => 2,
            'string' => 'value2',
        ]]);

        $result = $builder->table('builder_test')->orderBy('number')->get();

        $this->assertTrue($result->rows[0]['number'] == 1 && $result->rows[1]['number'] == 2, 'Correctly inserts data into table with values format and specified columns');

        $client->write([
            ['query' => 'drop table if exists default.builder_test'],
            ['query' => 'create table if not exists default.builder_test (number UInt64, string String) engine = Memory'],
        ], 1);

        $builder = new Builder($client);

        $builder->table('builder_test')->insert([
            [1, 'value1'], [2, 'value2'],
        ]);

        $result = $builder->table('builder_test')->orderBy('number')->get();

        $this->assertTrue($result->rows[0]['number'] == 1 && $result->rows[1]['number'] == 2, 'Correctly inserts data into table with values format without columns');

        $client->write([
            ['query' => 'drop table if exists default.builder_test'],
            ['query' => 'create table if not exists default.builder_test (number UInt64, string String) engine = Memory'],
        ], 1);

        $builder = new Builder($client);

        $builder->table('builder_test')->insert([1, 'value1']);

        $result = $builder->table('builder_test')->orderBy('number')->get();

        $this->assertTrue($result->rows[0]['number'] == 1, 'Correctly inserts data into table with values format and one row');

        $client->write([
            ['query' => 'drop table if exists default.builder_test'],
            ['query' => 'create table if not exists default.builder_test (number UInt64, string String) engine = Memory'],
        ], 1);

        $builder = new Builder($client);

        $builder->table('builder_test')->insert(['number' => 1, 'string' => 'value1']);

        $result = $builder->table('builder_test')->orderBy('number')->get();

        $this->assertTrue($result->rows[0]['number'] == 1, 'Correctly inserts data into table with values format and one row with columns');

        $this->assertFalse($builder->table('table')->insert([]), 'Fails to insert empty dataset');
    }

    protected function putInTempFile(string $content): string
    {
        $fileName = tempnam(sys_get_temp_dir(), 'builder_');
        file_put_contents($fileName, $content);

        return $fileName;
    }

    public function test_insert_files()
    {
        $server = new Server('127.0.0.1');
        $client = new Client((new ServerProvider())->addServer($server));

        $client->write([
            ['query' => 'drop table if exists default.builder_test'],
            ['query' => 'create table if not exists default.builder_test (number UInt64, string String) engine = Memory'],
        ], 1);

        $realFiles = [
            $this->putInTempFile('5'.PHP_EOL.'6'.PHP_EOL),
            $this->putInTempFile('7'.PHP_EOL.'8'.PHP_EOL),
            $this->putInTempFile('9'.PHP_EOL.'10'.PHP_EOL),
        ];

        $files = [
            '1'.PHP_EOL.'2'.PHP_EOL,
            new FileFromString('3'.PHP_EOL.'4'.PHP_EOL),
            new File($realFiles[0]),
            new TempTable('test', new File($realFiles[1]), ['number' => 'UInt64']),
            $realFiles[2],
        ];

        $builder = new Builder($client);
        $builder->table('builder_test')->insertFiles(['number'], $files, Format::TSV, 5);

        $builder = new Builder($client);
        $result = $builder->table('builder_test')->orderBy('number')->get();

        $this->assertEquals(10, count($result->rows), 'Correctly inserts all types of files');

        $this->expectException(\TypeError::class);

        $builder = new Builder($client);
        $builder->table('builder_test')->insertFiles(['number'], [new \Exception('test')], Format::TSV, 5);
    }

    public function testCompileAsyncQueries()
    {
        $builder = $this->getBuilder();
        $builder2 = null;
        $builder3 = null;

        $builder->from('table1')->asyncWithQuery(function ($builder) use (&$builder2, &$builder3) {
            $builder2 = $builder;

            $builder->from('table2')->asyncWithQuery(function ($builder) use (&$builder3) {
                $builder3 = $builder;

                $builder->from('table3');
            });
        });

        $sqls = $builder->getAsyncQueries();

        $this->assertEquals([
            $builder,
            $builder2,
            $builder3,
        ], $sqls);
    }

    protected function createBuilder()
    {
        $serverProvider = new ServerProvider();
        $serverProvider->addServer(new Server('localhost', 8123, 'default'));
        $client = new Client($serverProvider);

        return new Builder($client);
    }

    public function testDelete()
    {
        $builder = $this->createBuilder();
        $builder->dropTableIfExists('test');
        $builder->createTable('test', 'MergeTree order by number', [
            'number' => 'UInt64',
        ]);

        $builder->newQuery()->table('test')->insertFile(['number'], new FileFromString('0'.PHP_EOL.'1'.PHP_EOL.'2'));

        $result = $builder->newQuery()->table('test')->count();

        $this->assertEquals(3, $result);

        $builder->newQuery()->table('test')->where('number', '=', 1)->delete();

        /*
         * We have to sleep for 3 seconds while mutation in progress
         */
        sleep(3);

        $result = $builder->newQuery()->table('test')->count();

        $this->assertEquals(2, $result);
    }

    public function testDropTable()
    {
        $builder = $this->createBuilder();
        $builder->dropTableIfExists('test');
        $builder->createTable('test', 'MergeTree order by number', [
            'number' => 'UInt64',
        ]);
        $builder->dropTable('test');

        $result = $builder->newQuery()->table('system.tables')->where('name', '=', 'test')->count();

        $this->assertEquals(0, $result);
    }

    public function testCount()
    {
        $result = $this->createBuilder()->table(raw('numbers(0,10)'))->count();

        $this->assertEquals(10, $result);

        $result = $this->createBuilder()->newQuery()->table(raw('numbers(0,10)'))->groupBy(raw('number % 2'))->count();

        $this->assertEquals(2, $result);
    }

    public function testOnCluster()
    {
        $builder = $this->getBuilder();
        $builder->onCluster('test');

        $this->assertEquals('test', $builder->getOnCluster(), 'Can execute query on cluster');
    }

    public function testArrayJoin()
    {
        $builder = $this->getBuilder();
        $builder->table('test')->arrayJoin('someArr');

        $this->assertEquals('SELECT * FROM `test` ARRAY JOIN `someArr`', $builder->toSql());
    }

    public function testLeftArrayJoin()
    {
        $builder = $this->getBuilder();
        $builder->table('test')->leftArrayJoin('someArr');

        $this->assertEquals('SELECT * FROM `test` LEFT ARRAY JOIN `someArr`', $builder->toSql());
    }

    public function testAddFile()
    {
        $builder = $this->getBuilder();
        $builder->addFile(new TempTable('_numbers', '', ['number' => 'UInt64']));
        $builder->addFile(new TempTable('_numbers2', '', ['number' => 'UInt64']));

        $this->assertEquals(2, count($builder->getFiles()));
        $this->assertArrayHasKey('_numbers', $builder->getFiles());
        $this->assertArrayHasKey('_numbers2', $builder->getFiles());
    }

    public function testToAsyncSqlsAndQueries()
    {
        $builder = $this->createBuilder();
        $builder->table('system.tables')
            ->where('database', '=', 'default')
            ->where('name', '=', 'builder_test1');

        $builder->asyncWithQuery(function ($builder) {
            $builder
                ->table('system.tables')
                ->where('database', '=', 'default')
                ->where('name', '=', 'builder_test2');
        });

        $builder->asyncWithQuery(function ($builder) {
            $builder
                ->table('system.tables')
                ->where('database', '=', 'default')
                ->where('name', '=', 'builder_test3');
        });

        $sqls = $builder->toAsyncSqls();
        $queries = $builder->toAsyncQueries();

        $this->assertEquals(3, count($sqls));
        $this->assertEquals(3, count($queries));

        $sqls = array_column($sqls, 'query');
        $queries = array_map(function (Query $query) {
            return $query->getQuery();
        }, $queries);

        $this->assertContains('SELECT * FROM `system`.`tables` WHERE `database` = \'default\' AND `name` = \'builder_test1\'', $sqls);
        $this->assertContains('SELECT * FROM `system`.`tables` WHERE `database` = \'default\' AND `name` = \'builder_test2\'', $sqls);
        $this->assertContains('SELECT * FROM `system`.`tables` WHERE `database` = \'default\' AND `name` = \'builder_test3\'', $sqls);

        $this->assertContains('SELECT * FROM `system`.`tables` WHERE `database` = \'default\' AND `name` = \'builder_test1\'', $queries);
        $this->assertContains('SELECT * FROM `system`.`tables` WHERE `database` = \'default\' AND `name` = \'builder_test2\'', $queries);
        $this->assertContains('SELECT * FROM `system`.`tables` WHERE `database` = \'default\' AND `name` = \'builder_test3\'', $queries);
    }

    public function testJoinWithOnClause()
    {
        $builder = $this->getBuilder();
        $builder->from('table1')->anyLeftJoin(function (JoinClause $join) {
            $join->table('table2')->on('column_from_table_1', '=', 'column_from_table_2');
        });
        $this->assertEquals('SELECT * FROM `table1` ANY LEFT JOIN `table2` ON `column_from_table_1` = `column_from_table_2`', $builder->toSql());

        $builder = $this->getBuilder();
        $builder->from('table1')->anyLeftJoin(function (JoinClause $join) {
            $join->table('table2')->on('column_from_table_1', '=', raw('toUInt32(`column_from_table_2`)'));
        });
        $this->assertEquals('SELECT * FROM `table1` ANY LEFT JOIN `table2` ON `column_from_table_1` = toUInt32(`column_from_table_2`)', $builder->toSql());
    }

    public function testMultipleJoins()
    {
        $builder = $this->getBuilder();
        $builder->from('table1')->anyLeftJoin(function (JoinClause $join) {
            $join->table('table2')->on('column_from_table_2', '=', 'column_from_table_1');
        });
        $builder->from('table1')->allLeftJoin(function (JoinClause $join) {
            $join->table('table3')->on('column_from_table_3', '=', 'column_from_table_1');
        });
        $this->assertEquals('SELECT * FROM `table1` ANY LEFT JOIN `table2` ON `column_from_table_2` = `column_from_table_1` ALL LEFT JOIN `table3` ON `column_from_table_3` = `column_from_table_1`', $builder->toSql());
    }
}
