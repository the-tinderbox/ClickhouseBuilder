<?php

namespace Tinderbox\ClickhouseBuilder;

use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery as m;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Client;
use Tinderbox\ClickhouseBuilder\Query\Builder;
use Tinderbox\ClickhouseBuilder\Query\Column;
use Tinderbox\ClickhouseBuilder\Query\Expression;

class ColumnTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function getBuilder(): Builder
    {
        return new Builder(m::mock(Client::class));
    }

    public function testSettersGetters()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->as('alias');

        $this->assertEquals('alias', $column->getAlias());

        $column->alias('new_alias');

        $this->assertEquals('new_alias', $column->getAlias());

        $this->assertEquals('column', (string) $column->getColumnName());

        $column->name(function ($column) {
            $column->name('new_column');
        });

        $this->assertEquals('new_column', (string) $column->getColumnName()->getColumnName());
    }

    public function testRunningDifference()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->runningDifference();

        $functions = $column->getFunctions();

        $this->assertEquals([
            [
                'function' => 'runningDifference',
            ],
        ], $functions);
    }

    public function testSumIfPlain()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->sumIf('>', '1');

        $functions = $column->getFunctions();

        $this->assertEquals([
            [
                'function' => 'sumIf',
                'params'   => '> 1',
            ],
        ], $functions);
    }

    public function testSumIfExpression()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->sumIf(new Expression('> 1'));

        $functions = $column->getFunctions();

        $this->assertEquals([
            [
                'function' => 'sumIf',
                'params'   => '> 1',
            ],
        ], $functions);
    }

    public function testSum()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->sum();

        $functions = $column->getFunctions();

        $this->assertEquals([
            [
                'function' => 'sum',
            ],
        ], $functions);
    }

    public function testRound()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->round(2);

        $functions = $column->getFunctions();

        $this->assertEquals([
            [
                'function' => 'round',
                'params'   => 2,
            ],
        ], $functions);
    }

    public function testPlus()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->plus('2');

        $functions = $column->getFunctions();

        $this->assertEquals([
            [
                'function' => 'plus',
                'params'   => '2',
            ],
        ], $functions);
    }

    public function testCount()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->count();

        $functions = $column->getFunctions();

        $this->assertEquals([
            [
                'function' => 'count',
            ],
        ], $functions);
    }

    public function testDistinct()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->distinct();

        $functions = $column->getFunctions();

        $this->assertEquals([
            [
                'function' => 'distinct',
            ],
        ], $functions);
    }

    public function testMultiple()
    {
        $column = new Column($this->getBuilder());
        $column->name('column');
        $column->multiple('2');

        $functions = $column->getFunctions();

        $this->assertEquals([
            [
                'function' => 'multiple',
                'params'   => '2',
            ],
        ], $functions);
    }

    public function testQuery()
    {
        $column = new Column($this->getBuilder());
        $subQuery = $column->query()->select('column')->from('table');

        $this->assertInstanceOf(Builder::class, $subQuery);
        $this->assertEquals('SELECT `column` FROM `table`', $subQuery->toSql());

        $column = $column->query(function ($subQuery) {
            $subQuery->select('column')->from('table');
        });

        $this->assertInstanceOf(Column::class, $column);
        $this->assertInstanceOf(Expression::class, $column->getColumnName());
        $this->assertEquals('(SELECT `column` FROM `table`)', $column->getColumnName()->getValue());

        $subQuery = $column->getSubQuery();
        $this->assertInstanceOf(Builder::class, $subQuery);
        $this->assertEquals('SELECT `column` FROM `table`', $subQuery->toSql());

        $builder = $this->getBuilder();
        $builder->select('another_column')->from('another_table');

        $column = $column->query($builder);
        $this->assertInstanceOf(Column::class, $column);
        $this->assertEquals('(SELECT `another_column` FROM `another_table`)', $column->getColumnName()->getValue());
    }
}
