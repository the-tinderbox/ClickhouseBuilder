<?php

namespace Tinderbox\ClickhouseBuilder;

use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery as m;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Client;
use Tinderbox\ClickhouseBuilder\Query\Builder;
use Tinderbox\ClickhouseBuilder\Query\Enums\JoinStrict;
use Tinderbox\ClickhouseBuilder\Query\Enums\JoinType;
use Tinderbox\ClickhouseBuilder\Query\Enums\Operator;
use Tinderbox\ClickhouseBuilder\Query\Identifier;
use Tinderbox\ClickhouseBuilder\Query\JoinClause;
use Tinderbox\ClickhouseBuilder\Query\TwoElementsLogicExpression;

class JoinClauseTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function getBuilder(): Builder
    {
        return new Builder(m::mock(Client::class));
    }

    public function testSettersGetters()
    {
        $join = new JoinClause($this->getBuilder());
        $join->table('table');
        $join->using(['column', 'another_column']);
        $join->addUsing('third_column');
        $join->addUsing(new Identifier('other_column'));

        $this->assertEquals('table', $join->getTable());
        $this->assertEquals(['column', 'another_column', 'third_column', 'other_column'], array_map(function ($using) {
            return (string) $using;
        }, $join->getUsing()));

        $join = new JoinClause($this->getBuilder());
        $join->on('first_column', '=', 'second_column');
        $join->on('first_column', '=', 'third_column');
        $this->assertEquals([
            (new TwoElementsLogicExpression($this->getBuilder()))->firstElement(new Identifier('first_column'))->operator('=')->secondElement(new Identifier('second_column'))->concatOperator(Operator::AND),
            (new TwoElementsLogicExpression($this->getBuilder()))->firstElement(new Identifier('first_column'))->operator('=')->secondElement(new Identifier('third_column'))->concatOperator(Operator::AND),
        ], $join->getOnClauses());

        $join->strict(JoinStrict::ALL);

        $this->assertEquals(JoinStrict::ALL, (string) $join->getStrict());

        $join->type(JoinType::LEFT);

        $this->assertEquals(JoinType::LEFT, (string) $join->getType());

        $join->any();
        $this->assertEquals(JoinStrict::ANY, (string) $join->getStrict());

        $join->all();
        $this->assertEquals(JoinStrict::ALL, (string) $join->getStrict());

        $join->inner();
        $this->assertEquals(JoinType::INNER, (string) $join->getType());

        $join->left();
        $this->assertEquals(JoinType::LEFT, (string) $join->getType());

        $join->distributed(true);
        $this->assertTrue($join->isDistributed());

        $alias = 'test';
        $join->as($alias);
        $this->assertEquals($join->getAlias(), $alias);

        $alias = 'test1';
        $join->subQuery($alias);
        $this->assertEquals($join->getAlias(), $alias);
    }

    public function testQuery()
    {
        $join = new JoinClause($this->getBuilder());
        $join = $join->query();

        $this->assertInstanceOf(Builder::class, $join);

        $join = new JoinClause($this->getBuilder());
        $join->query(function ($join) {
            $join->table('table');
        });

        $this->assertEquals('(SELECT * FROM `table`)', (string) $join->getTable());
    }

    public function testSubQuery()
    {
        $join = new JoinClause($this->getBuilder());
        $join->query();

        $subQuery = $join->getSubQuery();

        $this->assertInstanceOf(Builder::class, $subQuery);
    }
}
