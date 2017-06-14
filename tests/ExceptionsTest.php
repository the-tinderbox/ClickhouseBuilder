<?php

namespace Tinderbox\ClickhouseBuilder;

use PHPUnit\Framework\TestCase;
use Tinderbox\ClickhouseBuilder\Exceptions\BuilderException;
use Tinderbox\ClickhouseBuilder\Exceptions\GrammarException;
use Tinderbox\ClickhouseBuilder\Exceptions\NotSupportedException;
use Tinderbox\ClickhouseBuilder\Query\Builder;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Tinderbox\Clickhouse\Client;
use Mockery as m;
use Tinderbox\ClickhouseBuilder\Query\From;
use Tinderbox\ClickhouseBuilder\Query\JoinClause;

class ExceptionsTest extends TestCase
{
    use MockeryPHPUnitIntegration;
    
    public function getBuilder() : Builder
    {
        return new Builder(m::mock(Client::class));
    }
    
    public function testBuilderException()
    {
        $e = BuilderException::cannotDetermineAliasForColumn();
        $this->assertInstanceOf(BuilderException::class, $e);
    }
    
    public function testGrammarException()
    {
        $e = GrammarException::missedTableForInsert();
        $this->assertInstanceOf(GrammarException::class, $e);
    
        $from = new From($this->getBuilder());
        
        $e = GrammarException::wrongFrom($from);
        $this->assertInstanceOf(GrammarException::class, $e);
    
        $join = new JoinClause($this->getBuilder());
        
        $e = GrammarException::wrongJoin($join);
        $this->assertInstanceOf(GrammarException::class, $e);
    }
    
    public function testNotSupportedException()
    {
        $e = NotSupportedException::transactions();
        $this->assertInstanceOf(NotSupportedException::class, $e);
    
        $e = NotSupportedException::updateAndDelete();
        $this->assertInstanceOf(NotSupportedException::class, $e);
    }
}
