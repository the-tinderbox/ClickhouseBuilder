<?php

namespace Tinderbox\ClickhouseBuilder;

use PHPUnit\Framework\TestCase;
use Tinderbox\ClickhouseBuilder\Query\Limit;

class LimitTest extends TestCase
{
    public function testGetters()
    {
        $limit = new Limit(10, 100, ['column']);

        $this->assertEquals(10, $limit->getLimit());
        $this->assertEquals(100, $limit->getOffset());
        $this->assertEquals(['column'], $limit->getBy());
    }
}
