<?php

namespace Tinderbox\ClickhouseBuilder;

use PHPUnit\Framework\TestCase;
use Tinderbox\ClickhouseBuilder\Query\Tuple;

class TupleTest extends TestCase
{
    public function testGetElements()
    {
        $elements = [
            'one', 'two',
        ];

        $tuple = new Tuple($elements);

        $this->assertEquals($elements, $tuple->getElements());
    }

    public function testAddElements()
    {
        $elements = [
            'one', 'two',
        ];

        $tuple = new Tuple($elements);

        $tuple->addElements(['three', 'four']);

        $this->assertEquals(['one', 'two', 'three', 'four'], $tuple->getElements());

        $tuple->addElements('five', 'six', 'seven');

        $this->assertEquals(['one', 'two', 'three', 'four', 'five', 'six', 'seven'], $tuple->getElements());
    }
}
