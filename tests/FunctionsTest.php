<?php

namespace Tinderbox\ClickhouseBuilder;

use PHPUnit\Framework\TestCase;
use Tinderbox\ClickhouseBuilder\Query\Expression;

class FunctionsTest extends TestCase
{
    public function testTap()
    {
        $value = 1;
        $result = 0;
        $callback = function () use (&$result) {
            $result = 2;
        };

        $returnedValue = tap($value, $callback);

        $this->assertEquals($value, $returnedValue);
        $this->assertEquals(2, $result);
    }

    public function testArrayFlatten()
    {
        $array = [
            'first' => [
                'second' => [
                    'third' => 'value',
                ],
            ],
        ];

        $flatten = array_flatten($array);

        $this->assertEquals(['value'], $flatten);

        $flatten = array_flatten($array, 1);

        $this->assertEquals(
            [
                [
                    'third' => 'value',
                ],
            ],
            $flatten
        );
    }

    public function testRaw()
    {
        $expression = raw('test');

        $this->assertInstanceOf(Expression::class, $expression);
    }
}
