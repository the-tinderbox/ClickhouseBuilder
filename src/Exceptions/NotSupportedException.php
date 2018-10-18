<?php

namespace Tinderbox\ClickhouseBuilder\Exceptions;

class NotSupportedException extends Exception
{
    public static function transactions()
    {
        return new static('Transactions is not supported by Clickhouse');
    }

    public static function update()
    {
        return new static('Update and delete queries is not supported by Clickhouse');
    }
}
