<?php

namespace Tinderbox\ClickhouseBuilder\Exceptions;

class BuilderException extends Exception
{
    public static function cannotDetermineAliasForColumn()
    {
        return new static('Cannot determine alias for the column');
    }
}
