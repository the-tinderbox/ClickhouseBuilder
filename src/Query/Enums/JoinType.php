<?php

namespace Tinderbox\ClickhouseBuilder\Query\Enums;

use MyCLabs\Enum\Enum;

/**
 * Join types.
 */
final class JoinType extends Enum
{
    public const INNER = 'INNER';
    public const LEFT = 'LEFT';
}
