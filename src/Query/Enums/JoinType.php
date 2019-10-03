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
    public const RIGHT = 'RIGHT';
    public const FULL = 'FULL';
    public const CROSS = 'CROSS';
    public const ASOF = 'ASOF';
}
