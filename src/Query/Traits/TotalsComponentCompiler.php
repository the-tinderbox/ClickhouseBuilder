<?php

namespace Tinderbox\ClickhouseBuilder\Query\Traits;

use Tinderbox\ClickhouseBuilder\Query\BaseBuilder as Builder;

trait TotalsComponentCompiler
{
    /**
     * @param Builder $builder
     * @param bool $hasTotals
     *
     * @return string
     */
    public function compileTotalsComponent(Builder $builder, bool $hasTotals): string
    {
        return 'WITH TOTALS';
    }
}
