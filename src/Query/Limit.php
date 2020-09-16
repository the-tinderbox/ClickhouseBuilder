<?php

namespace Tinderbox\ClickhouseBuilder\Query;

class Limit
{
    /**
     * Number of rows to take from result.
     *
     * @var int|null
     */
    private $limit;

    /**
     * Number of rows to skip.
     *
     * @var int|null
     */
    private $offset;

    /**
     * Columns to limit distinctly.
     *
     * @var array
     */
    private $by = [];

    /**
     * Limit constructor.
     *
     * @param int      $limit
     * @param int|null $offset
     * @param array    $by
     */
    public function __construct(int $limit, int $offset = null, array $by = [])
    {
        $this->limit = $limit;
        $this->offset = $offset;
        $this->by = $by;
    }

    /**
     * Get number of rows to take.
     *
     * @return int
     */
    public function getLimit(): ?int
    {
        return $this->limit;
    }

    /**
     * Get number of rows to skip.
     *
     * @return int
     */
    public function getOffset(): ?int
    {
        return $this->offset;
    }

    /**
     * Get columns to limit distinctly.
     *
     * @return array
     */
    public function getBy(): array
    {
        return $this->by;
    }
}
