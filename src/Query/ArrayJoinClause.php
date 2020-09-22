<?php

namespace Tinderbox\ClickhouseBuilder\Query;

class ArrayJoinClause
{
    /**
     * Identifier of array to join.
     *
     * @var Expression|Identifier
     */
    private $arrayIdentifier;

    /**
     * Builder which initiated join.
     *
     * @var Builder
     */
    private $query;

    /**
     * JoinClause constructor.
     *
     * @param BaseBuilder $query
     */
    public function __construct(BaseBuilder $query)
    {
        $this->query = $query;
    }

    /**
     * Set array identifier for join.
     *
     * @param string|Expression $arrayIdentifier
     *
     * @return ArrayJoinClause
     */
    public function array($arrayIdentifier): self
    {
        if (is_string($arrayIdentifier)) {
            $arrayIdentifier = new Identifier($arrayIdentifier);
        }

        $this->arrayIdentifier = $arrayIdentifier;

        return $this;
    }

    /**
     * Get array identifier to join.
     *
     * @return Expression|Identifier
     */
    public function getArrayIdentifier()
    {
        return $this->arrayIdentifier;
    }
}
