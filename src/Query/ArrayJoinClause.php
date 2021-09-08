<?php

namespace Tinderbox\ClickhouseBuilder\Query;

use Tinderbox\ClickhouseBuilder\Query\Enums\JoinType;

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
     * Join type.
     *
     * @var JoinType|null
     */
    private $type;

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
     * Set LEFT join type.
     *
     * @return ArrayJoinClause
     */
    public function left(): self
    {
        return $this->type(JoinType::LEFT);
    }

    /**
     * Set join type.
     *
     * @param string $type
     *
     * @return ArrayJoinClause
     */
    public function type(string $type): self
    {
        $this->type = new JoinType(strtoupper($type));

        return $this;
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

    /**
     * Get join type.
     *
     * @return JoinType|null
     */
    public function getType(): ?JoinType
    {
        return $this->type;
    }
}
