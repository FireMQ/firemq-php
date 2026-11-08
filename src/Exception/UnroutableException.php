<?php

declare(strict_types=1);

namespace FireMQ\Exception;

/**
 * A mandatory publish reached no queue. getCode() returns 312 (no-route).
 */
class UnroutableException extends FireMQException
{
    public function __construct(
        public readonly string $exchange,
        public readonly string $routingKey,
        int $code = 312,
        string $reason = 'NO_ROUTE',
    ) {
        parent::__construct(
            sprintf(
                "message returned (%s): no queue is bound to exchange '%s' with key '%s'",
                $reason === '' ? 'NO_ROUTE' : $reason,
                $exchange,
                $routingKey,
            ),
            $code,
        );
    }
}
