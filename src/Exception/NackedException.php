<?php

declare(strict_types=1);

namespace FireMQ\Exception;

/**
 * The broker rejected a publish, for example because a queue with
 * `x-overflow: reject-publish` is full. getCode() returns the AMQP reply code.
 */
class NackedException extends FireMQException
{
    public function __construct(int $code, string $reason)
    {
        parent::__construct(sprintf('publish rejected: %s (code %d)', $reason, $code), $code);
    }
}
