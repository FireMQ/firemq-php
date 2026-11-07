<?php

declare(strict_types=1);

namespace FireMQ\Exception;

/**
 * The broker refused a request. getCode() returns the AMQP reply code, for example
 * 404 (not found), 403 (access refused) or 406 (precondition failed).
 */
class ServerException extends FireMQException
{
    public function __construct(int $code, string $message, ?\Throwable $previous = null)
    {
        parent::__construct(sprintf('%s (code %d)', $message, $code), $code, $previous);
    }
}
