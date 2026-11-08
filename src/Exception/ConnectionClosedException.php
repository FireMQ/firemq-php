<?php

declare(strict_types=1);

namespace FireMQ\Exception;

/**
 * The connection could not be opened, or it is closed. When the broker closed it with a
 * reply code, getCode() returns that code; otherwise it returns 0.
 */
class ConnectionClosedException extends FireMQException
{
}
