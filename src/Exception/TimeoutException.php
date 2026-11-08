<?php

declare(strict_types=1);

namespace FireMQ\Exception;

/**
 * The broker did not answer a request or confirm a publish within the client's timeout.
 */
class TimeoutException extends FireMQException
{
}
