<?php

declare(strict_types=1);

namespace FireMQ\Exception;

/**
 * The broker sent something the client cannot decode, or the client was asked to send
 * something the protocol cannot carry.
 */
class ProtocolException extends FireMQException
{
}
