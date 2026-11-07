<?php

declare(strict_types=1);

namespace FireMQ;

/**
 * What Client::declareQueue() reports.
 */
final class QueueInfo
{
    public function __construct(
        /** The queue's name; the broker picks one when the declared name is empty. */
        public readonly string $name,
        /** Messages ready in the queue. */
        public readonly int $messages,
        /** Active subscriptions on the queue. */
        public readonly int $consumers,
    ) {
    }
}
