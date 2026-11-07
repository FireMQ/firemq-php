<?php

declare(strict_types=1);

namespace FireMQ;

/**
 * Options for Client::declareQueue(). Queues are durable by default.
 */
final class QueueOptions
{
    use OptionsTrait;

    /**
     * @param bool $durable survives a broker restart
     * @param bool $exclusive used only by this connection and deleted when it closes
     * @param bool $autoDelete deleted when its last subscription is removed
     * @param bool $passive only check that the queue exists
     * @param array<string, mixed> $args RabbitMQ queue arguments, for example `x-message-ttl`
     */
    public function __construct(
        public readonly bool $durable = true,
        public readonly bool $exclusive = false,
        public readonly bool $autoDelete = false,
        public readonly bool $passive = false,
        public readonly array $args = [],
    ) {
    }

    /**
     * A queue that lives only as long as this connection: not durable, exclusive and
     * auto-deleted. Pair it with an empty name to let the broker pick one.
     *
     * @param array<string, mixed> $args
     */
    public static function temporary(array $args = []): self
    {
        return new self(durable: false, exclusive: true, autoDelete: true, args: $args);
    }
}
