<?php

declare(strict_types=1);

namespace FireMQ;

use FireMQ\Exception\FireMQException;
use FireMQ\Protocol\Op;

/**
 * A message from Subscription::next(), Client::consume() or Client::get().
 */
final class Message
{
    private bool $settled;

    /**
     * Messages are created by the client. Creating one yourself is useful in tests: without
     * a connection, ack() and nack() do nothing.
     *
     * @param array<string, mixed> $properties message properties with the protocol's names,
     *                                         for example `content_type` and `reply_to`
     * @param array<string, mixed> $headers application headers
     */
    public function __construct(
        public readonly string $body,
        public readonly string $exchange = '',
        public readonly string $routingKey = '',
        public readonly array $properties = [],
        public readonly array $headers = [],
        /** True when the message was delivered before and not acknowledged. */
        public readonly bool $redelivered = false,
        public readonly int $deliveryTag = 0,
        private readonly ?Connection $connection = null,
        private readonly ?Subscription $subscription = null,
    ) {
        $this->settled = $connection === null;
    }

    /**
     * Acknowledges the message. Does nothing for `noAck` messages or when already settled.
     *
     * @throws FireMQException when the connection is closed
     */
    public function ack(): void
    {
        $this->settle(Op::ACK, ['tag' => $this->deliveryTag]);
    }

    /**
     * Rejects the message. With $requeue it is delivered again; otherwise it is dead-lettered
     * when the queue has a dead-letter exchange, and dropped if not.
     *
     * @throws FireMQException when the connection is closed
     */
    public function nack(bool $requeue): void
    {
        $header = ['tag' => $this->deliveryTag];
        if ($requeue) {
            $header['requeue'] = true;
        }
        $this->settle(Op::NACK, $header);
    }

    /**
     * True once ack() or nack() was called, and always for `noAck` messages.
     */
    public function isSettled(): bool
    {
        return $this->settled;
    }

    /**
     * Decodes a JSON body.
     *
     * @throws FireMQException when the body is not valid JSON
     */
    public function json(bool $associative = true): mixed
    {
        try {
            return json_decode($this->body, $associative, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new FireMQException('message body is not valid JSON: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * One message property, or $default when it is not set.
     */
    public function property(string $name, mixed $default = null): mixed
    {
        return $this->properties[$name] ?? $default;
    }

    /**
     * @param array<string, mixed> $header
     */
    private function settle(int $op, array $header): void
    {
        if ($this->settled || $this->connection === null) {
            return;
        }
        $this->connection->send($op, $header);
        $this->settled = true;
        $this->subscription?->release(1);
    }
}
