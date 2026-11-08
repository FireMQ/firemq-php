<?php

declare(strict_types=1);

namespace FireMQ\Protocol;

/**
 * A decoded frame: op code, header map and raw payload.
 */
final class Frame
{
    /**
     * @param array<string, mixed> $header
     */
    public function __construct(
        public readonly int $op,
        public readonly array $header = [],
        public readonly string $payload = '',
    ) {
    }

    public function name(): string
    {
        return Op::name($this->op);
    }

    /**
     * Encodes the frame with its length prefix, as sent on TCP and TLS.
     */
    public function encode(): string
    {
        return Codec::encodeFrame($this->op, $this->header, $this->payload);
    }
}
