<?php

declare(strict_types=1);

namespace FireMQ;

/**
 * Options for Client::subscribe() and Client::consume().
 */
final class SubscribeOptions
{
    use OptionsTrait;

    /**
     * @param int $prefetch how many unacknowledged messages the broker may send at once
     * @param bool $noAck treat messages as acknowledged when sent; faster, but messages are
     *                    lost if the consumer crashes
     * @param bool $exclusive fail with 405 if the queue has other consumers, and block new ones
     * @param string $tag consumer tag shown in the management UI
     * @param array<string, mixed> $args consumer arguments
     */
    public function __construct(
        public readonly int $prefetch = 100,
        public readonly bool $noAck = false,
        public readonly bool $exclusive = false,
        public readonly string $tag = '',
        public readonly array $args = [],
    ) {
        if ($prefetch < 0) {
            throw new \InvalidArgumentException('prefetch must not be negative');
        }
    }
}
