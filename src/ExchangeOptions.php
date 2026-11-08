<?php

declare(strict_types=1);

namespace FireMQ;

/**
 * Options for Client::declareExchange(). Exchanges are durable by default.
 */
final class ExchangeOptions
{
    use OptionsTrait;

    /**
     * @param bool $durable survives a broker restart
     * @param bool $autoDelete deleted when its last binding is removed
     * @param bool $internal cannot be published to directly
     * @param bool $passive only check that the exchange exists
     * @param array<string, mixed> $args exchange arguments, for example `alternate-exchange`
     */
    public function __construct(
        public readonly bool $durable = true,
        public readonly bool $autoDelete = false,
        public readonly bool $internal = false,
        public readonly bool $passive = false,
        public readonly array $args = [],
    ) {
    }
}
