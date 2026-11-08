<?php

declare(strict_types=1);

namespace FireMQ\Protocol;

use MessagePack\CanBePacked;
use MessagePack\Packer;

/**
 * A MessagePack map whose keys are always encoded as strings, even keys PHP stores as
 * integers (such as "42").
 *
 * @internal
 */
final class StrMap implements CanBePacked
{
    /**
     * @param array<array-key, mixed> $map values already normalized by Codec::normalize()
     */
    public function __construct(public readonly array $map)
    {
    }

    public function pack(Packer $packer): string
    {
        $out = $packer->packMapHeader(count($this->map));
        foreach ($this->map as $key => $value) {
            $out .= $packer->packStr((string) $key);
            $out .= $packer->pack($value);
        }

        return $out;
    }
}
