<?php

declare(strict_types=1);

namespace FireMQ\Protocol;

use FireMQ\Exception\ProtocolException;
use MessagePack\BufferUnpacker;
use MessagePack\CanBePacked;
use MessagePack\PackOptions;
use MessagePack\Packer;

/**
 * Encodes and decodes FireMQ frames:
 *
 *     len: u32 | op: u8 | hlen: u32 | header (MessagePack map) | payload
 *
 * All integers are big-endian. `len` counts every byte after itself.
 */
final class Codec
{
    /** Bytes a TCP or TLS client sends before its first frame. */
    public const MAGIC = 'FMQ1';

    /** Protocol major version. */
    public const PROTOCOL_VERSION = 1;

    /** Largest frame accepted before `welcome`, counted like `len`. */
    public const MAX_HELLO_FRAME = 65536;

    /** Default `welcome.max_frame`. */
    public const DEFAULT_MAX_FRAME = 16 * 1024 * 1024;

    private static ?Packer $packer = null;

    private static ?BufferUnpacker $unpacker = null;

    private function __construct()
    {
    }

    /**
     * Encodes a frame with its length prefix. An empty header is sent with `hlen` 0.
     *
     * @param array<string, mixed> $header
     */
    public static function encodeFrame(int $op, array $header = [], string $payload = ''): string
    {
        $h = $header === [] ? '' : self::encodeMap($header);

        return pack('NCN', 5 + strlen($h) + strlen($payload), $op, strlen($h)) . $h . $payload;
    }

    /**
     * Encodes a header map. Keys are always MessagePack strings.
     *
     * @param array<array-key, mixed> $map
     */
    public static function encodeMap(array $map): string
    {
        $packer = self::$packer ??= new Packer(PackOptions::DETECT_STR_BIN | PackOptions::DETECT_ARR_MAP | PackOptions::FORCE_FLOAT64);
        try {
            return (new StrMap(self::normalizeMap($map)))->pack($packer);
        } catch (\MessagePack\Exception\PackingFailedException $e) {
            throw new \InvalidArgumentException('cannot encode value as MessagePack: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Decodes a header. An empty string is an empty map.
     *
     * @return array<string, mixed>
     */
    public static function decodeHeader(string $bytes): array
    {
        if ($bytes === '') {
            return [];
        }
        $unpacker = self::$unpacker ??= new BufferUnpacker();
        try {
            $unpacker->reset($bytes);

            /** @var array<string, mixed> */
            return $unpacker->unpackMap();
        } catch (\Throwable $e) {
            throw new ProtocolException('header is not a MessagePack map: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Splits one frame off a TCP byte stream, starting at $offset.
     *
     * Returns null until a whole frame is buffered. On success, advances $offset past the
     * frame and returns [op, header bytes, payload]. $maxFrame bounds the value of `len`.
     *
     * @return array{0: int, 1: string, 2: string}|null
     *
     * @throws ProtocolException when the frame is too large or malformed
     */
    public static function splitFrame(string $buffer, int &$offset, int $maxFrame): ?array
    {
        $available = strlen($buffer) - $offset;
        if ($available < 4) {
            return null;
        }
        /** @var int $len */
        $len = unpack('N', $buffer, $offset)[1];
        if ($len > $maxFrame) {
            throw new ProtocolException(sprintf('frame of %d bytes exceeds the maximum of %d bytes', $len, $maxFrame));
        }
        if ($len < 5) {
            throw new ProtocolException('malformed frame: frame shorter than 5 bytes');
        }
        if ($available < 4 + $len) {
            return null;
        }
        $op = ord($buffer[$offset + 4]);
        /** @var int $hlen */
        $hlen = unpack('N', $buffer, $offset + 5)[1];
        if ($hlen > $len - 5) {
            throw new ProtocolException('malformed frame: header length exceeds frame');
        }
        $header = $hlen > 0 ? substr($buffer, $offset + 9, $hlen) : '';
        $payloadLength = $len - 5 - $hlen;
        $payload = $payloadLength > 0 ? substr($buffer, $offset + 9 + $hlen, $payloadLength) : '';
        $offset += 4 + $len;

        return [$op, $header, $payload];
    }

    /**
     * Decodes one frame from the front of $buffer. Returns null when the buffer holds less
     * than a whole frame.
     */
    public static function decodeFrame(string $buffer, int $maxFrame = self::DEFAULT_MAX_FRAME): ?Frame
    {
        $offset = 0;
        $raw = self::splitFrame($buffer, $offset, $maxFrame);
        if ($raw === null) {
            return null;
        }

        return new Frame($raw[0], self::decodeHeader($raw[1]), $raw[2]);
    }

    /**
     * Prepares a PHP value for MessagePack: arrays that are lists become MessagePack arrays,
     * other arrays and objects become maps with string keys. Use `new \stdClass()` for an
     * empty map; an empty PHP array is an empty MessagePack array.
     */
    public static function normalize(mixed $value): mixed
    {
        if ($value === null || is_scalar($value) || $value instanceof CanBePacked) {
            return $value;
        }
        if (is_array($value)) {
            if (array_is_list($value)) {
                return array_map(self::normalize(...), $value);
            }

            return new StrMap(self::normalizeMap($value));
        }
        if ($value instanceof \stdClass) {
            return new StrMap(self::normalizeMap(get_object_vars($value)));
        }
        if ($value instanceof \DateTimeInterface) {
            return $value->getTimestamp();
        }
        if ($value instanceof \JsonSerializable) {
            return self::normalize($value->jsonSerialize());
        }
        if ($value instanceof \Traversable) {
            return self::normalize(iterator_to_array($value));
        }

        throw new \InvalidArgumentException(sprintf('cannot encode a value of type %s as MessagePack', get_debug_type($value)));
    }

    /**
     * @param array<array-key, mixed> $map
     *
     * @return array<array-key, mixed>
     */
    private static function normalizeMap(array $map): array
    {
        foreach ($map as $key => $value) {
            $map[$key] = self::normalize($value);
        }

        return $map;
    }
}
