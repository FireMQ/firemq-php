<?php

declare(strict_types=1);

namespace FireMQ;

/**
 * Options for Client::publish(). Messages are persistent and confirmed by default.
 */
final class PublishOptions
{
    use OptionsTrait;

    /** Message properties that hold strings. */
    public const STRING_PROPERTIES = [
        'content_type',
        'content_encoding',
        'correlation_id',
        'reply_to',
        'message_id',
        'type',
        'user_id',
        'app_id',
    ];

    /** Message properties that hold unsigned integers. */
    public const INT_PROPERTIES = ['expiration', 'timestamp', 'priority'];

    /** @var array<string, string|int> */
    public readonly array $properties;

    /**
     * @param bool $persistent write the message to disk; it survives a restart when the
     *                         queue is durable
     * @param bool $mandatory fail with UnroutableException when no queue receives the message
     * @param bool $confirm wait for the broker's confirm; when false, publish() returns once
     *                      the frame is written
     * @param array<string, mixed> $properties message properties with the protocol's names:
     *                                         content_type, content_encoding, correlation_id,
     *                                         reply_to, expiration (ms), message_id,
     *                                         timestamp (Unix seconds or DateTimeInterface),
     *                                         type, user_id, app_id, priority (0-255)
     * @param array<string, mixed> $headers application headers; values are any MessagePack type
     */
    public function __construct(
        public readonly bool $persistent = true,
        public readonly bool $mandatory = false,
        public readonly bool $confirm = true,
        array $properties = [],
        public readonly array $headers = [],
    ) {
        $this->properties = self::normalizeProperties($properties);
    }

    /**
     * Validates property names and types. Null values are dropped. Integer properties
     * accept numeric strings, and `timestamp` accepts a DateTimeInterface.
     *
     * @param array<array-key, mixed> $properties
     *
     * @return array<string, string|int>
     */
    public static function normalizeProperties(array $properties): array
    {
        $out = [];
        foreach ($properties as $name => $value) {
            if ($value === null) {
                continue;
            }
            $name = (string) $name;
            if (in_array($name, self::STRING_PROPERTIES, true)) {
                if (!is_string($value) && !is_int($value) && !$value instanceof \Stringable) {
                    throw new \InvalidArgumentException(sprintf('property "%s" must be a string', $name));
                }
                $out[$name] = (string) $value;
            } elseif (in_array($name, self::INT_PROPERTIES, true)) {
                if ($name === 'timestamp' && $value instanceof \DateTimeInterface) {
                    $value = $value->getTimestamp();
                }
                if (is_string($value) && ctype_digit($value)) {
                    $value = (int) $value;
                }
                if (!is_int($value) || $value < 0 || ($name === 'priority' && $value > 255)) {
                    throw new \InvalidArgumentException(sprintf(
                        'property "%s" must be an integer %s',
                        $name,
                        $name === 'priority' ? 'from 0 to 255' : 'of at least 0',
                    ));
                }
                $out[$name] = $value;
            } else {
                throw new \InvalidArgumentException(sprintf(
                    'unknown message property "%s"; use one of %s',
                    $name,
                    implode(', ', [...self::STRING_PROPERTIES, ...self::INT_PROPERTIES]),
                ));
            }
        }

        return $out;
    }
}
