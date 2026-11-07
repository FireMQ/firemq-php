<?php

declare(strict_types=1);

namespace FireMQ;

/**
 * Builds an options object from an array whose keys are the constructor's parameter names,
 * in camelCase or snake_case (`autoDelete` or `auto_delete`).
 *
 * @internal
 */
trait OptionsTrait
{
    /**
     * @param self|array<string, mixed> $options
     */
    public static function from(self|array $options): static
    {
        if ($options instanceof self) {
            return $options;
        }
        $named = [];
        foreach ($options as $key => $value) {
            if (!is_string($key)) {
                throw new \InvalidArgumentException(sprintf('%s keys must be option names', self::class));
            }
            $named[lcfirst(str_replace('_', '', ucwords($key, '_')))] = $value;
        }
        try {
            return new static(...$named);
        } catch (\TypeError $e) {
            throw new \InvalidArgumentException(sprintf('%s: %s', self::class, $e->getMessage()), 0, $e);
        } catch (\Error $e) {
            if (str_contains($e->getMessage(), 'Unknown named parameter')) {
                throw new \InvalidArgumentException(sprintf('%s: %s', self::class, $e->getMessage()), 0, $e);
            }
            throw $e;
        }
    }
}
