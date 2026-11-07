<?php

/**
 * FireMQ conformance driver for the PHP client.
 *
 * Reads one JSON command per line on stdin and writes exactly one JSON reply per line on
 * stdout, as described in spec/conformance.md. Logs go to stderr.
 *
 *     firemq-conformance run --driver "php conformance/driver.php"
 */

declare(strict_types=1);

use FireMQ\Client;
use FireMQ\Exception\ConnectionClosedException;
use FireMQ\Exception\NackedException;
use FireMQ\Exception\ServerException;
use FireMQ\Exception\UnroutableException;
use FireMQ\Message;
use FireMQ\Subscription;

ini_set('display_errors', 'stderr');
error_reporting(E_ALL);

require dirname(__DIR__) . '/vendor/autoload.php';

final class DriverError extends Exception
{
    public function __construct(public readonly string $kind, string $message, int $code = 0)
    {
        parent::__construct($message, $code);
    }
}

final class Driver
{
    private ?Client $client = null;

    /** @var array<string, Subscription> */
    private array $subs = [];

    /** @var array<string, Message> unsettled messages by tag */
    private array $messages = [];

    /**
     * @return array<string, mixed>|stdClass
     */
    public function handle(stdClass $cmd): array|stdClass
    {
        switch ($cmd->op) {
            case 'hello':
                return ['sdk' => 'firemq-php', 'version' => Client::VERSION, 'capabilities' => []];
            case 'connect':
                $previous = $this->client;
                $this->reset();
                $previous?->close();
                $this->client = Client::connect($cmd->url);

                return new stdClass();
            case 'close':
                $client = $this->client;
                $this->reset();
                $client?->close();

                return new stdClass();
            case 'declare_queue':
                $info = $this->client()->declareQueue($cmd->name, [
                    'durable' => $cmd->durable,
                    'exclusive' => $cmd->exclusive,
                    'auto_delete' => $cmd->auto_delete,
                    'args' => self::map($cmd->args ?? null),
                ]);

                return ['name' => $info->name, 'messages' => $info->messages, 'consumers' => $info->consumers];
            case 'delete_queue':
                return ['messages' => $this->client()->deleteQueue($cmd->name)];
            case 'purge_queue':
                return ['messages' => $this->client()->purgeQueue($cmd->name)];
            case 'declare_exchange':
                $this->client()->declareExchange($cmd->name, $cmd->type, ['durable' => $cmd->durable]);

                return new stdClass();
            case 'delete_exchange':
                $this->client()->deleteExchange($cmd->name);

                return new stdClass();
            case 'bind_queue':
                $this->client()->bindQueue($cmd->queue, $cmd->exchange, $cmd->key, self::map($cmd->args ?? null));

                return new stdClass();
            case 'unbind_queue':
                $this->client()->unbindQueue($cmd->queue, $cmd->exchange, $cmd->key);

                return new stdClass();
            case 'bind_exchange':
                $this->client()->bindExchange($cmd->destination, $cmd->source, $cmd->key);

                return new stdClass();
            case 'publish':
                if (isset($cmd->body_base64)) {
                    $body = base64_decode($cmd->body_base64, true);
                    if ($body === false) {
                        throw new DriverError('other', 'body_base64 is not valid base64');
                    }
                } else {
                    $body = (string) ($cmd->body ?? '');
                }
                $this->client()->publish($cmd->exchange, $cmd->key, $body, [
                    'persistent' => $cmd->persistent,
                    'mandatory' => $cmd->mandatory,
                    'properties' => self::map($cmd->properties ?? null),
                    'headers' => self::map($cmd->headers ?? null),
                ]);

                return new stdClass();
            case 'subscribe':
                $sub = $this->client()->subscribe($cmd->queue, [
                    'prefetch' => $cmd->prefetch,
                    'no_ack' => $cmd->no_ack,
                ]);
                $id = 'sub-' . $sub->id();
                $this->subs[$id] = $sub;

                return ['sub' => $id];
            case 'next':
                $sub = $this->subs[(string) $cmd->sub] ?? throw new DriverError('other', 'unknown sub ' . $cmd->sub);
                $message = $sub->next($cmd->timeout_ms / 1000);

                return ['message' => $this->message($message)];
            case 'ack':
                $this->take((string) $cmd->tag)->ack();

                return new stdClass();
            case 'nack':
                $this->take((string) $cmd->tag)->nack($cmd->requeue);

                return new stdClass();
            case 'unsubscribe':
                $id = (string) $cmd->sub;
                $sub = $this->subs[$id] ?? throw new DriverError('other', 'unknown sub ' . $id);
                $sub->cancel();
                unset($this->subs[$id]);

                return new stdClass();
            case 'get':
                return ['message' => $this->message($this->client()->get($cmd->queue, $cmd->no_ack))];
            default:
                throw new DriverError('unsupported', 'unsupported op ' . $cmd->op);
        }
    }

    public function reset(): void
    {
        $this->client = null;
        $this->subs = [];
        $this->messages = [];
    }

    private function client(): Client
    {
        return $this->client ?? throw new DriverError('closed', 'not connected');
    }

    private function take(string $tag): Message
    {
        $message = $this->messages[$tag] ?? throw new DriverError('other', 'unknown tag ' . $tag);
        unset($this->messages[$tag]);

        return $message;
    }

    /**
     * @return array<string, mixed>|null
     */
    private function message(?Message $message): ?array
    {
        if ($message === null) {
            return null;
        }
        $tag = (string) $message->deliveryTag;
        if (!$message->isSettled()) {
            $this->messages[$tag] = $message;
        }

        return [
            'body' => preg_match('//u', $message->body) === 1 ? $message->body : null,
            'body_base64' => base64_encode($message->body),
            'exchange' => $message->exchange,
            'routing_key' => $message->routingKey,
            'redelivered' => $message->redelivered,
            'tag' => $tag,
            'properties' => self::object($message->properties),
            'headers' => self::object($message->headers),
        ];
    }

    /**
     * A JSON object from the runner as a PHP array. Nested objects stay stdClass so that the
     * client encodes them as MessagePack maps, even when empty.
     *
     * @return array<string, mixed>
     */
    private static function map(mixed $value): array
    {
        return $value instanceof stdClass ? get_object_vars($value) : [];
    }

    /**
     * A decoded MessagePack map as something json_encode writes as an object.
     *
     * @param array<array-key, mixed> $map
     */
    private static function object(array $map): stdClass|array
    {
        return $map === [] || array_is_list($map) ? (object) $map : $map;
    }
}

function reply(array $reply): void
{
    $json = json_encode(
        $reply,
        JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION | JSON_INVALID_UTF8_SUBSTITUTE,
    );
    if ($json === false) {
        $json = json_encode([
            'id' => $reply['id'] ?? 0,
            'ok' => false,
            'error' => ['kind' => 'other', 'code' => 0, 'message' => 'cannot encode reply: ' . json_last_error_msg()],
        ]);
    }
    fwrite(STDOUT, $json . "\n");
    fflush(STDOUT);
}

$driver = new Driver();
while (($line = fgets(STDIN)) !== false) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }
    $id = 0;
    try {
        $cmd = json_decode($line, false, 512, JSON_THROW_ON_ERROR);
        if (!$cmd instanceof stdClass) {
            throw new DriverError('other', 'command is not a JSON object');
        }
        $id = $cmd->id ?? 0;
        reply(['id' => $id, 'ok' => true, 'result' => $driver->handle($cmd)]);
    } catch (Throwable $e) {
        [$kind, $code] = match (true) {
            $e instanceof DriverError => [$e->kind, $e->getCode()],
            $e instanceof ServerException => ['server', $e->getCode()],
            $e instanceof UnroutableException => ['unroutable', $e->getCode()],
            $e instanceof NackedException => ['nacked', $e->getCode()],
            $e instanceof ConnectionClosedException => ['closed', $e->getCode()],
            default => ['other', 0],
        };
        if (!$e instanceof DriverError) {
            fwrite(STDERR, sprintf("[driver] %s: %s\n", get_class($e), $e->getMessage()));
        }
        reply(['id' => $id, 'ok' => false, 'error' => ['kind' => $kind, 'code' => (int) $code, 'message' => $e->getMessage()]]);
    }
}
$driver->reset();
