<?php

declare(strict_types=1);

namespace FireMQ;

use FireMQ\Exception\ConnectionClosedException;
use FireMQ\Exception\FireMQException;
use FireMQ\Exception\NackedException;
use FireMQ\Exception\PublishBatchException;
use FireMQ\Exception\ServerException;
use FireMQ\Exception\TimeoutException;
use FireMQ\Exception\UnroutableException;
use FireMQ\Protocol\ConnectUrl;
use FireMQ\Protocol\Op;

/**
 * A connection to FireMQ over the native FireMQ protocol.
 *
 * The client is synchronous: every call blocks until the broker answers. Defaults favour
 * safety: queues and exchanges are durable, messages are persistent, and publish() returns
 * once the broker has confirmed the message.
 *
 *     $client = Client::connect('fire://guest:guest@localhost:5673');
 *     $client->declareQueue('jobs');
 *     $client->publish('', 'jobs', 'hello');
 *     $client->consume('jobs', function (Message $m) { echo $m->body, "\n"; });
 */
final class Client
{
    public const VERSION = '0.1.0';

    private const DEFAULT_OPTIONS = [
        'heartbeat' => 30,
        'connection_name' => '',
        'connect_timeout' => 10.0,
        'timeout' => 30.0,
        'tls_ca_file' => null,
        'tls_insecure' => false,
        'tls' => [],
        'on_error' => null,
    ];

    private function __construct(private readonly Connection $connection)
    {
    }

    /**
     * Connects with a URL such as `fire://user:pass@host:5673/vhost` or `fires://...` for TLS.
     *
     * Options:
     * - `heartbeat` (int, 30): heartbeat interval in seconds; 0 disables heartbeats.
     * - `connection_name` (string): shown in the management UI.
     * - `connect_timeout` (float, 10): seconds to open the socket and finish the handshake.
     * - `timeout` (float|null, 30): seconds to wait for a reply or a confirm; null waits forever.
     * - `tls_ca_file` (string): extra trusted CA certificate (PEM file) for `fires://`.
     * - `tls_insecure` (bool, false): skip certificate verification. Only for local development.
     * - `tls` (array): extra `ssl` stream context options, merged last.
     * - `on_error` (callable): called with a ServerException for broker errors that are not
     *   tied to a request, such as acknowledging an unknown delivery tag. Do not call the
     *   client from it.
     *
     * @param array<string, mixed> $options
     *
     * @throws ServerException when the broker refuses the connection, for example with 403
     * @throws ConnectionClosedException when the broker cannot be reached
     */
    public static function connect(string $url = 'fire://localhost', array $options = []): self
    {
        $unknown = array_diff_key($options, self::DEFAULT_OPTIONS);
        if ($unknown !== []) {
            throw new \InvalidArgumentException(sprintf(
                'unknown connect option "%s"; use %s',
                (string) array_key_first($unknown),
                implode(', ', array_keys(self::DEFAULT_OPTIONS)),
            ));
        }
        $options += self::DEFAULT_OPTIONS;
        $timeout = $options['timeout'];
        if ($timeout !== null && (!is_numeric($timeout) || $timeout <= 0)) {
            throw new \InvalidArgumentException('timeout must be a positive number of seconds, or null');
        }
        $onError = $options['on_error'];
        if ($onError !== null && !is_callable($onError)) {
            throw new \InvalidArgumentException('on_error must be callable');
        }
        $tls = $options['tls'];
        if (!is_array($tls)) {
            throw new \InvalidArgumentException('tls must be an array of ssl context options');
        }
        $caFile = $options['tls_ca_file'];

        return new self(Connection::open(ConnectUrl::parse($url), [
            'heartbeat' => max(0, (int) $options['heartbeat']),
            'connection_name' => (string) $options['connection_name'],
            'connect_timeout' => max(0.001, (float) $options['connect_timeout']),
            'timeout' => $timeout === null ? null : (float) $timeout,
            'tls_ca_file' => $caFile === null ? null : (string) $caFile,
            'tls_insecure' => (bool) $options['tls_insecure'],
            'tls' => $tls,
            'on_error' => $onError,
        ]));
    }

    /**
     * Server name, version and limits from the handshake: `server`, `conn_id`, `heartbeat`,
     * `max_frame` and `caps`.
     *
     * @return array<string, mixed>
     */
    public function serverInfo(): array
    {
        return $this->connection->welcome();
    }

    /**
     * Declares a queue. An empty name asks the broker to generate one.
     *
     * @param QueueOptions|array<string, mixed> $options
     *
     * @throws ServerException for example 406 when the queue exists with other properties
     */
    public function declareQueue(string $name, QueueOptions|array $options = []): QueueInfo
    {
        $options = QueueOptions::from($options);
        $ok = $this->connection->call(Op::DECLARE_QUEUE, self::compact([
            'name' => $name,
            'durable' => $options->durable,
            'exclusive' => $options->exclusive,
            'auto_delete' => $options->autoDelete,
            'passive' => $options->passive,
            'args' => $options->args,
        ]))->header;

        return new QueueInfo(
            (string) ($ok['name'] ?? $name),
            (int) ($ok['messages'] ?? 0),
            (int) ($ok['consumers'] ?? 0),
        );
    }

    /**
     * Deletes a queue and returns how many messages it held.
     */
    public function deleteQueue(string $name, bool $ifUnused = false, bool $ifEmpty = false): int
    {
        $ok = $this->connection->call(Op::DELETE_QUEUE, self::compact([
            'name' => $name,
            'if_unused' => $ifUnused,
            'if_empty' => $ifEmpty,
        ]))->header;

        return (int) ($ok['messages'] ?? 0);
    }

    /**
     * Removes every ready message from a queue and returns how many were removed.
     */
    public function purgeQueue(string $name): int
    {
        $ok = $this->connection->call(Op::PURGE_QUEUE, ['name' => $name])->header;

        return (int) ($ok['messages'] ?? 0);
    }

    /**
     * Declares an exchange of type `direct`, `fanout`, `topic` or `headers`.
     *
     * @param ExchangeOptions|array<string, mixed> $options
     */
    public function declareExchange(string $name, string $type = 'direct', ExchangeOptions|array $options = []): void
    {
        $options = ExchangeOptions::from($options);
        $this->connection->call(Op::DECLARE_EXCHANGE, self::compact([
            'name' => $name,
            'type' => $type,
            'durable' => $options->durable,
            'auto_delete' => $options->autoDelete,
            'internal' => $options->internal,
            'passive' => $options->passive,
            'args' => $options->args,
        ]));
    }

    public function deleteExchange(string $name, bool $ifUnused = false): void
    {
        $this->connection->call(Op::DELETE_EXCHANGE, self::compact(['name' => $name, 'if_unused' => $ifUnused]));
    }

    /**
     * Binds a queue to an exchange with a routing key or pattern. $args holds binding
     * arguments, for example `x-match` for headers exchanges.
     *
     * @param array<string, mixed> $args
     */
    public function bindQueue(string $queue, string $exchange, string $key = '', array $args = []): void
    {
        $this->connection->call(Op::BIND, self::compact(['exchange' => $exchange, 'queue' => $queue, 'key' => $key, 'args' => $args]));
    }

    /**
     * @param array<string, mixed> $args
     */
    public function unbindQueue(string $queue, string $exchange, string $key = '', array $args = []): void
    {
        $this->connection->call(Op::UNBIND, self::compact(['exchange' => $exchange, 'queue' => $queue, 'key' => $key, 'args' => $args]));
    }

    /**
     * Binds exchange $destination to exchange $source: messages routed by $source with a
     * matching key also go to $destination.
     *
     * @param array<string, mixed> $args
     */
    public function bindExchange(string $destination, string $source, string $key = '', array $args = []): void
    {
        $this->connection->call(Op::BIND, self::compact(['exchange' => $source, 'to_exchange' => $destination, 'key' => $key, 'args' => $args]));
    }

    /**
     * @param array<string, mixed> $args
     */
    public function unbindExchange(string $destination, string $source, string $key = '', array $args = []): void
    {
        $this->connection->call(Op::UNBIND, self::compact(['exchange' => $source, 'to_exchange' => $destination, 'key' => $key, 'args' => $args]));
    }

    /**
     * Publishes a message and returns once the broker has confirmed it. Use exchange `''`
     * to send straight to the queue named by $routingKey.
     *
     * @param PublishOptions|array<string, mixed> $options
     *
     * @throws UnroutableException when the publish is mandatory and no queue received it
     * @throws NackedException when the broker rejected the message
     * @throws TimeoutException when no confirm arrived in time
     */
    public function publish(string $exchange, string $routingKey, string $body, PublishOptions|array $options = []): void
    {
        $this->connection->publish($exchange, $routingKey, $body, PublishOptions::from($options));
    }

    /**
     * Publishes a JSON-encoded value with content type `application/json`.
     *
     * @param PublishOptions|array<string, mixed> $options
     */
    public function publishJson(string $exchange, string $routingKey, mixed $value, PublishOptions|array $options = []): void
    {
        $options = PublishOptions::from($options);
        $options = new PublishOptions(
            persistent: $options->persistent,
            mandatory: $options->mandatory,
            confirm: $options->confirm,
            properties: $options->properties + ['content_type' => 'application/json'],
            headers: $options->headers,
        );
        try {
            $body = json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION);
        } catch (\JsonException $e) {
            throw new \InvalidArgumentException('cannot encode the value as JSON: ' . $e->getMessage(), 0, $e);
        }
        $this->connection->publish($exchange, $routingKey, $body, $options);
    }

    /**
     * Publishes many messages back to back and waits for all confirms at once, which is much
     * faster than one publish() per message.
     *
     * Each message is a list `[exchange, routingKey, body]` or
     * `[exchange, routingKey, body, options]`:
     *
     *     $client->publishBatch([
     *         ['', 'jobs', 'first'],
     *         ['', 'jobs', 'second', ['headers' => ['attempt' => 1]]],
     *     ]);
     *
     * @param iterable<array-key, array{0: string, 1: string, 2: string, 3?: PublishOptions|array<string, mixed>}> $messages
     *
     * @throws PublishBatchException when one or more messages failed; getFailures() maps the
     *                               message's key in $messages to its exception
     */
    public function publishBatch(iterable $messages): void
    {
        $batch = [];
        foreach ($messages as $index => $message) {
            if (!is_array($message) || !isset($message[0], $message[1], $message[2])) {
                throw new \InvalidArgumentException(sprintf('message %s must be [exchange, routingKey, body] or [exchange, routingKey, body, options]', $index));
            }
            $batch[$index] = [(string) $message[0], (string) $message[1], (string) $message[2], PublishOptions::from($message[3] ?? [])];
        }
        if ($batch === []) {
            return;
        }
        $failures = $this->connection->publishBatch($batch);
        if ($failures !== []) {
            throw new PublishBatchException($failures, count($batch));
        }
    }

    /**
     * Starts consuming from a queue. Read messages with Subscription::next().
     *
     * @param SubscribeOptions|array<string, mixed> $options
     */
    public function subscribe(string $queue, SubscribeOptions|array $options = []): Subscription
    {
        return $this->connection->subscribe($queue, SubscribeOptions::from($options));
    }

    /**
     * Consumes messages and calls $handler for each one, until the handler returns false,
     * the handler cancels the subscription, or the broker ends it (for example because the
     * queue was deleted).
     *
     * The handler receives the Message and the Subscription. A message the handler did not
     * settle is acknowledged when the handler returns. When the handler throws, the message
     * is requeued with nack(true), the subscription is cancelled and the exception is
     * rethrown.
     *
     * While it waits for messages, consume() answers and sends heartbeats. Heartbeats are
     * not sent while the handler runs, so a handler that runs longer than twice the
     * heartbeat interval needs a longer `heartbeat` (or 0) in Client::connect().
     *
     * @param callable(Message, Subscription): mixed $handler
     * @param SubscribeOptions|array<string, mixed> $options
     */
    public function consume(string $queue, callable $handler, SubscribeOptions|array $options = []): void
    {
        $subscription = $this->subscribe($queue, $options);
        try {
            while (($message = $subscription->next()) !== null) {
                try {
                    $result = $handler($message, $subscription);
                } catch (\Throwable $e) {
                    if (!$message->isSettled()) {
                        try {
                            $message->nack(true);
                        } catch (FireMQException) {
                            // the connection is gone; the broker requeues the message
                        }
                    }
                    throw $e;
                }
                if (!$message->isSettled()) {
                    $message->ack();
                }
                if ($result === false || !$subscription->isActive()) {
                    break;
                }
            }
        } finally {
            if ($subscription->isActive()) {
                try {
                    $subscription->cancel();
                } catch (FireMQException) {
                    // keep the original outcome
                }
            }
        }
    }

    /**
     * Takes one message from a queue, or returns null when it is empty. Unless $noAck is
     * true, acknowledge the message with Message::ack().
     */
    public function get(string $queue, bool $noAck = false): ?Message
    {
        return $this->connection->get($queue, $noAck);
    }

    /**
     * Closes the connection gracefully. Unacknowledged messages return to their queues.
     * Calling close() again does nothing.
     */
    public function close(): void
    {
        $this->connection->close();
    }

    public function isClosed(): bool
    {
        return $this->connection->isClosed();
    }

    /**
     * Drops fields with zero values, which the protocol treats as omitted.
     *
     * @param array<string, mixed> $header
     *
     * @return array<string, mixed>
     */
    private static function compact(array $header): array
    {
        return array_filter($header, static fn (mixed $v): bool => $v !== false && $v !== 0 && $v !== '' && $v !== []);
    }
}
