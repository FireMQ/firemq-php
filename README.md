# firemq-php

PHP client for [FireMQ](https://github.com/FireMQ/firemq), a RabbitMQ-compatible message broker. It speaks the native [FireMQ protocol](https://github.com/FireMQ/firemq/blob/main/spec/fire-protocol.md): length-prefixed frames with MessagePack headers and raw message bodies.

The client is pure PHP and synchronous. Every call blocks until the broker answers, which fits PHP's request model: publish from a web request, or run a long-lived worker with `consume()`.

Defaults favour safety: queues and exchanges are durable, messages are persistent, and `publish()` returns only after the broker confirms the message.

## Requirements

- PHP 8.1 or later.
- The `openssl` extension, only for TLS (`fires://`) connections.
- A FireMQ broker with the FireMQ protocol listener enabled (port 5673 by default).

## Install

```bash
composer require firemq/firemq
```

The only runtime dependency is [`rybakit/msgpack`](https://packagist.org/packages/rybakit/msgpack), a pure-PHP MessagePack library.

## Quickstart

Start a broker, then publish a message and read it back:

```php
<?php

require 'vendor/autoload.php';

use FireMQ\Client;

$client = Client::connect('fire://guest:guest@localhost:5673');
$client->declareQueue('jobs');

// Returns once the broker has confirmed the message.
$client->publish('', 'jobs', 'hello');

$message = $client->get('jobs');
echo $message?->body, "\n"; // hello
$message?->ack();

$client->close();
```

The empty exchange name `''` is the default exchange, which routes a message straight to the queue named by the routing key.

## Run a worker

`consume()` subscribes to a queue and calls your handler for each message. A message is acknowledged when the handler returns. If the handler throws, the message is requeued, the subscription is cancelled, and the exception is rethrown.

```php
use FireMQ\Client;
use FireMQ\Message;

$client = Client::connect(getenv('FIREMQ_URL') ?: 'fire://localhost');
$client->declareQueue('emails');

$client->consume('emails', function (Message $message): void {
    $job = $message->json();
    send_email($job['to'], $job['subject']);
}, ['prefetch' => 10]);
```

`consume()` runs until one of these happens:

- The handler returns `false`.
- The handler calls `$subscription->cancel()` on the subscription it receives as the second argument.
- The broker ends the subscription, for example because the queue was deleted.

To settle a message yourself, call `$message->ack()` or `$message->nack($requeue)` in the handler. The client doesn't acknowledge a message twice.

### Heartbeats and long-running handlers

The client sends heartbeats while it waits for messages, and answers the broker's pings whenever it reads from the socket. PHP runs one thing at a time, so no heartbeats are sent while your handler runs. The broker closes a connection that has been silent for twice the heartbeat interval (60 seconds with the default of 30).

If a handler can run longer than that, connect with a longer interval, or with `'heartbeat' => 0` to turn heartbeats off:

```php
$client = Client::connect($url, ['heartbeat' => 300]);
```

## Connect

```php
$client = Client::connect('fire://user:password@host:5673/vhost', $options);
```

| URL part | Default |
|---|---|
| Scheme | `fire://` for TCP, `fires://` for TLS |
| User and password | `guest` and `guest` |
| Port | 5673 (`fire`), 5674 (`fires`) |
| Path | The vhost, percent-encoded. Empty means `/`. |

Percent-encode special characters in the user, password and vhost. For example, `fire://app:p%40ss@mq.example.com/prod%2Feu` connects as `app` with password `p@ss` to vhost `prod/eu`.

| Option | Default | Meaning |
|---|---|---|
| `heartbeat` | `30` | Heartbeat interval in seconds. `0` disables heartbeats. |
| `connection_name` | `''` | Name shown in the management UI. |
| `connect_timeout` | `10.0` | Seconds to open the socket and finish the handshake. |
| `timeout` | `30.0` | Seconds to wait for a reply or a publish confirm. `null` waits forever. |
| `tls_ca_file` | `null` | PEM file with an extra trusted CA certificate for `fires://`. |
| `tls_insecure` | `false` | Skip certificate verification. Only for local development. |
| `tls` | `[]` | Extra [`ssl` context options](https://www.php.net/manual/en/context.ssl.php), applied last. |
| `on_error` | `null` | Callable that receives a `ServerException` for broker errors not tied to a request, such as acknowledging an unknown delivery tag. Don't call the client from it. |

Call `$client->close()` when you're done. Unacknowledged messages return to their queues when a connection closes. A client that is garbage collected sends `close` without waiting for the broker's reply.

## API

### Queues and exchanges

```php
use FireMQ\QueueOptions;

// Durable by default. Returns a QueueInfo with name, messages and consumers.
$info = $client->declareQueue('orders', ['args' => ['x-message-ttl' => 60000]]);

// A server-named queue that is deleted when this connection closes.
$replies = $client->declareQueue('', QueueOptions::temporary());
echo $replies->name; // amq.gen-...

$count = $client->purgeQueue('orders');   // messages removed
$count = $client->deleteQueue('orders');  // messages deleted with the queue

$client->declareExchange('events', 'topic');           // direct, fanout, topic or headers
$client->bindQueue('audit', 'events', 'order.*');      // queue, exchange, routing key or pattern
$client->unbindQueue('audit', 'events', 'order.*');
$client->bindExchange('archive', 'events', '#');       // destination exchange, source exchange, key
$client->unbindExchange('archive', 'events', '#');
$client->deleteExchange('events');
```

Options accept an options object or an array. Array keys can be camelCase or snake_case:

| Class | Options |
|---|---|
| `QueueOptions` | `durable` (true), `exclusive`, `autoDelete`, `passive`, `args` |
| `ExchangeOptions` | `durable` (true), `autoDelete`, `internal`, `passive`, `args` |
| `PublishOptions` | `persistent` (true), `mandatory`, `confirm` (true), `properties`, `headers` |
| `SubscribeOptions` | `prefetch` (100), `noAck`, `exclusive`, `tag`, `args` |

Queue `args` use RabbitMQ's names, such as `x-message-ttl`, `x-max-length`, `x-overflow`, `x-dead-letter-exchange` and `x-max-priority`.

### Publish

```php
$client->publish('events', 'order.created', $body, [
    'mandatory' => true,
    'properties' => [
        'content_type' => 'application/json',
        'correlation_id' => 'req-1',
        'reply_to' => 'replies',
        'expiration' => 60000,       // milliseconds
    ],
    'headers' => ['attempt' => 1, 'tags' => ['new', 'eu']],
]);

$client->publishJson('events', 'order.created', ['id' => 42]); // sets content_type
```

`publish()` returns after the broker confirms the message. With `'confirm' => false` it returns once the message is written to the socket.

Message properties use the protocol's names: `content_type`, `content_encoding`, `correlation_id`, `reply_to`, `expiration` (milliseconds), `message_id`, `timestamp` (Unix seconds or a `DateTimeInterface`), `type`, `user_id`, `app_id` and `priority` (0 to 255). Unknown property names throw `InvalidArgumentException`.

Header values can be strings, integers, floats, booleans, `null`, lists and maps. A PHP list becomes a MessagePack array. Use `new \stdClass()` for an empty map.

To publish many messages, use `publishBatch()`. It writes all messages back to back and waits for the confirms once, which is much faster than calling `publish()` in a loop:

```php
$client->publishBatch([
    ['', 'jobs', 'first'],
    ['', 'jobs', 'second', ['headers' => ['attempt' => 1]]],
]);
```

Each message is `[exchange, routingKey, body]` or `[exchange, routingKey, body, options]`. The input can be any iterable, such as a generator. When some messages fail, `publishBatch()` throws a `PublishBatchException` after every message is settled. Its `getFailures()` method maps each failed message's key in the input to its exception.

### Subscribe

```php
$subscription = $client->subscribe('jobs', ['prefetch' => 10]);

while (($message = $subscription->next(5.0)) !== null) {
    process($message->body);
    $message->ack();
}

$subscription->cancel();
```

`next($timeoutSeconds)` returns the next message, or `null` when the timeout passes or the subscription has ended. Without a timeout it waits as long as it takes.

The broker sends at most `prefetch` unacknowledged messages. The client grants more in batches of half that number as you acknowledge messages, or as `next()` returns them when `noAck` is true.

`cancel()` stops the subscription. You can still acknowledge messages that `next()` already returned. Messages that arrived but weren't returned yet are requeued.

`isActive()` tells you whether the subscription is still running, and `cancelReason()` returns the broker's reason when the broker ended it.

### Get one message

```php
$message = $client->get('jobs');          // null when the queue is empty
$message?->ack();

$message = $client->get('jobs', true);    // no ack needed
```

### Messages

| Member | Meaning |
|---|---|
| `body` | The body, as a string of bytes. |
| `exchange`, `routingKey` | Where the message was published. |
| `properties`, `headers` | Arrays with the message properties and application headers. |
| `redelivered` | `true` when the message was delivered before and not acknowledged. |
| `deliveryTag` | The broker's delivery tag. |
| `ack()` | Acknowledges the message. |
| `nack(bool $requeue)` | Rejects the message. With `$requeue = false`, the message goes to the queue's dead-letter exchange, or is dropped when there is none. |
| `json()` | Decodes a JSON body. |
| `property($name, $default = null)` | Returns one property. |

`ack()` and `nack()` do nothing for messages received with `noAck`, or when the message is already settled. You can create a `Message` in your own tests, for example `new Message('{"id":1}')`, to unit test a handler.

## Errors

Every exception the client throws extends `FireMQ\Exception\FireMQException`.

| Exception | When | `getCode()` |
|---|---|---|
| `ServerException` | The broker refused a request, such as a redeclare with other properties, a missing queue or exchange, or bad credentials. | AMQP reply code: 403, 404, 405, 406, ... |
| `UnroutableException` | A `mandatory` publish reached no queue. | 312 |
| `NackedException` | The broker rejected a publish, for example because a queue with `x-overflow: reject-publish` is full. | AMQP reply code |
| `PublishBatchException` | One or more messages of `publishBatch()` failed. | Code of the first failure |
| `ConnectionClosedException` | The connection couldn't be opened, or it closed. | Broker's code when it sent one, otherwise 0 |
| `TimeoutException` | No reply or confirm arrived within the `timeout` option. | 0 |
| `ProtocolException` | The broker sent something the client can't decode. | 0 |

A `ServerException` for a request doesn't close the connection, so you can keep using the client:

```php
use FireMQ\Exception\ServerException;

try {
    $client->declareQueue('orders', ['durable' => false]);
} catch (ServerException $e) {
    if ($e->getCode() === 406) {
        // The queue exists with other properties.
    }
}
```

## TLS

Use a `fires://` URL. The client verifies the broker's certificate against the system's trusted CAs:

```php
$client = Client::connect('fires://app:secret@mq.example.com');
```

For a private CA or a self-signed certificate, pass the CA certificate:

```php
$client = Client::connect('fires://app:secret@mq.internal', ['tls_ca_file' => '/etc/firemq/ca.pem']);
```

For local development only, you can skip verification with `'tls_insecure' => true`. Other settings, such as a client certificate, go in the `tls` option as PHP `ssl` context options:

```php
$client = Client::connect($url, ['tls' => ['local_cert' => '/etc/firemq/client.pem']]);
```

The PHP client doesn't support the WebSocket transport (`ws://` and `wss://`), which is meant for browsers.

## Examples

The [`examples`](examples) directory has the classic tutorials: `hello_world.php`, `work_queues.php`, `publish_subscribe.php`, `routing.php`, `topics.php` and `rpc.php`. Run them against a local broker:

```bash
php examples/hello_world.php
```

Set `FIREMQ_URL` to connect somewhere other than `fire://localhost`.

## Development

```bash
composer install
vendor/bin/phpunit --testsuite unit
vendor/bin/phpstan analyse
```

The unit tests decode every frame in the protocol's [test vectors](tests/fixtures/frames.json) and drive the client against a scripted broker. The integration tests need a running broker and are skipped unless `FIREMQ_URL` is set:

```bash
FIREMQ_URL=fire://guest:guest@localhost:5673 vendor/bin/phpunit --testsuite integration
```

The [conformance driver](conformance/driver.php) runs FireMQ's SDK conformance suite. Build `firemq-conformance` from the FireMQ repository, then run:

```bash
firemq-conformance run --driver "php conformance/driver.php"
```

## License

Apache-2.0. See [LICENSE](LICENSE).
