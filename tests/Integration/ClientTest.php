<?php

declare(strict_types=1);

namespace FireMQ\Tests\Integration;

use FireMQ\Client;
use FireMQ\Exception\ConnectionClosedException;
use FireMQ\Exception\NackedException;
use FireMQ\Exception\PublishBatchException;
use FireMQ\Exception\ServerException;
use FireMQ\Exception\UnroutableException;
use FireMQ\Message;
use FireMQ\PublishOptions;
use FireMQ\QueueOptions;
use FireMQ\Subscription;
use PHPUnit\Framework\TestCase;

/**
 * Runs against a real broker. Set FIREMQ_URL, for example
 * `FIREMQ_URL=fire://guest:guest@localhost:5673 vendor/bin/phpunit --testsuite integration`.
 */
final class ClientTest extends TestCase
{
    private Client $client;

    private string $prefix;

    protected function setUp(): void
    {
        if (!self::url()) {
            $this->markTestSkipped('set FIREMQ_URL to run the integration tests');
        }
        $this->client = Client::connect(self::url(), ['connection_name' => 'firemq-php tests']);
        $this->prefix = sprintf('it.%s.%s', $this->name(), bin2hex(random_bytes(3)));
    }

    protected function tearDown(): void
    {
        if (isset($this->client) && !$this->client->isClosed()) {
            $this->client->close();
        }
    }

    public function testPublishAndGetRoundTripsPropertiesAndHeaders(): void
    {
        $queue = $this->queue();
        $headers = [
            'int' => 42,
            'negative' => -7,
            'float' => 1.5,
            'bool' => true,
            'nil' => null,
            'string' => 's',
            'list' => [1, 'two'],
            'map' => ['k' => 'v'],
        ];
        $properties = [
            'content_type' => 'application/json',
            'content_encoding' => 'identity',
            'correlation_id' => 'c-1',
            'reply_to' => 'replies',
            'expiration' => 600000,
            'message_id' => 'm-1',
            'timestamp' => 1767225600,
            'type' => 'order.created',
            'app_id' => 'shop',
            'priority' => 3,
        ];

        $this->client->publish('', $queue, '{"id":1}', ['properties' => $properties, 'headers' => $headers]);
        $message = $this->client->get($queue);

        $this->assertNotNull($message);
        $this->assertSame('{"id":1}', $message->body);
        $this->assertSame(['id' => 1], $message->json());
        $this->assertSame('', $message->exchange);
        $this->assertSame($queue, $message->routingKey);
        $this->assertFalse($message->redelivered);
        $this->assertEquals($properties, $message->properties);
        $this->assertEquals($headers, $message->headers);
        $message->ack();
        $this->assertNull($this->client->get($queue));
    }

    public function testBinaryAndLargeBodies(): void
    {
        $queue = $this->queue();
        $binary = implode('', array_map('chr', range(0, 255)));
        $large = random_bytes(1024 * 1024);

        $this->client->publish('', $queue, $binary);
        $this->client->publish('', $queue, $large);

        $this->assertSame($binary, $this->client->get($queue, true)?->body);
        $this->assertSame($large, $this->client->get($queue, true)?->body);
    }

    public function testPublishJson(): void
    {
        $queue = $this->queue();

        $this->client->publishJson('', $queue, ['id' => 7, 'price' => 2.0]);
        $message = $this->client->get($queue, true);

        $this->assertNotNull($message);
        $this->assertSame('application/json', $message->property('content_type'));
        $this->assertSame('{"id":7,"price":2.0}', $message->body);
    }

    public function testSubscribeDeliversInOrder(): void
    {
        $queue = $this->queue();
        $this->client->publishBatch(array_map(fn (int $i): array => ['', $queue, "m$i"], range(0, 99)));

        $subscription = $this->client->subscribe($queue, ['prefetch' => 10]);
        for ($i = 0; $i < 100; $i++) {
            $message = $subscription->next(5.0);
            $this->assertNotNull($message);
            $this->assertSame("m$i", $message->body);
            $message->ack();
        }
        $subscription->cancel();

        $this->assertSame(0, $this->client->purgeQueue($queue));
    }

    public function testPrefetchLimitsDeliveries(): void
    {
        $queue = $this->queue();
        for ($i = 0; $i < 4; $i++) {
            $this->client->publish('', $queue, "m$i");
        }
        $subscription = $this->client->subscribe($queue, ['prefetch' => 2]);

        $first = $subscription->next(3.0);
        $this->assertNotNull($subscription->next(3.0));
        $this->assertNull($subscription->next(0.3));
        $this->assertNotNull($first);
        $first->ack();

        $this->assertSame('m2', $subscription->next(3.0)?->body);
    }

    public function testNackRequeueRedelivers(): void
    {
        $queue = $this->queue();
        $this->client->publish('', $queue, 'again');
        $subscription = $this->client->subscribe($queue, ['prefetch' => 1]);

        $message = $subscription->next(3.0);
        $this->assertNotNull($message);
        $this->assertFalse($message->redelivered);
        $message->nack(true);

        $again = $subscription->next(3.0);
        $this->assertNotNull($again);
        $this->assertTrue($again->redelivered);
        $this->assertSame('again', $again->body);
        $again->ack();
    }

    public function testNackWithoutRequeueDeadLetters(): void
    {
        $dlx = $this->prefix . '.dlx';
        $dead = $this->queue('dead');
        $this->client->declareExchange($dlx, 'fanout', ['durable' => false, 'auto_delete' => false]);
        $this->client->bindQueue($dead, $dlx);
        $work = $this->queue('work', ['args' => ['x-dead-letter-exchange' => $dlx]]);
        $this->client->publish('', $work, 'bad');

        $this->client->get($work)?->nack(false);

        // nack has no reply, and dead-lettering happens after it, so poll briefly
        $deadline = microtime(true) + 3.0;
        do {
            $deadLetter = $this->client->get($dead, true);
        } while ($deadLetter === null && microtime(true) < $deadline && usleep(20_000) === null);
        $this->assertSame('bad', $deadLetter?->body);
        $this->assertArrayHasKey('x-death', $deadLetter->headers);
        $this->client->deleteExchange($dlx);
    }

    public function testMandatoryPublishToNowhereIsUnroutable(): void
    {
        try {
            $this->client->publish('', $this->prefix . '.nowhere', 'lost', ['mandatory' => true]);
            $this->fail('expected an UnroutableException');
        } catch (UnroutableException $e) {
            $this->assertSame(312, $e->getCode());
        }
        $this->client->publish('', $this->prefix . '.nowhere', 'dropped');
        $this->addToAssertionCount(1);
    }

    public function testFullRejectPublishQueueNacks(): void
    {
        $queue = $this->queue('full', ['args' => ['x-max-length' => 1, 'x-overflow' => 'reject-publish']]);
        $this->client->publish('', $queue, 'fits');

        $this->expectException(NackedException::class);
        $this->expectExceptionCode(406);

        $this->client->publish('', $queue, 'rejected');
    }

    public function testBatchReportsWhichMessagesFailed(): void
    {
        $queue = $this->queue('full', ['args' => ['x-max-length' => 2, 'x-overflow' => 'reject-publish']]);

        try {
            $this->client->publishBatch(array_map(fn (int $i): array => ['', $queue, "m$i"], range(0, 4)));
            $this->fail('expected a PublishBatchException');
        } catch (PublishBatchException $e) {
            $this->assertSame([2, 3, 4], array_keys($e->getFailures()));
            $this->assertContainsOnlyInstancesOf(NackedException::class, $e->getFailures());
        }
        $this->assertSame(2, $this->client->deleteQueue($queue));
    }

    public function testBatchOfManyMessages(): void
    {
        $queue = $this->queue();
        $options = new PublishOptions(persistent: false);

        $this->client->publishBatch((function () use ($queue, $options): \Generator {
            for ($i = 0; $i < 5000; $i++) {
                yield ['', $queue, str_repeat('x', 100), $options];
            }
        })());

        $this->assertSame(5000, $this->client->declareQueue($queue, ['durable' => false, 'passive' => true])->messages);
    }

    public function testTopicRouting(): void
    {
        $exchange = $this->prefix . '.topic';
        $this->client->declareExchange($exchange, 'topic', ['durable' => false]);
        $queue = $this->queue();
        $this->client->bindQueue($queue, $exchange, '*.critical.#');
        foreach (['kern.critical', 'kern.critical.disk', 'kern.info', 'critical'] as $key) {
            $this->client->publish($exchange, $key, $key);
        }

        $this->assertSame('kern.critical', $this->client->get($queue, true)?->body);
        $this->assertSame('kern.critical.disk', $this->client->get($queue, true)?->routingKey);
        $this->assertNull($this->client->get($queue, true));

        $this->client->unbindQueue($queue, $exchange, '*.critical.#');
        $this->client->publish($exchange, 'kern.critical', 'unbound');
        $this->assertNull($this->client->get($queue, true));
        $this->client->deleteExchange($exchange);
    }

    public function testExchangeToExchangeBindings(): void
    {
        $up = $this->prefix . '.up';
        $down = $this->prefix . '.down';
        $this->client->declareExchange($up, 'direct', ['durable' => false]);
        $this->client->declareExchange($down, 'fanout', ['durable' => false]);
        $queue = $this->queue();
        $this->client->bindQueue($queue, $down);
        $this->client->bindExchange($down, $up, 'audit');

        $this->client->publish($up, 'audit', 'yes');
        $this->client->publish($up, 'other', 'no');

        $message = $this->client->get($queue, true);
        $this->assertSame('yes', $message?->body);
        $this->assertSame($up, $message->exchange);
        $this->assertNull($this->client->get($queue, true));
        $this->client->deleteExchange($up);
        $this->client->deleteExchange($down);
    }

    public function testErrorsCarryAmqpCodesAndKeepTheConnection(): void
    {
        $queue = $this->queue();

        try {
            $this->client->declareQueue($queue, ['durable' => true]);
            $this->fail('expected a ServerException');
        } catch (ServerException $e) {
            $this->assertSame(406, $e->getCode());
        }
        try {
            $this->client->bindQueue($queue, $this->prefix . '.missing');
            $this->fail('expected a ServerException');
        } catch (ServerException $e) {
            $this->assertSame(404, $e->getCode());
        }

        $this->client->publish('', $queue, 'still works');
        $this->assertSame('still works', $this->client->get($queue, true)?->body);
    }

    public function testPurgeAndDeleteReportCounts(): void
    {
        $queue = $this->queue();
        $this->client->publishBatch([['', $queue, 'x'], ['', $queue, 'x'], ['', $queue, 'x']]);

        $this->assertSame(3, $this->client->purgeQueue($queue));
        $this->client->publish('', $queue, 'y');
        $this->assertSame(1, $this->client->deleteQueue($queue));
    }

    public function testServerNamedTemporaryQueueDisappearsWithItsConnection(): void
    {
        $other = Client::connect(self::url());
        $info = $other->declareQueue('', QueueOptions::temporary());
        $this->assertStringStartsWith('amq.gen-', $info->name);
        $other->close();

        try {
            $this->client->declareQueue($info->name, ['passive' => true]);
            $this->fail('expected a ServerException');
        } catch (ServerException $e) {
            $this->assertSame(404, $e->getCode());
        }
    }

    public function testUnackedMessagesReturnWhenTheConnectionCloses(): void
    {
        $queue = $this->queue();
        $this->client->publish('', $queue, 'work');
        $worker = Client::connect(self::url());
        $this->assertSame('work', $worker->subscribe($queue, ['prefetch' => 1])->next(3.0)?->body);

        $worker->close();

        $message = $this->client->get($queue, true);
        $this->assertSame('work', $message?->body);
        $this->assertTrue($message->redelivered);
    }

    public function testCancelStopsDeliveriesAndRequeuesUnseenMessages(): void
    {
        $queue = $this->queue();
        $this->client->publishBatch([['', $queue, 'a'], ['', $queue, 'b'], ['', $queue, 'c']]);
        $subscription = $this->client->subscribe($queue, ['prefetch' => 10]);
        $first = $subscription->next(3.0);
        $this->assertSame('a', $first?->body);

        $subscription->cancel();
        $first->ack();
        $this->client->publish('', $queue, 'after');

        $this->assertNull($subscription->next(0.2));
        $bodies = [];
        while (($message = $this->client->get($queue, true)) !== null) {
            $bodies[] = $message->body;
        }
        sort($bodies);
        $this->assertSame(['after', 'b', 'c'], $bodies);
    }

    public function testDeletingTheQueueEndsTheSubscription(): void
    {
        $queue = $this->queue();
        $subscription = $this->client->subscribe($queue);

        $this->client->deleteQueue($queue);

        $this->assertNull($subscription->next(3.0));
        $this->assertFalse($subscription->isActive());
        $this->assertNotNull($subscription->cancelReason());
    }

    public function testConsumeLoop(): void
    {
        $queue = $this->queue();
        $this->client->publishBatch(array_map(fn (int $i): array => ['', $queue, "task $i"], range(1, 5)));
        $done = [];

        $this->client->consume($queue, function (Message $task, Subscription $subscription) use (&$done): bool {
            $done[] = $task->body;

            return count($done) < 5;
        }, ['prefetch' => 2]);

        $this->assertSame(['task 1', 'task 2', 'task 3', 'task 4', 'task 5'], $done);
        $this->assertNull($this->client->get($queue));
    }

    public function testConsumeRequeuesWhenTheHandlerFails(): void
    {
        $queue = $this->queue();
        $this->client->publish('', $queue, 'poison');

        try {
            $this->client->consume($queue, static function (): void {
                throw new \RuntimeException('handler failed');
            });
            $this->fail('expected the handler exception');
        } catch (\RuntimeException $e) {
            $this->assertSame('handler failed', $e->getMessage());
        }

        $message = $this->client->get($queue, true);
        $this->assertSame('poison', $message?->body);
        $this->assertTrue($message->redelivered);
    }

    public function testWrongPasswordIsRefused(): void
    {
        $url = (string) preg_replace('#//([^:/@]+)(:[^@]*)?@#', '//$1:wrong-password@', self::url());
        if (!str_contains($url, 'wrong-password')) {
            $url = str_replace('://', '://guest:wrong-password@', self::url());
        }

        try {
            Client::connect($url);
            $this->fail('expected a ServerException');
        } catch (ServerException $e) {
            $this->assertSame(403, $e->getCode());
        }
    }

    public function testClosedClientThrows(): void
    {
        $this->client->close();

        $this->assertTrue($this->client->isClosed());
        $this->expectException(ConnectionClosedException::class);

        $this->client->declareQueue($this->prefix);
    }

    public function testServerInfo(): void
    {
        $info = $this->client->serverInfo();

        $this->assertSame(1, $info['ver']);
        $this->assertSame('firemq', $info['server']['name']);
        $this->assertContains('confirms', $info['caps']);
    }

    private static function url(): string
    {
        return (string) getenv('FIREMQ_URL');
    }

    /**
     * Declares a non-durable queue for this test. Queue names end in `.php`.
     *
     * @param array<string, mixed> $options
     */
    private function queue(string $name = 'q', array $options = []): string
    {
        $queue = sprintf('%s.%s.php', $this->prefix, $name);

        return $this->client->declareQueue($queue, ['durable' => false] + $options)->name;
    }
}
