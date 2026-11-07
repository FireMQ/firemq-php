<?php

declare(strict_types=1);

namespace FireMQ\Tests\Unit;

use FireMQ\Connection;
use FireMQ\Exception\ConnectionClosedException;
use FireMQ\Exception\FireMQException;
use FireMQ\Exception\NackedException;
use FireMQ\Exception\PublishBatchException;
use FireMQ\Exception\ServerException;
use FireMQ\Exception\TimeoutException;
use FireMQ\Exception\UnroutableException;
use FireMQ\Message;
use FireMQ\Protocol\ConnectUrl;
use FireMQ\Protocol\Frame;
use FireMQ\Protocol\Op;
use FireMQ\PublishOptions;
use FireMQ\SubscribeOptions;
use PHPUnit\Framework\TestCase;

/**
 * Drives the client against a scripted broker on a socket pair.
 */
final class ConnectionTest extends TestCase
{
    private FakeBroker $broker;

    protected function setUp(): void
    {
        $this->broker = new FakeBroker();
    }

    public function testHandshakeSendsMagicAndHello(): void
    {
        $this->broker->welcome(heartbeat: 15);
        $connection = Connection::fromStream($this->broker->client, ConnectUrl::parse('fire://app:secret@localhost/prod'), 20);

        $frames = $this->broker->frames();
        $this->assertCount(1, $frames);
        $hello = $frames[0]->header;
        $this->assertSame(Op::HELLO, $frames[0]->op);
        $this->assertSame(1, $hello['ver']);
        $this->assertSame('prod', $hello['vhost']);
        $this->assertSame(['mech' => 'plain', 'user' => 'app', 'pass' => 'secret'], $hello['auth']);
        $this->assertSame(20, $hello['heartbeat']);
        $this->assertSame('firemq-php', $hello['client']['name']);
        $this->assertSame(['confirms', 'credit'], $hello['caps']);
        $this->assertSame(15, $connection->welcome()['heartbeat']);
    }

    public function testRefusedLoginFailsWithTheServerCode(): void
    {
        $this->broker->send(Op::ERROR, ['code' => 403, 'msg' => 'ACCESS_REFUSED', 'fatal' => true]);

        try {
            Connection::fromStream($this->broker->client, ConnectUrl::parse('fire://localhost'));
            $this->fail('expected a ServerException');
        } catch (ServerException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString('ACCESS_REFUSED', $e->getMessage());
        }
    }

    public function testPublishWaitsForTheConfirm(): void
    {
        $connection = $this->broker->connection();
        $this->broker->send(Op::CONFIRM, ['upto' => 1]);

        $connection->publish('orders', 'order.created', '{"id":1}', new PublishOptions(properties: ['content_type' => 'application/json'], headers: ['n' => 1]));

        [$publish] = $this->broker->frames();
        $this->assertSame(Op::PUBLISH, $publish->op);
        $this->assertSame([
            'seq' => 1,
            'exchange' => 'orders',
            'key' => 'order.created',
            'persistent' => true,
            'props' => ['content_type' => 'application/json'],
            'headers' => ['n' => 1],
        ], $publish->header);
        $this->assertSame('{"id":1}', $publish->payload);
    }

    public function testPublishWithoutConfirmSendsSeqZero(): void
    {
        $connection = $this->broker->connection();

        $connection->publish('', 'q', 'x', new PublishOptions(confirm: false, persistent: false));

        [$publish] = $this->broker->frames();
        $this->assertSame(['key' => 'q'], $publish->header);
    }

    public function testReturnBeforeConfirmFailsTheMandatoryPublish(): void
    {
        $connection = $this->broker->connection();
        $this->broker
            ->send(Op::RETURN, ['seq' => 1, 'code' => 312, 'msg' => 'NO_ROUTE', 'exchange' => 'ex', 'key' => 'nowhere'], 'x')
            ->send(Op::CONFIRM, ['upto' => 1])
            ->send(Op::CONFIRM, ['upto' => 2]);

        try {
            $connection->publish('ex', 'nowhere', 'x', new PublishOptions(mandatory: true));
            $this->fail('expected an UnroutableException');
        } catch (UnroutableException $e) {
            $this->assertSame(312, $e->getCode());
            $this->assertSame('ex', $e->exchange);
            $this->assertSame('nowhere', $e->routingKey);
        }
        $connection->publish('ex', 'somewhere', 'y', new PublishOptions(mandatory: true));
        $this->assertTrue($this->broker->framesOf(Op::PUBLISH)[0]->header['mandatory']);
    }

    public function testNackFailsThePublish(): void
    {
        $connection = $this->broker->connection();
        $this->broker
            ->send(Op::CONFIRM, ['seq' => 1, 'nack' => true, 'code' => 406, 'msg' => "queue 'jobs' is full"])
            ->send(Op::CONFIRM, ['upto' => 1]);

        $this->expectException(NackedException::class);
        $this->expectExceptionCode(406);

        $connection->publish('', 'jobs', 'x', new PublishOptions());
    }

    public function testBatchUsesCumulativeConfirmsAndReportsIndividualNacks(): void
    {
        $connection = $this->broker->connection();
        $this->broker
            ->send(Op::CONFIRM, ['seq' => 2, 'nack' => true, 'code' => 406, 'msg' => 'full'])
            ->send(Op::CONFIRM, ['upto' => 3]);
        $options = new PublishOptions();

        $failures = $connection->publishBatch([
            'a' => ['', 'q', '1', $options],
            'b' => ['', 'q', '2', $options],
            'c' => ['', 'q', '3', $options],
        ]);

        $this->assertSame(['b'], array_keys($failures));
        $this->assertInstanceOf(NackedException::class, $failures['b']);
        $this->assertSame([1, 2, 3], array_map(static fn (Frame $f): int => $f->header['seq'], $this->broker->frames()));
    }

    public function testClientBatchThrowsWithFailures(): void
    {
        $client = $this->broker->client();
        $this->broker
            ->send(Op::RETURN, ['seq' => 1, 'code' => 312, 'msg' => 'NO_ROUTE', 'key' => 'k'])
            ->send(Op::CONFIRM, ['upto' => 2]);

        try {
            $client->publishBatch([['', 'k', 'x', ['mandatory' => true]], ['', 'q', 'y']]);
            $this->fail('expected a PublishBatchException');
        } catch (PublishBatchException $e) {
            $this->assertSame([0], array_keys($e->getFailures()));
            $this->assertInstanceOf(UnroutableException::class, $e->getPrevious());
        }
    }

    public function testRequestErrorsCarryTheServerCodeAndKeepTheConnection(): void
    {
        $client = $this->broker->client();
        $this->broker
            ->send(Op::ERROR, ['rid' => 1, 'code' => 404, 'msg' => "NOT_FOUND - no exchange 'missing'"])
            ->send(Op::OK, ['rid' => 2, 'name' => 'jobs', 'messages' => 3, 'consumers' => 1]);

        try {
            $client->bindQueue('jobs', 'missing', 'k');
            $this->fail('expected a ServerException');
        } catch (ServerException $e) {
            $this->assertSame(404, $e->getCode());
        }
        $info = $client->declareQueue('jobs');

        $this->assertSame(['jobs', 3, 1], [$info->name, $info->messages, $info->consumers]);
        [$bind, $declare] = $this->broker->frames();
        $this->assertSame(['rid' => 1, 'exchange' => 'missing', 'queue' => 'jobs', 'key' => 'k'], $bind->header);
        $this->assertSame(['rid' => 2, 'name' => 'jobs', 'durable' => true], $declare->header);
    }

    public function testExchangeBindingsUseToExchange(): void
    {
        $client = $this->broker->client();
        $this->broker->send(Op::OK, ['rid' => 1])->send(Op::OK, ['rid' => 2]);

        $client->bindExchange('audit', 'orders', '#');
        $client->unbindExchange('audit', 'orders', '#');

        [$bind, $unbind] = $this->broker->frames();
        $this->assertSame([Op::BIND, Op::UNBIND], [$bind->op, $unbind->op]);
        $this->assertSame(['rid' => 1, 'exchange' => 'orders', 'to_exchange' => 'audit', 'key' => '#'], $bind->header);
    }

    public function testPingIsAnsweredWithPong(): void
    {
        $client = $this->broker->client();
        $this->broker->send(Op::PING)->send(Op::OK, ['rid' => 1, 'messages' => 0]);

        $client->purgeQueue('jobs');

        $this->assertSame([Op::PURGE_QUEUE, Op::PONG], array_map(static fn (Frame $f): int => $f->op, $this->broker->frames()));
    }

    public function testHeartbeatsAreSentWhileWaiting(): void
    {
        $client = $this->broker->client(heartbeat: 1);
        $this->broker->send(Op::OK, ['rid' => 1]);
        $subscription = $client->subscribe('jobs');

        $this->assertNull($subscription->next(0.8));

        $this->assertCount(1, $this->broker->framesOf(Op::PING));
    }

    public function testMissedHeartbeatsCloseTheConnection(): void
    {
        $client = $this->broker->client(heartbeat: 1);
        $this->broker->send(Op::OK, ['rid' => 1]);
        $subscription = $client->subscribe('jobs');

        $this->expectException(ConnectionClosedException::class);
        $this->expectExceptionMessage('missed heartbeats');

        $subscription->next(3.0);
    }

    public function testCreditIsToppedUpInHalfWindows(): void
    {
        $client = $this->broker->client();
        $this->broker->send(Op::OK, ['rid' => 1]);
        for ($tag = 1; $tag <= 4; $tag++) {
            $this->broker->send(Op::DELIVER, ['sub' => 1, 'tag' => $tag, 'key' => 'jobs'], "m$tag");
        }
        $subscription = $client->subscribe('jobs', ['prefetch' => 4]);

        for ($i = 1; $i <= 4; $i++) {
            $message = $subscription->next(1.0);
            $this->assertNotNull($message);
            $this->assertSame("m$i", $message->body);
            $message->ack();
        }

        $frames = $this->broker->frames();
        $this->assertSame(['rid' => 1, 'sub' => 1, 'queue' => 'jobs', 'credit' => 4], $frames[0]->header);
        $this->assertSame(
            [Op::SUBSCRIBE, Op::ACK, Op::ACK, Op::CREDIT, Op::ACK, Op::ACK, Op::CREDIT],
            array_map(static fn (Frame $f): int => $f->op, $frames),
        );
        $this->assertSame(['sub' => 1, 'credit' => 2], $frames[3]->header);
    }

    public function testNoAckGrantsCreditOnReceipt(): void
    {
        $client = $this->broker->client();
        $this->broker
            ->send(Op::OK, ['rid' => 1])
            ->send(Op::DELIVER, ['sub' => 1, 'tag' => 1], 'a')
            ->send(Op::DELIVER, ['sub' => 1, 'tag' => 2], 'b');
        $subscription = $client->subscribe('jobs', new SubscribeOptions(prefetch: 2, noAck: true));

        $first = $subscription->next(1.0);
        $subscription->next(1.0);

        $this->assertNotNull($first);
        $this->assertTrue($first->isSettled());
        $first->ack();
        $frames = $this->broker->frames();
        $this->assertTrue($frames[0]->header['no_ack']);
        $this->assertSame([Op::SUBSCRIBE, Op::CREDIT, Op::CREDIT], array_map(static fn (Frame $f): int => $f->op, $frames));
    }

    public function testNackRequeueAndDoubleSettleIsIgnored(): void
    {
        $client = $this->broker->client();
        $this->broker->send(Op::GET_OK, ['rid' => 1, 'tag' => 7, 'key' => 'jobs', 'redelivered' => true, 'remaining' => 0], 'x');

        $message = $client->get('jobs');

        $this->assertInstanceOf(Message::class, $message);
        $this->assertTrue($message->redelivered);
        $message->nack(true);
        $message->ack();
        $frames = $this->broker->frames();
        $this->assertSame([Op::GET, Op::NACK], array_map(static fn (Frame $f): int => $f->op, $frames));
        $this->assertSame(['tag' => 7, 'requeue' => true], $frames[1]->header);
    }

    public function testGetOnEmptyQueueReturnsNull(): void
    {
        $client = $this->broker->client();
        $this->broker->send(Op::OK, ['rid' => 1, 'empty' => true]);

        $this->assertNull($client->get('jobs', true));
        $this->assertSame(['rid' => 1, 'queue' => 'jobs', 'no_ack' => true], $this->broker->frames()[0]->header);
    }

    public function testCancelRequeuesMessagesNotYetReturned(): void
    {
        $client = $this->broker->client();
        $this->broker
            ->send(Op::OK, ['rid' => 1])
            ->send(Op::DELIVER, ['sub' => 1, 'tag' => 1], 'a')
            ->send(Op::DELIVER, ['sub' => 1, 'tag' => 2], 'b')
            ->send(Op::OK, ['rid' => 2]);
        $subscription = $client->subscribe('jobs');
        $first = $subscription->next(1.0);

        $subscription->cancel();

        $this->assertFalse($subscription->isActive());
        $this->assertNull($subscription->next(0.0));
        $this->assertNotNull($first);
        $first->ack();
        $frames = $this->broker->frames();
        $this->assertSame([Op::SUBSCRIBE, Op::UNSUBSCRIBE, Op::NACK, Op::ACK], array_map(static fn (Frame $f): int => $f->op, $frames));
        $this->assertSame(['tag' => 2, 'requeue' => true], $frames[2]->header);
    }

    public function testBrokerCancelEndsTheSubscription(): void
    {
        $client = $this->broker->client();
        $this->broker
            ->send(Op::OK, ['rid' => 1])
            ->send(Op::DELIVER, ['sub' => 1, 'tag' => 1], 'a')
            ->send(Op::CANCELLED, ['sub' => 1, 'code' => 404, 'msg' => "queue 'jobs' was deleted"]);
        $subscription = $client->subscribe('jobs');

        $this->assertSame('a', $subscription->next(1.0)?->body);
        $this->assertNull($subscription->next(1.0));
        $this->assertFalse($subscription->isActive());
        $this->assertSame(404, $subscription->cancelReason()?->getCode());
    }

    public function testConsumeAcksUnsettledMessagesAndStopsOnFalse(): void
    {
        $client = $this->broker->client();
        $this->broker
            ->send(Op::OK, ['rid' => 1])
            ->send(Op::DELIVER, ['sub' => 1, 'tag' => 1], 'a')
            ->send(Op::DELIVER, ['sub' => 1, 'tag' => 2], 'b')
            ->send(Op::DELIVER, ['sub' => 1, 'tag' => 3], 'c')
            ->send(Op::OK, ['rid' => 2]);
        $seen = [];

        $client->consume('jobs', function (Message $message) use (&$seen): bool {
            $seen[] = $message->body;

            return count($seen) < 2;
        }, ['prefetch' => 10]);

        $this->assertSame(['a', 'b'], $seen);
        $frames = $this->broker->frames();
        $this->assertSame(
            [Op::SUBSCRIBE, Op::ACK, Op::ACK, Op::UNSUBSCRIBE, Op::NACK],
            array_map(static fn (Frame $f): int => $f->op, $frames),
        );
        $this->assertSame(['tag' => 3, 'requeue' => true], $frames[4]->header);
    }

    public function testConsumeRequeuesWhenTheHandlerThrows(): void
    {
        $client = $this->broker->client();
        $this->broker
            ->send(Op::OK, ['rid' => 1])
            ->send(Op::DELIVER, ['sub' => 1, 'tag' => 1], 'a')
            ->send(Op::OK, ['rid' => 2]);

        try {
            $client->consume('jobs', static function (): void {
                throw new \DomainException('boom');
            });
            $this->fail('expected the handler exception');
        } catch (\DomainException $e) {
            $this->assertSame('boom', $e->getMessage());
        }

        $frames = $this->broker->frames();
        $this->assertSame([Op::SUBSCRIBE, Op::NACK, Op::UNSUBSCRIBE], array_map(static fn (Frame $f): int => $f->op, $frames));
        $this->assertSame(['tag' => 1, 'requeue' => true], $frames[1]->header);
    }

    public function testBrokerCloseFailsLaterCalls(): void
    {
        $client = $this->broker->client();
        $this->broker->send(Op::CLOSE, ['code' => 320, 'msg' => 'shutting down']);

        try {
            $client->purgeQueue('jobs');
            $this->fail('expected a ConnectionClosedException');
        } catch (ConnectionClosedException $e) {
            $this->assertSame(320, $e->getCode());
            $this->assertStringContainsString('shutting down', $e->getMessage());
        }
        $this->assertTrue($client->isClosed());
        $this->assertSame([Op::PURGE_QUEUE, Op::CLOSE], array_map(static fn (Frame $f): int => $f->op, $this->broker->frames()));

        $this->expectException(ConnectionClosedException::class);
        $client->publish('', 'jobs', 'x');
    }

    public function testFatalErrorClosesTheConnection(): void
    {
        $client = $this->broker->client();
        $this->broker->send(Op::ERROR, ['rid' => 1, 'code' => 501, 'msg' => 'FRAME_ERROR', 'fatal' => true]);

        try {
            $client->purgeQueue('jobs');
            $this->fail('expected a ServerException');
        } catch (ServerException $e) {
            $this->assertSame(501, $e->getCode());
        }
        $this->assertTrue($client->isClosed());
    }

    public function testSocketClosedByBrokerFailsPendingPublish(): void
    {
        $client = $this->broker->client();
        $this->broker->close();

        $this->expectException(ConnectionClosedException::class);

        $client->publish('', 'jobs', 'x');
    }

    public function testAsyncErrorsGoToTheCallback(): void
    {
        $errors = [];
        $this->broker->welcome();
        $connection = Connection::fromStream($this->broker->client, ConnectUrl::parse('fire://localhost'), 0, 2.0);
        $property = new \ReflectionProperty(Connection::class, 'onError');
        $property->setValue($connection, static function (ServerException $e) use (&$errors): void {
            $errors[] = $e->getCode();
        });
        $this->broker->send(Op::ERROR, ['code' => 406, 'msg' => 'unknown delivery tag 9'])->send(Op::OK, ['rid' => 1]);

        $connection->call(Op::PURGE_QUEUE, ['name' => 'jobs']);

        $this->assertSame([406], $errors);
    }

    public function testRequestsTimeOut(): void
    {
        $client = $this->broker->client(timeout: 0.2);

        $this->expectException(TimeoutException::class);

        $client->deleteExchange('orders');
    }

    public function testMessagesLargerThanMaxFrameAreRejectedLocally(): void
    {
        $connection = $this->broker->connection(maxFrame: 1000);

        try {
            $connection->publish('', 'jobs', str_repeat('x', 2000), new PublishOptions());
            $this->fail('expected a FireMQException');
        } catch (FireMQException $e) {
            $this->assertStringContainsString('does not fit in a frame', $e->getMessage());
        }
        $this->broker->send(Op::CONFIRM, ['upto' => 1]);
        $connection->publish('', 'jobs', 'small', new PublishOptions());
        $this->assertSame(1, $this->broker->frames()[0]->header['seq']);
    }

    public function testBatchWithAMessageThatCannotBeSentSendsNothing(): void
    {
        $connection = $this->broker->connection(maxFrame: 1000);
        $options = new PublishOptions();

        try {
            $connection->publishBatch([['', 'q', 'small', $options], ['', 'q', str_repeat('x', 2000), $options]]);
            $this->fail('expected a FireMQException');
        } catch (FireMQException $e) {
            $this->assertStringContainsString('does not fit in a frame', $e->getMessage());
        }
        $this->assertSame([], $this->broker->frames());

        $this->broker->send(Op::CONFIRM, ['upto' => 1]);
        $connection->publish('', 'q', 'next', $options);
        $this->assertSame(1, $this->broker->frames()[0]->header['seq']);
    }

    public function testCloseIsGraceful(): void
    {
        $client = $this->broker->client();
        $this->broker->send(Op::CLOSE, ['code' => 200]);

        $client->close();
        $client->close();

        $this->assertTrue($client->isClosed());
        $frames = $this->broker->frames();
        $this->assertCount(1, $frames);
        $this->assertSame(['code' => 200, 'msg' => 'client closed'], $frames[0]->header);
    }
}
