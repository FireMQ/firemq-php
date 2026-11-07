<?php

declare(strict_types=1);

namespace FireMQ\Tests\Unit;

use FireMQ\ExchangeOptions;
use FireMQ\PublishOptions;
use FireMQ\QueueOptions;
use FireMQ\SubscribeOptions;
use PHPUnit\Framework\TestCase;

final class OptionsTest extends TestCase
{
    public function testDefaultsFavourSafety(): void
    {
        $this->assertTrue((new QueueOptions())->durable);
        $this->assertTrue((new ExchangeOptions())->durable);
        $publish = new PublishOptions();
        $this->assertTrue($publish->persistent);
        $this->assertTrue($publish->confirm);
        $this->assertFalse($publish->mandatory);
        $this->assertSame(100, (new SubscribeOptions())->prefetch);
    }

    public function testTemporaryQueues(): void
    {
        $options = QueueOptions::temporary();

        $this->assertFalse($options->durable);
        $this->assertTrue($options->exclusive);
        $this->assertTrue($options->autoDelete);
    }

    public function testArraysAcceptSnakeAndCamelCase(): void
    {
        $this->assertTrue(QueueOptions::from(['auto_delete' => true])->autoDelete);
        $this->assertTrue(QueueOptions::from(['autoDelete' => true])->autoDelete);
        $this->assertTrue(SubscribeOptions::from(['no_ack' => true])->noAck);
        $this->assertSame(['x-max-length' => 1], QueueOptions::from(['args' => ['x-max-length' => 1]])->args);
    }

    public function testObjectsPassThrough(): void
    {
        $options = new PublishOptions(mandatory: true);

        $this->assertSame($options, PublishOptions::from($options));
    }

    public function testUnknownOptionsAreRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('durabel');

        QueueOptions::from(['durabel' => false]);
    }

    public function testWrongTypesAreRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        SubscribeOptions::from(['prefetch' => 'many']);
    }

    public function testPropertiesAreValidated(): void
    {
        $options = new PublishOptions(properties: [
            'content_type' => 'application/json',
            'expiration' => '60000',
            'timestamp' => new \DateTimeImmutable('@1767225600'),
            'priority' => 9,
            'reply_to' => null,
        ]);

        $this->assertSame([
            'content_type' => 'application/json',
            'expiration' => 60000,
            'timestamp' => 1767225600,
            'priority' => 9,
        ], $options->properties);
    }

    public function testUnknownPropertiesAreRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('unknown message property "contentType"');

        new PublishOptions(properties: ['contentType' => 'text/plain']);
    }

    public function testPriorityIsBounded(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new PublishOptions(properties: ['priority' => 256]);
    }
}
