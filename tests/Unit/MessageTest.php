<?php

declare(strict_types=1);

namespace FireMQ\Tests\Unit;

use FireMQ\Exception\FireMQException;
use FireMQ\Message;
use PHPUnit\Framework\TestCase;

final class MessageTest extends TestCase
{
    public function testJsonBodies(): void
    {
        $message = new Message('{"id":42,"tags":["a"]}');

        $this->assertSame(['id' => 42, 'tags' => ['a']], $message->json());
        $this->assertSame(42, $message->json(false)->id);
    }

    public function testInvalidJsonThrows(): void
    {
        $this->expectException(FireMQException::class);

        (new Message('not json'))->json();
    }

    public function testProperties(): void
    {
        $message = new Message('x', properties: ['reply_to' => 'replies']);

        $this->assertSame('replies', $message->property('reply_to'));
        $this->assertNull($message->property('correlation_id'));
        $this->assertSame('none', $message->property('correlation_id', 'none'));
    }

    public function testMessagesWithoutConnectionNeedNoAck(): void
    {
        $message = new Message('x');

        $this->assertTrue($message->isSettled());
        $message->ack();
        $message->nack(true);
        $this->assertTrue($message->isSettled());
    }
}
