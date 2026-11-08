<?php

declare(strict_types=1);

namespace FireMQ\Tests\Unit;

use FireMQ\Protocol\ConnectUrl;
use PHPUnit\Framework\TestCase;

final class ConnectUrlTest extends TestCase
{
    public function testDefaultsFillIn(): void
    {
        $url = ConnectUrl::parse('fire://localhost');

        $this->assertSame('fire', $url->scheme);
        $this->assertSame('localhost', $url->host);
        $this->assertSame(5673, $url->port);
        $this->assertSame('/', $url->vhost);
        $this->assertSame(['guest', 'guest'], [$url->user, $url->password]);
        $this->assertFalse($url->isTls());
    }

    public function testCredentialsAndVhostAreDecoded(): void
    {
        $url = ConnectUrl::parse('fires://app:p%40ss@mq.example.com:7000/prod%2Feu');

        $this->assertTrue($url->isTls());
        $this->assertSame('mq.example.com', $url->host);
        $this->assertSame(7000, $url->port);
        $this->assertSame('app', $url->user);
        $this->assertSame('p@ss', $url->password);
        $this->assertSame('prod/eu', $url->vhost);
        $this->assertSame('mq.example.com:7000', $url->authority());
    }

    public function testTlsDefaultPort(): void
    {
        $this->assertSame(5674, ConnectUrl::parse('fires://mq.example.com')->port);
    }

    public function testEmptyPathMeansDefaultVhost(): void
    {
        $this->assertSame('/', ConnectUrl::parse('fire://localhost/')->vhost);
        $this->assertSame('/', ConnectUrl::parse('fire://localhost/%2F')->vhost);
        $this->assertSame('dev', ConnectUrl::parse('fire://localhost/dev')->vhost);
        $this->assertSame('dev', ConnectUrl::parse('fire://localhost?vhost=dev')->vhost);
    }

    public function testUserWithoutPassword(): void
    {
        $url = ConnectUrl::parse('fire://admin@localhost');

        $this->assertSame('admin', $url->user);
        $this->assertSame('guest', $url->password);
    }

    public function testWebSocketUrlsUseQueryVhost(): void
    {
        $url = ConnectUrl::parse('ws://localhost:15672/fire?vhost=dev');
        $this->assertSame('/fire', $url->path);
        $this->assertSame('dev', $url->vhost);

        $url = ConnectUrl::parse('wss://mq.example.com');
        $this->assertSame([15671, '/fire', '/'], [$url->port, $url->path, $url->vhost]);
    }

    public function testOtherSchemesAreRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('unsupported scheme');

        ConnectUrl::parse('amqp://localhost');
    }

    public function testInvalidUrlsHideThePassword(): void
    {
        try {
            ConnectUrl::parse('fire:///');
            $this->fail('expected an exception');
        } catch (\InvalidArgumentException $e) {
            $this->assertStringContainsString('invalid URL', $e->getMessage());
        }
        try {
            ConnectUrl::parse('nope//user:secret@host');
            $this->fail('expected an exception');
        } catch (\InvalidArgumentException $e) {
            $this->assertStringNotContainsString('secret', $e->getMessage());
        }
    }

    public function testIpv6HostsAreBracketedInAuthority(): void
    {
        $url = ConnectUrl::parse('fire://[::1]:5673');

        $this->assertSame('::1', $url->host);
        $this->assertSame('[::1]:5673', $url->authority());
    }

    public function testStringFormOmitsThePassword(): void
    {
        $this->assertSame('fire://app@localhost:5673/prod', (string) ConnectUrl::parse('fire://app:secret@localhost/prod'));
    }
}
