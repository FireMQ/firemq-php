<?php

declare(strict_types=1);

namespace FireMQ\Tests\Unit;

use FireMQ\Client;
use FireMQ\Connection;
use FireMQ\Protocol\Codec;
use FireMQ\Protocol\ConnectUrl;
use FireMQ\Protocol\Frame;
use FireMQ\Protocol\Op;

/**
 * The broker end of a socket pair. Tests write the broker's frames ahead of time, call the
 * client, then inspect what the client sent.
 */
final class FakeBroker
{
    /** @var resource */
    public $client;

    /** @var resource */
    private $server;

    private string $received = '';

    private int $offset = 0;

    public function __construct()
    {
        $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if ($pair === false) {
            throw new \RuntimeException('cannot create a socket pair');
        }
        [$this->client, $this->server] = $pair;
        stream_set_blocking($this->server, false);
    }

    /**
     * @param array<string, mixed> $header
     */
    public function send(int $op, array $header = [], string $payload = ''): self
    {
        fwrite($this->server, Codec::encodeFrame($op, $header, $payload));

        return $this;
    }

    public function welcome(int $heartbeat = 0, int $maxFrame = Codec::DEFAULT_MAX_FRAME): self
    {
        return $this->send(Op::WELCOME, [
            'ver' => 1,
            'server' => ['name' => 'fake', 'version' => '0.0.0'],
            'conn_id' => 'test',
            'heartbeat' => $heartbeat,
            'max_frame' => $maxFrame,
            'caps' => ['confirms', 'credit'],
        ]);
    }

    public function connection(int $heartbeat = 0, ?float $timeout = 2.0, int $maxFrame = Codec::DEFAULT_MAX_FRAME): Connection
    {
        $this->welcome($heartbeat, $maxFrame);
        $connection = Connection::fromStream($this->client, ConnectUrl::parse('fire://localhost'), $heartbeat, $timeout);
        $hello = $this->frames();
        if (count($hello) !== 1 || $hello[0]->op !== Op::HELLO) {
            throw new \RuntimeException('expected hello');
        }

        return $connection;
    }

    public function client(int $heartbeat = 0, ?float $timeout = 2.0): Client
    {
        $connection = $this->connection($heartbeat, $timeout);

        return \Closure::bind(static fn (Connection $c): Client => new Client($c), null, Client::class)($connection);
    }

    public function close(): void
    {
        fclose($this->server);
    }

    /**
     * Frames the client sent since the last call. The `FMQ1` magic is checked and skipped.
     *
     * @return list<Frame>
     */
    public function frames(): array
    {
        while (($chunk = fread($this->server, 65536)) !== false && $chunk !== '') {
            $this->received .= $chunk;
        }
        if ($this->offset === 0 && str_starts_with($this->received, Codec::MAGIC)) {
            $this->offset = 4;
        }
        $frames = [];
        while (($raw = Codec::splitFrame($this->received, $this->offset, PHP_INT_MAX)) !== null) {
            $frames[] = new Frame($raw[0], Codec::decodeHeader($raw[1]), $raw[2]);
        }

        return $frames;
    }

    /**
     * @return list<Frame>
     */
    public function framesOf(int $op): array
    {
        return array_values(array_filter($this->frames(), static fn (Frame $f): bool => $f->op === $op));
    }
}
