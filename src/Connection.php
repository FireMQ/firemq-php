<?php

declare(strict_types=1);

namespace FireMQ;

use FireMQ\Exception\ConnectionClosedException;
use FireMQ\Exception\FireMQException;
use FireMQ\Exception\NackedException;
use FireMQ\Exception\ProtocolException;
use FireMQ\Exception\ServerException;
use FireMQ\Exception\TimeoutException;
use FireMQ\Exception\UnroutableException;
use FireMQ\Protocol\Codec;
use FireMQ\Protocol\ConnectUrl;
use FireMQ\Protocol\Frame;
use FireMQ\Protocol\Op;

/**
 * One FireMQ connection: the socket, frame I/O, request/reply matching, publisher confirms,
 * subscriptions and heartbeats. Everything is synchronous: frames from the broker are read
 * and dispatched whenever the client waits for something.
 *
 * @internal use Client
 */
final class Connection
{
    private const READ_CHUNK = 65536;

    /** publishBatch() writes once this many bytes are queued. */
    private const FLUSH_THRESHOLD = 262144;

    /** @var resource|null */
    private $stream;

    private string $rbuf = '';

    private int $rpos = 0;

    private bool $eof = false;

    private string $wbuf = '';

    private int $maxFrame = Codec::MAX_HELLO_FRAME;

    private int $sendMaxFrame = Codec::DEFAULT_MAX_FRAME;

    private int $heartbeat = 0;

    private float $lastSend;

    private float $lastRecv;

    private int $nextRid = 1;

    private int $nextSeq = 1;

    private int $nextSub = 1;

    /** @var array<int, true> requests waiting for a reply */
    private array $pending = [];

    /** @var array<int, Frame|FireMQException> replies by request id */
    private array $replies = [];

    /** @var array<int, true> publishes waiting for a confirm, in sequence order */
    private array $unconfirmed = [];

    /** @var array<int, true|FireMQException> settled publishes not yet collected */
    private array $confirmed = [];

    /** @var array<int, array{0: string, 1: string, 2: int, 3: string}> returned mandatory publishes */
    private array $returned = [];

    /** @var array<int, Subscription> */
    private array $subs = [];

    private ?ConnectionClosedException $closed = null;

    private bool $closing = false;

    /** @var array<string, mixed> */
    private array $welcome = [];

    /** @var (\Closure(ServerException): void)|null */
    private ?\Closure $onError;

    /**
     * @param resource $stream
     */
    private function __construct($stream, private readonly ?float $timeout, ?callable $onError)
    {
        $this->stream = $stream;
        $this->onError = $onError === null ? null : \Closure::fromCallable($onError);
        $this->lastSend = $this->lastRecv = microtime(true);
    }

    /**
     * Opens a socket and performs the handshake.
     *
     * @param array{heartbeat: int, connection_name: string, connect_timeout: float, timeout: ?float, tls_ca_file: ?string, tls_insecure: bool, tls: array<string, mixed>, on_error: ?callable} $options
     */
    public static function open(ConnectUrl $url, array $options): self
    {
        if ($url->scheme === 'ws' || $url->scheme === 'wss') {
            throw new \InvalidArgumentException('the PHP client connects with fire:// or fires://; WebSocket is for browsers');
        }
        $ssl = [];
        if ($url->isTls()) {
            if (!extension_loaded('openssl')) {
                throw new ConnectionClosedException('fires:// needs the openssl extension');
            }
            $insecure = $options['tls_insecure'];
            $ssl = [
                'peer_name' => $url->host,
                'verify_peer' => !$insecure,
                'verify_peer_name' => !$insecure,
                'allow_self_signed' => $insecure,
                'SNI_enabled' => true,
            ];
            if ($options['tls_ca_file'] !== null) {
                $ssl['cafile'] = $options['tls_ca_file'];
            }
            $ssl = array_replace($ssl, $options['tls']);
        }
        $context = stream_context_create(['socket' => ['tcp_nodelay' => true], 'ssl' => $ssl]);
        $address = sprintf('%s://%s', $url->isTls() ? 'tls' : 'tcp', $url->authority());
        $errno = 0;
        $errstr = '';
        $warnings = [];
        set_error_handler(static function (int $type, string $message) use (&$warnings): bool {
            $warnings[] = (string) preg_replace('/^stream_socket_client\(\): /', '', $message);

            return true;
        });
        try {
            $stream = stream_socket_client($address, $errno, $errstr, $options['connect_timeout'], STREAM_CLIENT_CONNECT, $context);
        } finally {
            restore_error_handler();
        }
        if (!is_resource($stream)) {
            if ($errstr !== '') {
                $warnings[] = $errstr;
            }
            $reason = $warnings === [] ? 'unknown error' : implode('; ', array_unique($warnings));
            throw new ConnectionClosedException(sprintf('cannot connect to %s: %s', $url->authority(), $reason));
        }
        stream_set_blocking($stream, false);
        @stream_set_read_buffer($stream, 0);
        @stream_set_write_buffer($stream, 0);

        $connection = new self($stream, $options['timeout'], $options['on_error']);
        try {
            $connection->handshake($url, $options['heartbeat'], $options['connection_name'], $options['connect_timeout']);
        } catch (\Throwable $e) {
            $connection->markClosed(new ConnectionClosedException('handshake failed', 0, $e));
            throw $e;
        }

        return $connection;
    }

    /**
     * Performs the handshake on an already connected stream. Used by tests.
     *
     * @param resource $stream
     */
    public static function fromStream($stream, ConnectUrl $url, int $heartbeat = 30, ?float $timeout = 30.0, float $connectTimeout = 10.0): self
    {
        stream_set_blocking($stream, false);
        $connection = new self($stream, $timeout, null);
        $connection->handshake($url, $heartbeat, '', $connectTimeout);

        return $connection;
    }

    /**
     * A connection that is garbage collected sends `close` without waiting for the reply.
     */
    public function __destruct()
    {
        if ($this->closed === null && $this->stream !== null) {
            try {
                $this->close(0.0);
            } catch (\Throwable) {
                // nothing left to do
            }
        }
    }

    /**
     * The `welcome` header: server name and version, conn_id, heartbeat, max_frame and caps.
     *
     * @return array<string, mixed>
     */
    public function welcome(): array
    {
        return $this->welcome;
    }

    public function isClosed(): bool
    {
        return $this->closed !== null;
    }

    /**
     * @throws ConnectionClosedException when the connection is closed
     */
    public function ensureOpen(): void
    {
        if ($this->closed !== null) {
            throw $this->closedError();
        }
    }

    /**
     * Sends a request and waits for its reply: `ok` or `get_ok`.
     *
     * @param array<string, mixed> $header the request's fields, without `rid`
     *
     * @throws ServerException when the broker refuses the request
     */
    public function call(int $op, array $header): Frame
    {
        $this->ensureOpen();
        $rid = $this->nextRid;
        $this->nextRid = $rid >= 0xFFFFFFFF ? 1 : $rid + 1;
        $this->pending[$rid] = true;
        try {
            $this->send($op, ['rid' => $rid] + $header);
            $deadline = $this->timeout === null ? null : microtime(true) + $this->timeout;
            if (!$this->waitUntil(fn (): bool => isset($this->replies[$rid]), $deadline)) {
                throw new TimeoutException(sprintf('no reply to %s within %s seconds', Op::name($op), $this->timeout));
            }
            $reply = $this->replies[$rid];
        } finally {
            unset($this->pending[$rid], $this->replies[$rid]);
        }
        if ($reply instanceof FireMQException) {
            throw $reply;
        }

        return $reply;
    }

    /**
     * Writes a frame.
     *
     * @param array<string, mixed> $header
     */
    public function send(int $op, array $header = [], string $payload = ''): void
    {
        $this->ensureOpen();
        $this->wbuf .= Codec::encodeFrame($op, $header, $payload);
        $this->flush();
    }

    /**
     * Publishes one message and, unless `confirm` is off, waits for the broker's confirm.
     */
    public function publish(string $exchange, string $key, string $body, PublishOptions $options): void
    {
        $this->ensureOpen();
        $seq = $options->confirm ? $this->nextSeq : 0;
        $frame = $this->encodePublish($seq, $exchange, $key, $body, $options);
        if ($seq === 0) {
            $this->wbuf .= $frame;
            $this->flush();

            return;
        }
        $this->nextSeq++;
        $this->unconfirmed[$seq] = true;
        $this->wbuf .= $frame;
        try {
            $this->flush();
        } catch (ConnectionClosedException) {
            // markClosed() failed the publish; awaitConfirms() reports it
        }
        $failures = $this->awaitConfirms([$seq]);
        if ($failures !== []) {
            throw reset($failures);
        }
    }

    /**
     * Writes many publishes back to back, then waits for all their confirms. Every message is
     * encoded and checked first, so a message that cannot be sent fails the batch before
     * anything is written.
     *
     * @param array<array-key, array{0: string, 1: string, 2: string, 3: PublishOptions}> $messages
     *
     * @return array<array-key, FireMQException> failures keyed like $messages
     */
    public function publishBatch(array $messages): array
    {
        $this->ensureOpen();
        $next = $this->nextSeq;
        $seqs = [];
        $frames = [];
        foreach ($messages as $index => [$exchange, $key, $body, $options]) {
            $seq = $options->confirm ? $next++ : 0;
            $frames[] = $this->encodePublish($seq, $exchange, $key, $body, $options);
            $seqs[$index] = $seq;
        }
        $this->nextSeq = $next;
        foreach ($seqs as $seq) {
            if ($seq > 0) {
                $this->unconfirmed[$seq] = true;
            }
        }
        try {
            foreach ($frames as $i => $frame) {
                $this->wbuf .= $frame;
                unset($frames[$i]);
                if (strlen($this->wbuf) >= self::FLUSH_THRESHOLD) {
                    $this->flush();
                }
            }
            $this->flush();
        } catch (ConnectionClosedException) {
            // markClosed() failed every unconfirmed publish; awaitConfirms() reports them
        }

        return $this->awaitConfirms($seqs);
    }

    /**
     * Starts a subscription with the given credit window.
     */
    public function subscribe(string $queue, SubscribeOptions $options): Subscription
    {
        $this->ensureOpen();
        $id = $this->nextSub++;
        $window = max(1, $options->prefetch);
        $subscription = new Subscription($this, $id, $queue, $window, $options->noAck);
        $this->subs[$id] = $subscription;
        try {
            $this->call(Op::SUBSCRIBE, self::compact([
                'sub' => $id,
                'queue' => $queue,
                'credit' => $window,
                'no_ack' => $options->noAck,
                'exclusive' => $options->exclusive,
                'tag' => $options->tag,
                'args' => $options->args,
            ]));
        } catch (\Throwable $e) {
            unset($this->subs[$id]);
            throw $e;
        }

        return $subscription;
    }

    /**
     * Ends a subscription and waits for the broker to confirm. Deliveries that arrive before
     * the confirmation are still handed to the subscription.
     */
    public function unsubscribe(int $id): void
    {
        if (!isset($this->subs[$id])) {
            return;
        }
        try {
            $this->call(Op::UNSUBSCRIBE, ['sub' => $id]);
        } finally {
            unset($this->subs[$id]);
        }
    }

    /**
     * Takes one message from a queue, or null when it is empty.
     */
    public function get(string $queue, bool $noAck): ?Message
    {
        $reply = $this->call(Op::GET, self::compact(['queue' => $queue, 'no_ack' => $noAck]));
        if ($reply->op !== Op::GET_OK) {
            return null;
        }

        return $this->message($reply, $noAck ? null : $this, null);
    }

    /**
     * Reads and dispatches frames until $done returns true or the deadline passes. Sends
     * heartbeats while it waits. With a deadline in the past, it still dispatches whatever
     * the socket already holds.
     *
     * @param callable(): bool $done
     *
     * @return bool whether $done returned true
     *
     * @throws ConnectionClosedException when the connection closes first
     */
    public function waitUntil(callable $done, ?float $deadline): bool
    {
        while (true) {
            if ($done()) {
                return true;
            }
            if ($this->closed !== null) {
                throw $this->closedError();
            }
            $expired = $deadline !== null && microtime(true) >= $deadline;
            $this->pump($expired ? 0.0 : $deadline, $done);
            if ($expired) {
                if ($done()) {
                    return true;
                }
                if ($this->closed !== null) {
                    throw $this->closedError();
                }

                return false;
            }
        }
    }

    /**
     * Closes the connection gracefully: sends `close` and waits briefly for the broker's.
     */
    public function close(float $wait = 2.0): void
    {
        if ($this->closed !== null) {
            return;
        }
        $this->closing = true;
        try {
            $this->send(Op::CLOSE, ['code' => 200, 'msg' => 'client closed']);
            $deadline = microtime(true) + $wait;
            while ($this->closed === null && microtime(true) < $deadline) {
                $this->pump($deadline);
            }
        } catch (FireMQException) {
            // the connection is going away regardless
        } finally {
            $this->markClosed(new ConnectionClosedException('the connection was closed by the client'));
        }
    }

    private function handshake(ConnectUrl $url, int $heartbeat, string $connectionName, float $connectTimeout): void
    {
        $client = ['name' => 'firemq-php', 'version' => Client::VERSION, 'lang' => 'php', 'platform' => 'PHP ' . PHP_VERSION];
        if ($connectionName !== '') {
            $client['connection_name'] = $connectionName;
        }
        $this->wbuf = Codec::MAGIC . Codec::encodeFrame(Op::HELLO, [
            'ver' => Codec::PROTOCOL_VERSION,
            'vhost' => $url->vhost,
            'auth' => ['mech' => 'plain', 'user' => $url->user, 'pass' => $url->password],
            'heartbeat' => $heartbeat,
            'client' => $client,
            'caps' => ['confirms', 'credit'],
        ]);
        $deadline = microtime(true) + $connectTimeout;
        $this->flush($deadline);
        $frame = $this->readHandshakeFrame($deadline);
        switch ($frame->op) {
            case Op::WELCOME:
                $this->welcome = $frame->header;
                $this->heartbeat = (int) ($frame->header['heartbeat'] ?? 0);
                $max = (int) ($frame->header['max_frame'] ?? 0);
                $this->sendMaxFrame = $max > 0 ? $max : Codec::DEFAULT_MAX_FRAME;
                $this->maxFrame = max($this->sendMaxFrame, Codec::MAX_HELLO_FRAME);

                return;
            case Op::ERROR:
                throw new ServerException((int) ($frame->header['code'] ?? 0), (string) ($frame->header['msg'] ?? ''));
            default:
                throw new ProtocolException(sprintf('expected welcome, got %s', $frame->name()));
        }
    }

    private function readHandshakeFrame(float $deadline): Frame
    {
        while (true) {
            $raw = Codec::splitFrame($this->rbuf, $this->rpos, Codec::MAX_HELLO_FRAME);
            if ($raw !== null) {
                return new Frame($raw[0], Codec::decodeHeader($raw[1]), $raw[2]);
            }
            if ($this->eof) {
                throw new ConnectionClosedException('the server closed the connection during the handshake');
            }
            $wait = $deadline - microtime(true);
            if ($wait <= 0) {
                throw new TimeoutException('the handshake timed out');
            }
            $this->fill($wait);
        }
    }

    /**
     * Encodes a publish frame and checks that the broker accepts its size.
     */
    private function encodePublish(int $seq, string $exchange, string $key, string $body, PublishOptions $options): string
    {
        $frame = Codec::encodeFrame(Op::PUBLISH, self::compact([
            'seq' => $seq,
            'exchange' => $exchange,
            'key' => $key,
            'mandatory' => $options->mandatory,
            'persistent' => $options->persistent,
            'props' => $options->properties,
            'headers' => $options->headers,
        ]), $body);
        if (strlen($frame) - 4 > $this->sendMaxFrame) {
            throw new FireMQException(sprintf(
                'message of %d bytes does not fit in a frame; the broker accepts frames of up to %d bytes',
                strlen($body),
                $this->sendMaxFrame,
            ));
        }

        return $frame;
    }

    /**
     * Waits until every sequence number is confirmed or nacked. The timeout restarts
     * whenever a confirm arrives.
     *
     * @param array<array-key, int> $seqs 0 entries need no confirm
     *
     * @return array<array-key, FireMQException> failures keyed like $seqs
     */
    private function awaitConfirms(array $seqs): array
    {
        $waiting = array_values(array_filter($seqs, static fn (int $seq): bool => $seq > 0));
        $count = count($waiting);
        $settled = 0;
        $done = function () use (&$settled, $waiting, $count): bool {
            while ($settled < $count && isset($this->confirmed[$waiting[$settled]])) {
                $settled++;
            }

            return $settled >= $count;
        };
        while (true) {
            $before = $settled;
            $deadline = $this->timeout === null ? null : microtime(true) + $this->timeout;
            try {
                if ($this->waitUntil($done, $deadline)) {
                    break;
                }
            } catch (ConnectionClosedException $e) {
                // markClosed() settled every outstanding publish; fall through to collect them
                break;
            }
            if ($settled > $before) {
                continue;
            }
            foreach ($waiting as $seq) {
                if (!isset($this->confirmed[$seq])) {
                    unset($this->unconfirmed[$seq], $this->returned[$seq]);
                    $this->confirmed[$seq] = new TimeoutException(sprintf('no confirm for publish %d within %s seconds', $seq, $this->timeout));
                }
            }
            break;
        }
        $failures = [];
        foreach ($seqs as $index => $seq) {
            if ($seq === 0) {
                continue;
            }
            $result = $this->confirmed[$seq] ?? $this->closedError();
            unset($this->confirmed[$seq]);
            if ($result instanceof FireMQException) {
                $failures[$index] = $result;
            }
        }

        return $failures;
    }

    /**
     * Dispatches buffered frames, sends a heartbeat when due, and waits for more data until
     * the deadline (0.0 means do not wait). Dispatching stops as soon as $done returns true;
     * later frames stay buffered for the next wait.
     *
     * @param (callable(): bool)|null $done
     */
    private function pump(?float $deadline, ?callable $done = null): void
    {
        if ($this->dispatchBuffered($done) > 0 || $this->closed !== null) {
            return;
        }
        $now = microtime(true);
        $wait = $deadline === null ? 60.0 : $deadline - $now;
        if ($this->heartbeat > 0) {
            $due = $this->heartbeatTick($now);
            if ($due === null) {
                return;
            }
            $wait = min($wait, $due);
        }
        $this->fill(max(0.0, $wait));
        $this->dispatchBuffered($done);
    }

    /**
     * Sends a ping when nothing was sent for half the heartbeat interval, and gives up on a
     * broker that sent nothing for two intervals.
     *
     * @return float|null seconds until the next heartbeat check is due, or null when the
     *                    broker is considered dead and the connection was closed
     */
    private function heartbeatTick(float $now): ?float
    {
        $half = $this->heartbeat / 2;
        if ($now - $this->lastRecv > 2 * $this->heartbeat) {
            $this->markClosed(new ConnectionClosedException(sprintf('the broker sent nothing for %d seconds (missed heartbeats)', 2 * $this->heartbeat)));

            return null;
        }
        if ($now - $this->lastSend >= $half) {
            $this->wbuf .= Codec::encodeFrame(Op::PING);
            $this->flush();
        }

        return max(0.01, min($this->lastSend + $half, $this->lastRecv + 2 * $this->heartbeat) - $now + 0.001);
    }

    /**
     * Waits up to $wait seconds for data and appends it to the read buffer.
     */
    private function fill(float $wait): void
    {
        if ($this->stream === null || $this->readAvailable() || $this->eof || $wait <= 0) {
            return;
        }
        $read = [$this->stream];
        $write = null;
        $except = null;
        $seconds = (int) floor($wait);
        $micro = (int) (($wait - $seconds) * 1_000_000);
        if (@stream_select($read, $write, $except, $seconds, $micro) > 0) {
            $this->readAvailable();
        }
    }

    /**
     * Reads everything the socket holds without blocking.
     *
     * @return bool whether any bytes were read
     */
    private function readAvailable(): bool
    {
        if ($this->stream === null) {
            return false;
        }
        $got = false;
        while (true) {
            $data = @fread($this->stream, self::READ_CHUNK);
            if ($data === false || $data === '') {
                if ($data === false || feof($this->stream)) {
                    $this->eof = true;
                }

                return $got;
            }
            $this->rbuf .= $data;
            $this->lastRecv = microtime(true);
            $got = true;
        }
    }

    /**
     * Writes the whole write buffer. When the socket is full, it keeps reading so the broker
     * never blocks on a full socket of its own.
     */
    private function flush(?float $deadline = null): void
    {
        $stalledSince = null;
        while ($this->wbuf !== '') {
            if ($this->stream === null) {
                throw $this->closedError();
            }
            $written = @fwrite($this->stream, $this->wbuf);
            if ($written === false) {
                $error = error_get_last();
                $this->markClosed(new ConnectionClosedException('write failed: ' . ($error['message'] ?? 'unknown error')));
                throw $this->closedError();
            }
            if ($written > 0) {
                $this->wbuf = $written >= strlen($this->wbuf) ? '' : substr($this->wbuf, $written);
                $this->lastSend = microtime(true);
                $stalledSince = null;
                continue;
            }
            $now = microtime(true);
            $stalledSince ??= $now;
            $limit = $deadline ?? ($this->timeout === null ? null : $stalledSince + max($this->timeout, 1.0));
            if ($limit !== null && $now >= $limit) {
                $this->markClosed(new ConnectionClosedException('the broker stopped reading; write timed out'));
                throw $this->closedError();
            }
            $read = [$this->stream];
            $write = [$this->stream];
            $except = null;
            if (@stream_select($read, $write, $except, 0, 200_000) > 0) {
                $this->readAvailable();
                if ($this->eof) {
                    $this->markClosed(new ConnectionClosedException('the server closed the connection'));
                    throw $this->closedError();
                }
            }
        }
    }

    /**
     * Decodes and dispatches complete frames from the read buffer until $done returns true.
     *
     * @param (callable(): bool)|null $done
     *
     * @return int how many frames were dispatched
     */
    private function dispatchBuffered(?callable $done = null): int
    {
        $count = 0;
        while ($this->closed === null && ($done === null || !$done())) {
            try {
                $raw = Codec::splitFrame($this->rbuf, $this->rpos, $this->maxFrame);
            } catch (ProtocolException $e) {
                $this->markClosed(new ConnectionClosedException('protocol error: ' . $e->getMessage(), 501, $e));
                break;
            }
            if ($raw === null) {
                break;
            }
            $count++;
            try {
                $header = Codec::decodeHeader($raw[1]);
            } catch (ProtocolException) {
                continue; // like the Rust client, skip frames whose header cannot be decoded
            }
            $this->dispatch(new Frame($raw[0], $header, $raw[2]));
        }
        if ($this->rpos > 0 && ($this->rpos >= strlen($this->rbuf) || $this->rpos > 1048576)) {
            $this->rbuf = (string) substr($this->rbuf, $this->rpos);
            $this->rpos = 0;
        }
        if ($this->eof && $this->closed === null && ($done === null || !$done())) {
            $this->markClosed(new ConnectionClosedException('the server closed the connection'));
        }

        return $count;
    }

    private function dispatch(Frame $frame): void
    {
        $h = $frame->header;
        switch ($frame->op) {
            case Op::OK:
            case Op::GET_OK:
                $rid = (int) ($h['rid'] ?? 0);
                if (isset($this->pending[$rid])) {
                    $this->replies[$rid] = $frame;
                }
                break;
            case Op::ERROR:
                $code = (int) ($h['code'] ?? 0);
                $error = new ServerException($code, (string) ($h['msg'] ?? ''));
                $rid = isset($h['rid']) ? (int) $h['rid'] : null;
                if ($rid !== null && isset($this->pending[$rid])) {
                    $this->replies[$rid] = $error;
                } elseif (empty($h['fatal']) && $this->onError !== null) {
                    ($this->onError)($error);
                }
                if (!empty($h['fatal'])) {
                    $this->markClosed(new ConnectionClosedException('closed by the broker: ' . $error->getMessage(), $code, $error));
                }
                break;
            case Op::CONFIRM:
                $this->onConfirm($h);
                break;
            case Op::RETURN:
                $seq = (int) ($h['seq'] ?? 0);
                if ($seq > 0 && isset($this->unconfirmed[$seq])) {
                    $this->returned[$seq] = [
                        (string) ($h['exchange'] ?? ''),
                        (string) ($h['key'] ?? ''),
                        (int) ($h['code'] ?? 312),
                        (string) ($h['msg'] ?? ''),
                    ];
                }
                break;
            case Op::DELIVER:
                $subscription = $this->subs[(int) ($h['sub'] ?? 0)] ?? null;
                if ($subscription !== null) {
                    $subscription->push($this->message($frame, $subscription->noAck ? null : $this, $subscription));
                }
                break;
            case Op::CANCELLED:
                $id = (int) ($h['sub'] ?? 0);
                if (isset($this->subs[$id])) {
                    $this->subs[$id]->cancelledByBroker(new ServerException((int) ($h['code'] ?? 0), (string) ($h['msg'] ?? 'subscription cancelled by the broker')));
                    unset($this->subs[$id]);
                }
                break;
            case Op::PING:
                $this->wbuf .= Codec::encodeFrame(Op::PONG);
                $this->flush();
                break;
            case Op::CLOSE:
                $code = (int) ($h['code'] ?? 0);
                $msg = (string) ($h['msg'] ?? '');
                if (!$this->closing) {
                    $this->wbuf .= Codec::encodeFrame(Op::CLOSE, ['code' => 200]);
                    try {
                        $this->flush(microtime(true) + 1.0);
                    } catch (FireMQException) {
                        // the broker is closing the socket anyway
                    }
                }
                $this->markClosed(new ConnectionClosedException(
                    $msg === '' ? sprintf('closed by the broker (code %d)', $code) : sprintf('closed by the broker: %s (code %d)', $msg, $code),
                    $code,
                ));
                break;
            default:
                // pong, flow and unexpected frames need no action
                break;
        }
    }

    /**
     * @param array<string, mixed> $h
     */
    private function onConfirm(array $h): void
    {
        if (!empty($h['nack'])) {
            $seq = (int) ($h['seq'] ?? 0);
            unset($this->returned[$seq]);
            if (isset($this->unconfirmed[$seq])) {
                unset($this->unconfirmed[$seq]);
                $this->confirmed[$seq] = new NackedException((int) ($h['code'] ?? 0), (string) ($h['msg'] ?? ''));
            }

            return;
        }
        if (!isset($h['upto'])) {
            return;
        }
        $upto = (int) $h['upto'];
        foreach ($this->unconfirmed as $seq => $_) {
            if ($seq > $upto) {
                break;
            }
            unset($this->unconfirmed[$seq]);
            if (isset($this->returned[$seq])) {
                [$exchange, $key, $code, $msg] = $this->returned[$seq];
                unset($this->returned[$seq]);
                $this->confirmed[$seq] = new UnroutableException($exchange, $key, $code, $msg);
            } else {
                $this->confirmed[$seq] = true;
            }
        }
    }

    private function message(Frame $frame, ?self $connection, ?Subscription $subscription): Message
    {
        $h = $frame->header;
        $properties = $h['props'] ?? [];
        $headers = $h['headers'] ?? [];

        return new Message(
            body: $frame->payload,
            exchange: (string) ($h['exchange'] ?? ''),
            routingKey: (string) ($h['key'] ?? ''),
            properties: is_array($properties) ? $properties : [],
            headers: is_array($headers) ? $headers : [],
            redelivered: (bool) ($h['redelivered'] ?? false),
            deliveryTag: (int) ($h['tag'] ?? 0),
            connection: $connection,
            subscription: $subscription,
        );
    }

    /**
     * Records why the connection ended, fails everything that waits on it and closes the
     * socket. Only the first reason is kept.
     */
    private function markClosed(ConnectionClosedException $reason): void
    {
        if ($this->closed !== null) {
            return;
        }
        if ($this->closing) {
            $reason = new ConnectionClosedException('the connection was closed by the client');
        }
        $this->closed = $reason;
        foreach (array_keys($this->pending) as $rid) {
            $this->replies[$rid] ??= $reason;
        }
        foreach (array_keys($this->unconfirmed) as $seq) {
            $this->confirmed[$seq] = $reason;
        }
        $this->unconfirmed = [];
        $this->returned = [];
        $subs = $this->subs;
        $this->subs = [];
        foreach ($subs as $subscription) {
            $subscription->connectionClosed();
        }
        if ($this->stream !== null) {
            @fclose($this->stream);
            $this->stream = null;
        }
        $this->wbuf = '';
    }

    private function closedError(): ConnectionClosedException
    {
        $closed = $this->closed ?? new ConnectionClosedException('the connection is closed');

        return new ConnectionClosedException($closed->getMessage(), (int) $closed->getCode(), $closed);
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
