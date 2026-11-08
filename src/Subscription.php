<?php

declare(strict_types=1);

namespace FireMQ;

use FireMQ\Exception\ConnectionClosedException;
use FireMQ\Exception\ServerException;
use FireMQ\Protocol\Op;

/**
 * Messages from one queue. Get them with next(); stop with cancel().
 *
 * The broker sends at most `prefetch` messages ahead. Credit is granted back in batches of
 * half the window as messages are acknowledged (or, with `noAck`, as next() returns them).
 */
final class Subscription
{
    /** @var \SplQueue<Message> */
    private \SplQueue $buffer;

    private bool $active = true;

    private ?ServerException $cancelReason = null;

    private int $released = 0;

    /**
     * @internal use Client::subscribe()
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly int $id,
        private readonly string $queue,
        private readonly int $window,
        public readonly bool $noAck,
    ) {
        $this->buffer = new \SplQueue();
    }

    /**
     * The subscription id on this connection.
     */
    public function id(): int
    {
        return $this->id;
    }

    public function queue(): string
    {
        return $this->queue;
    }

    /**
     * False once the subscription was cancelled, by cancel() or by the broker, or the
     * connection closed.
     */
    public function isActive(): bool
    {
        return $this->active;
    }

    /**
     * Why the broker ended the subscription, for example because its queue was deleted.
     */
    public function cancelReason(): ?ServerException
    {
        return $this->cancelReason;
    }

    /**
     * Waits for the next message.
     *
     * Returns null when the timeout passes first, or once the subscription has ended and
     * every buffered message was returned. With a null timeout it waits as long as it takes.
     * Heartbeats are answered and sent while it waits.
     *
     * @throws ConnectionClosedException when the connection is closed
     */
    public function next(?float $timeoutSeconds = null): ?Message
    {
        if ($this->buffer->isEmpty()) {
            $this->connection->ensureOpen();
            if (!$this->active) {
                return null;
            }
            $deadline = $timeoutSeconds === null ? null : microtime(true) + max(0.0, $timeoutSeconds);
            $this->connection->waitUntil($this->readyOrEnded(...), $deadline);
        }

        return $this->take();
    }

    /**
     * Stops the subscription and waits for the broker to confirm. Messages already returned
     * by next() can still be acknowledged. Messages that arrived but were not returned yet
     * are requeued. Does nothing when the subscription has already ended.
     */
    public function cancel(): void
    {
        if (!$this->active) {
            return;
        }
        try {
            $this->connection->unsubscribe($this->id);
        } catch (ConnectionClosedException) {
            // the subscription ended with the connection
        } finally {
            $this->active = false;
            $unseen = $this->buffer;
            $this->buffer = new \SplQueue();
            if (!$this->noAck && !$this->connection->isClosed()) {
                foreach ($unseen as $message) {
                    $message->nack(true);
                }
            }
        }
    }

    private function readyOrEnded(): bool
    {
        return !$this->buffer->isEmpty() || $this->cancelReason !== null;
    }

    private function take(): ?Message
    {
        if ($this->buffer->isEmpty()) {
            return null;
        }
        $message = $this->buffer->dequeue();
        if ($this->noAck) {
            $this->release(1);
        }

        return $message;
    }

    /**
     * @internal
     */
    public function push(Message $message): void
    {
        $this->buffer->enqueue($message);
    }

    /**
     * Records handled messages and grants credit back in batches of half the window.
     *
     * @internal
     */
    public function release(int $count): void
    {
        if (!$this->active) {
            return;
        }
        $this->released += $count;
        if ($this->released >= max(1, intdiv($this->window, 2))) {
            $grant = $this->released;
            $this->released = 0;
            $this->connection->send(Op::CREDIT, ['sub' => $this->id, 'credit' => $grant]);
        }
    }

    /**
     * @internal
     */
    public function cancelledByBroker(ServerException $reason): void
    {
        $this->active = false;
        $this->cancelReason = $reason;
    }

    /**
     * @internal
     */
    public function connectionClosed(): void
    {
        $this->active = false;
        $this->buffer = new \SplQueue();
    }
}
