<?php

declare(strict_types=1);

namespace FireMQ\Exception;

/**
 * One or more messages of Client::publishBatch() failed. The others were confirmed.
 */
class PublishBatchException extends FireMQException
{
    /**
     * @param array<array-key, FireMQException> $failures failed messages, keyed like the input
     */
    public function __construct(private readonly array $failures, int $total)
    {
        $first = reset($failures);
        parent::__construct(
            sprintf(
                '%d of %d messages failed; first failure: %s',
                count($failures),
                $total,
                $first instanceof FireMQException ? $first->getMessage() : 'unknown',
            ),
            $first instanceof FireMQException ? (int) $first->getCode() : 0,
            $first instanceof FireMQException ? $first : null,
        );
    }

    /**
     * The failed messages: input key => the exception for that message.
     *
     * @return array<array-key, FireMQException>
     */
    public function getFailures(): array
    {
        return $this->failures;
    }
}
