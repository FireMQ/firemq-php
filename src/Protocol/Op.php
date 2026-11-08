<?php

declare(strict_types=1);

namespace FireMQ\Protocol;

/**
 * Op codes of the FireMQ protocol, version 1.
 */
final class Op
{
    public const HELLO = 0x01;
    public const WELCOME = 0x02;
    public const PING = 0x03;
    public const PONG = 0x04;
    public const CLOSE = 0x05;
    public const OK = 0x06;
    public const ERROR = 0x07;
    public const FLOW = 0x08;
    public const DECLARE_EXCHANGE = 0x10;
    public const DELETE_EXCHANGE = 0x11;
    public const DECLARE_QUEUE = 0x12;
    public const DELETE_QUEUE = 0x13;
    public const PURGE_QUEUE = 0x14;
    public const BIND = 0x15;
    public const UNBIND = 0x16;
    public const PUBLISH = 0x20;
    public const CONFIRM = 0x21;
    public const RETURN = 0x22;
    public const SUBSCRIBE = 0x30;
    public const UNSUBSCRIBE = 0x31;
    public const CREDIT = 0x32;
    public const DELIVER = 0x33;
    public const CANCELLED = 0x34;
    public const ACK = 0x35;
    public const NACK = 0x36;
    public const GET = 0x37;
    public const GET_OK = 0x38;

    private const NAMES = [
        self::HELLO => 'hello',
        self::WELCOME => 'welcome',
        self::PING => 'ping',
        self::PONG => 'pong',
        self::CLOSE => 'close',
        self::OK => 'ok',
        self::ERROR => 'error',
        self::FLOW => 'flow',
        self::DECLARE_EXCHANGE => 'declare_exchange',
        self::DELETE_EXCHANGE => 'delete_exchange',
        self::DECLARE_QUEUE => 'declare_queue',
        self::DELETE_QUEUE => 'delete_queue',
        self::PURGE_QUEUE => 'purge_queue',
        self::BIND => 'bind',
        self::UNBIND => 'unbind',
        self::PUBLISH => 'publish',
        self::CONFIRM => 'confirm',
        self::RETURN => 'return',
        self::SUBSCRIBE => 'subscribe',
        self::UNSUBSCRIBE => 'unsubscribe',
        self::CREDIT => 'credit',
        self::DELIVER => 'deliver',
        self::CANCELLED => 'cancelled',
        self::ACK => 'ack',
        self::NACK => 'nack',
        self::GET => 'get',
        self::GET_OK => 'get_ok',
    ];

    private function __construct()
    {
    }

    /**
     * The op's name as written in the spec, for example `declare_queue`, or `unknown`.
     */
    public static function name(int $op): string
    {
        return self::NAMES[$op] ?? 'unknown';
    }
}
