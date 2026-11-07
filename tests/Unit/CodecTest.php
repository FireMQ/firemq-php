<?php

declare(strict_types=1);

namespace FireMQ\Tests\Unit;

use FireMQ\Exception\ProtocolException;
use FireMQ\Protocol\Codec;
use FireMQ\Protocol\Op;
use PHPUnit\Framework\TestCase;

final class CodecTest extends TestCase
{
    public function testSplitsFramesThatArriveByteByByte(): void
    {
        $stream = Codec::encodeFrame(Op::PUBLISH, ['seq' => 1], 'hello') . Codec::encodeFrame(Op::PING);
        $buffer = '';
        $offset = 0;
        $frames = [];
        foreach (str_split($stream) as $byte) {
            $buffer .= $byte;
            while (($raw = Codec::splitFrame($buffer, $offset, 1024)) !== null) {
                $frames[] = $raw;
            }
        }

        $this->assertCount(2, $frames);
        $this->assertSame(Op::PUBLISH, $frames[0][0]);
        $this->assertSame(['seq' => 1], Codec::decodeHeader($frames[0][1]));
        $this->assertSame('hello', $frames[0][2]);
        $this->assertSame([Op::PING, '', ''], $frames[1]);
        $this->assertSame(strlen($buffer), $offset);
    }

    public function testRejectsOversizedFrames(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('exceeds the maximum');

        Codec::decodeFrame(Codec::encodeFrame(Op::PUBLISH, [], str_repeat('x', 100)), 50);
    }

    public function testRejectsHeaderLongerThanFrame(): void
    {
        $this->expectException(ProtocolException::class);

        Codec::decodeFrame(pack('NCN', 6, Op::PUBLISH, 9) . "\x80");
    }

    public function testRejectsFramesShorterThanFiveBytes(): void
    {
        $this->expectException(ProtocolException::class);

        Codec::decodeFrame(pack('N', 3) . 'abc');
    }

    public function testRejectsHeadersThatAreNotMaps(): void
    {
        $this->expectException(ProtocolException::class);

        Codec::decodeHeader("\x93\x01\x02\x03");
    }

    public function testEmptyHeaderIsAnEmptyMap(): void
    {
        $this->assertSame([], Codec::decodeHeader(''));
        $this->assertSame([], Codec::decodeHeader("\x80"));
    }

    public function testIgnoresUnknownHeaderKeys(): void
    {
        $frame = Codec::decodeFrame(Codec::encodeFrame(Op::ACK, ['tag' => 5, 'future_field' => [1, 2]]));

        $this->assertNotNull($frame);
        $this->assertSame(5, $frame->header['tag']);
    }

    public function testKeysAreAlwaysStrings(): void
    {
        // {"42": 1} must not become {42: 1}
        $this->assertSame('81a2343201', bin2hex(Codec::encodeMap(['42' => 1])));
        // a list-shaped PHP array is a MessagePack array; an object keeps its keys
        $this->assertSame('81a161920102', bin2hex(Codec::encodeMap(['a' => [1, 2]])));
        $this->assertSame('81a16182a13001a1310a', bin2hex(Codec::encodeMap(['a' => (object) ['0' => 1, '1' => 10]])));
    }

    public function testMapsAndArrays(): void
    {
        $encoded = Codec::encodeMap([
            'list' => [1, 'two'],
            'map' => ['k' => 'v'],
            'empty_list' => [],
            'empty_map' => new \stdClass(),
            'object' => (object) ['n' => null],
        ]);
        $decoded = Codec::decodeHeader($encoded);

        $this->assertSame([1, 'two'], $decoded['list']);
        $this->assertSame(['k' => 'v'], $decoded['map']);
        $this->assertSame([], $decoded['empty_list']);
        $this->assertSame([], $decoded['empty_map']);
        $this->assertSame(['n' => null], $decoded['object']);
        $this->assertStringContainsString(bin2hex("\xa9empty_map\x80"), bin2hex($encoded));
        $this->assertStringContainsString(bin2hex("\xaaempty_list\x90"), bin2hex($encoded));
    }

    public function testScalarTypesSurvive(): void
    {
        $values = [
            'int' => 42,
            'negative' => -7,
            'big' => PHP_INT_MAX,
            'float' => 1.5,
            'whole_float' => 2.0,
            'true' => true,
            'false' => false,
            'nil' => null,
            'string' => 'héllo',
            'binary' => "\xff\x00\xfe",
        ];

        $this->assertSame($values, Codec::decodeHeader(Codec::encodeMap($values)));
    }

    public function testNonUtf8StringsAreEncodedAsBin(): void
    {
        $this->assertSame('81a162c403ff00fe', bin2hex(Codec::encodeMap(['b' => "\xff\x00\xfe"])));
        $this->assertSame('81a173a3616263', bin2hex(Codec::encodeMap(['s' => 'abc'])));
    }

    public function testDatesBecomeUnixSeconds(): void
    {
        $decoded = Codec::decodeHeader(Codec::encodeMap(['at' => new \DateTimeImmutable('@1767225600')]));

        $this->assertSame(['at' => 1767225600], $decoded);
    }

    public function testRejectsValuesWithoutAMessagePackForm(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Codec::encodeMap(['handle' => fopen('php://memory', 'r')]);
    }
}
