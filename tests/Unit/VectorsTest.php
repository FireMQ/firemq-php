<?php

declare(strict_types=1);

namespace FireMQ\Tests\Unit;

use FireMQ\Protocol\Codec;
use FireMQ\Protocol\Op;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Decodes every frame in spec/test-vectors/frames.json (copied to tests/fixtures).
 */
final class VectorsTest extends TestCase
{
    /**
     * @return iterable<string, array{array<string, mixed>}>
     */
    public static function vectors(): iterable
    {
        $file = dirname(__DIR__) . '/fixtures/frames.json';
        $data = json_decode((string) file_get_contents($file), true, 512, JSON_THROW_ON_ERROR);
        foreach ($data['vectors'] as $vector) {
            yield $vector['name'] => [$vector];
        }
    }

    public function testEveryOpHasAVector(): void
    {
        $ops = [];
        foreach (self::vectors() as [$vector]) {
            $ops[$vector['op']] = true;
        }
        $this->assertCount(27, $ops);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('vectors')]
    public function testDecodesVector(array $vector): void
    {
        $bytes = (string) hex2bin($vector['frame_hex']);
        $frame = Codec::decodeFrame($bytes);

        $this->assertNotNull($frame);
        $this->assertSame($vector['op'], $frame->op);
        $this->assertSame($vector['op_name'], Op::name($frame->op));
        $this->assertEquals($vector['header'], $frame->header);
        $this->assertSame($vector['payload_hex'], bin2hex($frame->payload));
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('vectors')]
    public function testRoundTripsVector(array $vector): void
    {
        $frame = Codec::decodeFrame((string) hex2bin($vector['frame_hex']));
        $this->assertNotNull($frame);

        $again = Codec::decodeFrame($frame->encode());

        $this->assertNotNull($again);
        $this->assertSame($frame->op, $again->op);
        $this->assertEquals($frame->header, $again->header);
        $this->assertSame($frame->payload, $again->payload);
    }

    /**
     * @param array<string, mixed> $vector
     */
    #[DataProvider('vectors')]
    public function testWaitsForTheWholeFrame(array $vector): void
    {
        $bytes = (string) hex2bin($vector['frame_hex']);

        $this->assertNull(Codec::decodeFrame(substr($bytes, 0, -1)));
    }

    public function testEncodesSmallFramesByteForByte(): void
    {
        $this->assertSame('000000050300000000', bin2hex(Codec::encodeFrame(Op::PING)));
        $this->assertSame('000000050400000000', bin2hex(Codec::encodeFrame(Op::PONG)));
        $this->assertSame('0000000c210000000781a47570746f01', bin2hex(Codec::encodeFrame(Op::CONFIRM, ['upto' => 1])));
        $this->assertSame(
            '00000015350000001082a374616705a86d756c7469706c65c3',
            bin2hex(Codec::encodeFrame(Op::ACK, ['tag' => 5, 'multiple' => true])),
        );
        $this->assertSame(
            '00000028380000001f84a372696408a374616709a36b6579a46a6f6273a972656d61696e696e6703776f726b',
            bin2hex(Codec::encodeFrame(Op::GET_OK, ['rid' => 8, 'tag' => 9, 'key' => 'jobs', 'remaining' => 3], 'work')),
        );
    }
}
