<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\Internal\Support;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Temporal\Api\Common\V1\Memo;
use Temporal\Api\Common\V1\Payload;
use Temporal\Api\Common\V1\Payloads;
use Temporal\Internal\Support\MessageSize;

/**
 * The size is counted instead of being produced, so every shape must agree with the bytes the
 * protobuf implementation in use actually writes.
 */
final class MessageSizeTestCase extends TestCase
{
    /**
     * @return iterable<string, array{\Closure(): Payload}>
     */
    public static function payloads(): iterable
    {
        yield 'empty' => [static fn() => new Payload()];
        yield 'data only' => [static fn() => (new Payload())->setData('ab')];
        yield 'empty data' => [static fn() => (new Payload())->setData('')];
        yield 'metadata only' => [static fn() => self::payload(['encoding' => 'json/plain'], '')];
        yield 'metadata and data' => [static fn() => self::payload(['encoding' => 'json/plain'], '"x"')];
        yield 'several metadata keys' => [
            static fn() => self::payload(['encoding' => 'binary/protobuf', 'messageType' => 'Foo'], 'bin'),
        ];
        yield 'empty metadata value' => [static fn() => self::payload(['encoding' => ''], 'x')];
        yield 'empty metadata key' => [static fn() => self::payload(['' => 'v'], 'x')];
        yield 'empty metadata key and value' => [static fn() => self::payload(['' => ''], '')];
        yield 'binary data' => [static fn() => (new Payload())->setData(\random_bytes(300))];

        // The length of a field is a varint: the size of the prefix changes on every 7 bits
        foreach ([1, 126, 127, 128, 129, 16_382, 16_383, 16_384, 16_385, 2_097_152] as $length) {
            yield "data of $length bytes" => [
                static fn() => self::payload(['encoding' => 'binary/null'], \str_repeat('x', $length)),
            ];
        }
    }

    #[DataProvider('payloads')]
    public function testPayloadSize(\Closure $payload): void
    {
        $payload = $payload();

        self::assertSame(\strlen($payload->serializeToString()), MessageSize::ofPayload($payload));
    }

    #[DataProvider('payloads')]
    public function testPayloadsSize(\Closure $payload): void
    {
        $payloads = new Payloads(['payloads' => [$payload(), new Payload(), $payload()]]);

        self::assertSame(\strlen($payloads->serializeToString()), MessageSize::ofPayloads($payloads));
    }

    #[DataProvider('payloads')]
    public function testMemoSize(\Closure $payload): void
    {
        $memo = new Memo();
        $memo->getFields()['key'] = $payload();
        $memo->getFields()[''] = new Payload();

        self::assertSame(\strlen($memo->serializeToString()), MessageSize::ofMemo($memo));
    }

    public function testEmptyMessages(): void
    {
        self::assertSame(0, MessageSize::ofPayloads(null));
        self::assertSame(0, MessageSize::ofMemo(null));
        self::assertSame(0, MessageSize::ofPayload(null));
        self::assertSame(0, MessageSize::ofPayloads(new Payloads()));
        self::assertSame(0, MessageSize::ofMemo(new Memo()));
    }

    public function testManyPayloads(): void
    {
        $list = [];
        for ($i = 0; $i < 50; ++$i) {
            $list[] = self::payload(['encoding' => 'json/plain'], \str_repeat('x', $i * 37));
        }

        $payloads = new Payloads(['payloads' => $list]);

        self::assertSame(\strlen($payloads->serializeToString()), MessageSize::ofPayloads($payloads));
    }

    /**
     * @param array<string, string> $metadata
     */
    private static function payload(array $metadata, string $data): Payload
    {
        $payload = (new Payload())->setData($data);
        foreach ($metadata as $key => $value) {
            $payload->getMetadata()[$key] = $value;
        }

        return $payload;
    }
}
