<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Support;

use Temporal\Api\Common\V1\Memo;
use Temporal\Api\Common\V1\Payload;
use Temporal\Api\Common\V1\Payloads;

/**
 * Wire size of the messages the payload size warnings measure.
 *
 * The size is counted from the values instead of serializing the message: producing the bytes of
 * a megabyte of payload costs milliseconds and allocates a copy of it, while counting them is a
 * walk over a handful of strings. The `protobuf` extension offers no way to measure a message,
 * and the pure PHP implementation only offers one that is not always available.
 *
 * @internal
 */
final class MessageSize
{
    /**
     * Field numbers below 16 take a single byte of tag.
     */
    private const TAG_SIZE = 1;

    /**
     * The pure PHP implementation omits a default scalar in a map entry, the extension writes it.
     * Both branches are load-bearing, so the test suite has to run under both implementations.
     */
    private static ?bool $writesMapDefaults = null;

    public static function ofPayloads(?Payloads $payloads): int
    {
        if ($payloads === null) {
            return 0;
        }

        $size = 0;
        foreach ($payloads->getPayloads() as $payload) {
            // repeated Payload payloads = 1
            $size += self::embedded(self::ofPayload($payload));
        }

        return $size;
    }

    public static function ofMemo(?Memo $memo): int
    {
        if ($memo === null) {
            return 0;
        }

        $size = 0;
        foreach ($memo->getFields() as $key => $payload) {
            // map<string, Payload> fields = 1
            $entry = self::mapScalar((string) $key) + self::embedded(self::ofPayload($payload));
            $size += self::embedded($entry);
        }

        return $size;
    }

    public static function ofPayload(?Payload $payload): int
    {
        if ($payload === null) {
            return 0;
        }

        $size = 0;
        foreach ($payload->getMetadata() as $key => $value) {
            // map<string, bytes> metadata = 1
            $entry = self::mapScalar((string) $key) + self::mapScalar((string) $value);
            $size += self::embedded($entry);
        }

        // bytes data = 2, omitted when empty
        $data = \strlen($payload->getData());
        if ($data !== 0) {
            $size += self::embedded($data);
        }

        return $size;
    }

    /**
     * Size of a length delimited field: its tag, its length and its content.
     */
    private static function embedded(int $size): int
    {
        return self::TAG_SIZE + self::varint($size) + $size;
    }

    /**
     * Size of a string or bytes field of a map entry, which is skipped when empty by the pure PHP
     * implementation and written by the extension.
     */
    private static function mapScalar(string $value): int
    {
        $length = \strlen($value);

        return $length === 0 && !self::writesMapDefaults() ? 0 : self::embedded($length);
    }

    /**
     * Bytes a length takes as a varint.
     */
    private static function varint(int $value): int
    {
        $bytes = 1;
        while ($value >= 128) {
            $value >>= 7;
            ++$bytes;
        }

        return $bytes;
    }

    private static function writesMapDefaults(): bool
    {
        if (self::$writesMapDefaults === null) {
            $probe = new Payload();
            /** @psalm-suppress InvalidArgument The values of the map are bytes, not messages */
            $probe->getMetadata()['k'] = '';

            // `0a 03 0a 01 6b` when the empty value is skipped, two bytes longer when it is not
            self::$writesMapDefaults = \strlen($probe->serializeToString()) > 5;
        }

        return self::$writesMapDefaults;
    }
}
