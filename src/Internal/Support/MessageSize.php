<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Support;

use Google\Protobuf\Internal\Message;

/**
 * Wire size of a protobuf message, measured the cheapest way the current protobuf
 * implementation allows.
 *
 * The pure PHP implementation can count the bytes without producing them, which is orders of
 * magnitude cheaper for large payloads; the `protobuf` extension has no such method, but its
 * serialization is fast enough to be used instead.
 *
 * @internal
 */
final class MessageSize
{
    private static ?bool $canCount = null;

    public static function of(Message $message): int
    {
        self::$canCount ??= \method_exists($message, 'byteSize');

        /** @psalm-suppress UndefinedMethod */
        return self::$canCount
            ? (int) $message->byteSize()
            : \strlen($message->serializeToString());
    }
}
