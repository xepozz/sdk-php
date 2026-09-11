<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Client;

/**
 * Payload size limits at which the SDK logs a warning before sending a request to the server.
 *
 * The server rejects requests with oversized payloads, but by default it only logs the problem
 * on its own side, which is invisible to Temporal Cloud users. These limits make the SDK warn
 * about such payloads locally.
 *
 * @see \Temporal\Client\ClientOptions::withPayloadLimits()
 *
 * @experimental This API is experimental and may change in the future.
 */
final class PayloadLimitOptions
{
    /**
     * Default limit (in bytes) at which a payload size warning is logged.
     */
    public const DEFAULT_PAYLOAD_SIZE_WARNING = 512 * 1024;

    /**
     * Default limit (in bytes) at which an aggregate memo size warning is logged.
     */
    public const DEFAULT_MEMO_SIZE_WARNING = 2 * 1024;

    /**
     * @param null|positive-int $payloadSizeWarning Limit in bytes at which a payload size warning
     *        is logged. NULL disables the warning.
     * @param null|positive-int $memoSizeWarning Limit in bytes at which an aggregate memo size
     *        warning is logged. NULL disables the warning.
     */
    public function __construct(
        public readonly ?int $payloadSizeWarning = self::DEFAULT_PAYLOAD_SIZE_WARNING,
        public readonly ?int $memoSizeWarning = self::DEFAULT_MEMO_SIZE_WARNING,
    ) {
        self::assertPositive($payloadSizeWarning, 'payloadSizeWarning');
        self::assertPositive($memoSizeWarning, 'memoSizeWarning');
    }

    public static function new(): self
    {
        return new self();
    }

    /**
     * Limit in bytes at which a payload size warning is logged.
     *
     * @param null|positive-int $bytes NULL disables the warning.
     */
    public function withPayloadSizeWarning(?int $bytes): self
    {
        return new self($bytes, $this->memoSizeWarning);
    }

    /**
     * Limit in bytes at which an aggregate memo size warning is logged.
     *
     * @param null|positive-int $bytes NULL disables the warning.
     */
    public function withMemoSizeWarning(?int $bytes): self
    {
        return new self($this->payloadSizeWarning, $bytes);
    }

    /**
     * Whether any of the limits is set.
     */
    public function isEnabled(): bool
    {
        return $this->payloadSizeWarning !== null || $this->memoSizeWarning !== null;
    }

    private static function assertPositive(?int $value, string $name): void
    {
        $value === null || $value > 0 or throw new \InvalidArgumentException(
            "`$name` must be a positive number of bytes or NULL to disable the warning.",
        );
    }
}
