<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Exception;

/**
 * Payloads larger than the limit the namespace enforces are not sent to the server at all:
 * the Workflow Task fails instead, so the Workflow can be fixed and the task retried.
 *
 * @internal
 */
final class PayloadSizeExceededException extends TemporalException
{
    /**
     * Message code used by all the SDKs for the payload size limits.
     */
    private const MESSAGE_CODE = 'TMPRL1103';

    /**
     * @param non-empty-string $kind What was measured, `payloads` or `memo`.
     * @param int $size Size of the value, in bytes.
     * @param int $limit Limit it exceeded, in bytes.
     */
    public function __construct(
        string $kind,
        public readonly int $size,
        public readonly int $limit,
    ) {
        parent::__construct(\sprintf(
            '[%s] Attempted to upload %s with size that exceeded the error limit. Size: %d, limit: %d.',
            self::MESSAGE_CODE,
            $kind,
            $size,
            $limit,
        ));
    }

    /**
     * A Worker reports a failure as a string, and it ends up in the history of the Workflow:
     * the message alone belongs there, not the file and the stack of the SDK.
     */
    public function __toString(): string
    {
        return $this->getMessage();
    }
}
