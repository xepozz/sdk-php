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
 * Payloads larger than the error limit are not sent to the server at all.
 *
 * A Client request throws it instead of being sent; a Workflow Task fails instead of being
 * completed with the oversized payloads, so the Workflow can be fixed and the task retried.
 *
 * @experimental This API is experimental and may change in the future.
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
            '[%s] Attempted to upload %s with size that exceeded the error limit.',
            self::MESSAGE_CODE,
            $kind,
        ));
    }
}
