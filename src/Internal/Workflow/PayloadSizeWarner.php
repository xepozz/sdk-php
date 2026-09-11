<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Workflow;

use Psr\Log\LoggerInterface;
use Temporal\Common\PayloadLimitOptions;
use Temporal\DataConverter\DataConverterInterface;
use Temporal\Worker\Transport\Command\RequestInterface;

/**
 * Warns when a command produced by a Workflow carries payloads larger than the configured limit.
 *
 * The payloads are measured the same way the server measures them, and the conversion result is
 * reused when the command is encoded, so the check does not serialize the values twice.
 *
 * @internal
 */
final class PayloadSizeWarner
{
    /**
     * Message code used by all the SDKs for the payload size warning.
     */
    private const MESSAGE_CODE = 'TMPRL1103';

    public function __construct(
        private readonly PayloadLimitOptions $limits,
        private readonly DataConverterInterface $converter,
        private readonly LoggerInterface $logger,
    ) {}

    public function check(RequestInterface $request): void
    {
        $limit = $this->limits->payloadSizeWarning;
        if ($limit === null) {
            return;
        }

        $payloads = $request->getPayloads();
        if ($payloads->count() === 0) {
            return;
        }

        $payloads->setDataConverter($this->converter);
        $size = $payloads->toPayloads()->byteSize();

        if ($size <= $limit) {
            return;
        }

        $this->logger->warning(
            \sprintf(
                '[%s] Attempted to send payloads with size that exceeded the warning limit.',
                self::MESSAGE_CODE,
            ),
            ['command' => $request->getName(), 'size' => $size, 'limit' => $limit],
        );
    }
}
