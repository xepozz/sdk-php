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
use Temporal\Internal\Transport\Request\ExecuteLocalActivity;
use Temporal\Worker\Environment\EnvironmentInterface;
use Temporal\Worker\Transport\Command\RequestInterface;

/**
 * Warns when a command produced by a Workflow carries payloads larger than the configured limit.
 *
 * The payloads are measured the same way the server measures them, and the conversion result is
 * reused when the command is encoded, so the check does not serialize the values twice. Replayed
 * commands are skipped entirely: they are not sent to the server, so there is nothing to warn about.
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
        private readonly EnvironmentInterface $env,
        private readonly LoggerInterface $logger,
    ) {}

    public function check(RequestInterface $request): void
    {
        $limit = $this->limits->payloadSizeWarning;
        // A replayed command is not sent anywhere, so it is never measured nor reported,
        // the same way the other SDKs check the payloads only when the request is sent.
        if ($limit === null || $this->env->isReplaying()) {
            return;
        }

        // Local Activity arguments are not sent to the server
        if ($request->getName() === ExecuteLocalActivity::NAME) {
            return;
        }

        try {
            $payloads = $request->getPayloads();
            if ($payloads->count() === 0) {
                return;
            }

            $payloads->setDataConverter($this->converter);
            // `byteSize()` is not available when the protobuf extension is used
            $size = \strlen($payloads->toPayloads()->serializeToString());
        } catch (\Throwable) {
            // Measuring is an observability feature: it must not affect the Workflow in any way.
            // A value that cannot be converted fails later, in the codec, as it did before.
            return;
        }

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
