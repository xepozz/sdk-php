<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Transport;

use Temporal\Api\Workflowservice\V1\DescribeNamespaceRequest;
use Temporal\Client\WorkflowClientInterface;
use Temporal\DataConverter\DataConverterInterface;
use Temporal\Exception\PayloadSizeExceededException;
use Temporal\Internal\Support\CommandPayloads;
use Temporal\Worker\Transport\Command\CommandInterface;
use Temporal\Worker\Transport\Command\RequestInterface;

/**
 * Keeps a Worker from sending payloads the namespace is known to reject.
 *
 * The commands of a Workflow Task are measured as a whole once the task is over, so the task
 * fails instead of being completed with an oversized payload. Measuring each command as it is
 * produced would let the Workflow catch the failure and carry on without the command it asked
 * for.
 *
 * Responses to Queries and Updates are left to the server: the Go SDK turns an oversized Query
 * result into a failed Query rather than failing the task, and failing the task instead would be
 * worse than what the server does with it.
 *
 * Only the payloads a Workflow produces are measured. A memo and the Search Attributes travel as
 * raw values and are converted by RoadRunner, so the size measured here is an estimate: it is
 * good enough to warn about, but not to refuse to send. Those the server rejects on its own.
 *
 * @internal
 */
final class PayloadSizeLimiter
{
    /**
     * Seconds the namespace lookup may take, the same as the default RPC timeout of the Go SDK.
     */
    private const LOOKUP_TIMEOUT = 10;

    /**
     * @param int $payloadSize Limit in bytes, zero when the namespace enforces none.
     */
    public function __construct(
        private readonly int $payloadSize,
        private readonly DataConverterInterface $converter,
    ) {}

    /**
     * Ask the namespace for the limits it enforces.
     *
     * NULL when the namespace enforces none, or when it cannot be asked: the payloads are then
     * sent and the server rejects them, the way it worked before the limits existed.
     */
    public static function fromClient(
        WorkflowClientInterface $client,
        DataConverterInterface $converter,
    ): ?self {
        try {
            $serviceClient = $client->getServiceClient();
            $context = $serviceClient->getContext();

            $limits = $serviceClient->DescribeNamespace(
                (new DescribeNamespaceRequest())
                    // The namespace the Client is bound to, as it sends it with every request
                    ->setNamespace((string) ($context->getMetadata()['Temporal-Namespace'][0] ?? '')),
                // A Worker must not hang at startup on a server that does not answer
                $context->withTimeout(self::LOOKUP_TIMEOUT),
            )->getNamespaceInfo()?->getLimits();
        } catch (\Throwable) {
            return null;
        }

        $payloadSize = (int) $limits?->getBlobSizeLimitError();

        return $payloadSize > 0 ? new self($payloadSize, $converter) : null;
    }

    /**
     * @param iterable<CommandInterface> $commands
     * @param bool $replaying Whether any part of the batch was replayed: a replayed command is
     *        matched against the history instead of being sent, so its size fails nothing.
     * @return list<CommandInterface> The same commands: the caller sends them on.
     *
     * @throws PayloadSizeExceededException
     */
    public function enforce(iterable $commands, bool $replaying = false): array
    {
        // The queue of a Worker drains as it is read, so nothing may be measured before it is kept
        $commands = \is_array($commands) ? \array_values($commands) : \iterator_to_array($commands, false);

        if ($replaying || $this->payloadSize <= 0) {
            return $commands;
        }

        foreach ($commands as $command) {
            if (!$command instanceof RequestInterface) {
                continue;
            }

            $size = CommandPayloads::wireSize($command, $this->converter);

            if ($size > $this->payloadSize) {
                throw new PayloadSizeExceededException('payloads', $size, $this->payloadSize);
            }
        }

        return $commands;
    }
}
