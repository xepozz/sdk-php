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
use Temporal\Worker\Environment\EnvironmentInterface;
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
 * result into a failed Query rather than failing the task, which a Worker cannot do from here.
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
     * @param int $memoSize Limit in bytes, zero when the namespace enforces none.
     */
    public function __construct(
        private readonly int $payloadSize,
        private readonly int $memoSize,
        private readonly DataConverterInterface $converter,
        private readonly EnvironmentInterface $env,
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
        EnvironmentInterface $env,
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
        $memoSize = (int) $limits?->getMemoSizeLimitError();

        return $payloadSize > 0 || $memoSize > 0
            ? new self($payloadSize, $memoSize, $converter, $env)
            : null;
    }

    /**
     * @param iterable<CommandInterface> $commands
     * @return list<CommandInterface> The same commands: the caller sends them on.
     *
     * @throws PayloadSizeExceededException
     */
    public function enforce(iterable $commands): array
    {
        // The queue of a Worker drains as it is read, so nothing may be measured before it is kept
        $commands = \is_array($commands) ? \array_values($commands) : \iterator_to_array($commands, false);

        // A replayed command is not sent to the server, so its size cannot fail anything
        if ($this->env->isReplaying()) {
            return $commands;
        }

        foreach ($commands as $command) {
            if (!$command instanceof RequestInterface) {
                continue;
            }

            $sizes = CommandPayloads::sizes(
                $command,
                $this->converter,
                payloads: $this->payloadSize > 0,
                memo: $this->memoSize > 0,
            );

            $this->assert('payloads', $sizes['payloads'], $this->payloadSize);
            $this->assert('memo', $sizes['memo'], $this->memoSize);
        }

        return $commands;
    }

    /**
     * @param non-empty-string $kind
     *
     * @throws PayloadSizeExceededException
     */
    private function assert(string $kind, int $size, int $limit): void
    {
        if ($limit > 0 && $size > $limit) {
            throw new PayloadSizeExceededException($kind, $size, $limit);
        }
    }
}
