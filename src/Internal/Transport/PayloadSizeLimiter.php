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
use Temporal\Client\WorkflowClient;
use Temporal\DataConverter\DataConverterInterface;
use Temporal\Exception\PayloadSizeExceededException;
use Temporal\Internal\Support\CommandPayloads;
use Temporal\Worker\Transport\Command\CommandInterface;

/**
 * Keeps a Worker from sending payloads the server is known to reject.
 *
 * The limits are the ones the namespace reports, and the whole batch of commands is measured
 * before any of it leaves the Worker, so an oversized payload fails the Workflow Task instead of
 * being uploaded: the Workflow keeps its history and can be fixed, and the bytes are never paid
 * for. The batch is measured as a whole rather than each command as it is produced, because a
 * Workflow must not be able to catch the failure and carry on without the command it asked for.
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
     * @param int $payloadSize Limit in bytes, zero when the namespace reports none.
     * @param int $memoSize Limit in bytes, zero when the namespace reports none.
     */
    private function __construct(
        private readonly int $payloadSize,
        private readonly int $memoSize,
        private readonly DataConverterInterface $converter,
    ) {}

    /**
     * Ask the namespace for the limits it enforces.
     *
     * NULL when there is no Client to ask, when the namespace reports no limits, or when it
     * cannot be reached: the payloads are then sent and the server rejects them, as before.
     */
    public static function fromClient(?WorkflowClient $client, DataConverterInterface $converter): ?self
    {
        if ($client === null) {
            return null;
        }

        try {
            $serviceClient = $client->getServiceClient();
            $context = $serviceClient->getContext();
            $namespace = $context->getMetadata()['Temporal-Namespace'][0] ?? null;

            $info = $serviceClient->DescribeNamespace(
                (new DescribeNamespaceRequest())->setNamespace((string) $namespace),
                // A Worker must not hang on a server that does not answer
                $context->withTimeout(self::LOOKUP_TIMEOUT),
            )->getNamespaceInfo();

            // The limits were added to the API later than the version the DTO package pins
            $limits = $info !== null && \method_exists($info, 'getLimits') ? $info->getLimits() : null;
            if ($limits === null) {
                return null;
            }

            $self = new self(
                (int) $limits->getBlobSizeLimitError(),
                (int) $limits->getMemoSizeLimitError(),
                $converter,
            );
        } catch (\Throwable) {
            // A Worker that cannot ask keeps working the way it did before the limits existed
            return null;
        }

        return $self->payloadSize > 0 || $self->memoSize > 0 ? $self : null;
    }

    /**
     * @param iterable<CommandInterface> $commands
     * @return list<CommandInterface> The same commands, measured.
     *
     * @throws PayloadSizeExceededException
     */
    public function check(iterable $commands): array
    {
        $result = [];
        foreach ($commands as $command) {
            $result[] = $command;

            $sizes = CommandPayloads::sizes($command, $this->converter);

            $this->assert('payloads', $sizes['payloads'], $this->payloadSize);
            $this->assert('memo', $sizes['memo'], $this->memoSize);
        }

        return $result;
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
