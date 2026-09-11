<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Transport;

use Temporal\Common\PayloadLimitOptions;
use Temporal\DataConverter\DataConverterInterface;
use Temporal\Exception\PayloadSizeExceededException;
use Temporal\Internal\Support\CommandPayloads;
use Temporal\Worker\Transport\Command\CommandInterface;
use Temporal\Worker\WorkerInterface;

/**
 * Keeps a Worker from sending payloads the server is known to reject.
 *
 * The whole batch of commands is measured before any of it leaves the Worker, so an oversized
 * payload fails the Workflow Task instead of being uploaded: the Workflow keeps its history and
 * can be fixed, and the bytes are never paid for.
 *
 * It runs on the batch rather than on each command as it is produced, because a Workflow must not
 * be able to catch the failure and carry on without the command it asked for.
 *
 * @internal
 */
final class PayloadSizeLimiter
{
    public function __construct(
        private readonly PayloadLimitOptions $limits,
        private readonly DataConverterInterface $converter,
    ) {}

    /**
     * NULL when the Worker sends everything it produces and lets the server reject it.
     */
    public static function forWorker(WorkerInterface $worker, DataConverterInterface $converter): ?self
    {
        $options = $worker->getOptions();
        $limits = $options->getPayloadLimits();

        // The Worker delegates the limits to RoadRunner when they are disabled here
        return $options->disablePayloadErrorLimit || !$limits->hasErrorLimits()
            ? null
            : new self($limits, $converter);
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

            $this->assert('payloads', $sizes['payloads'], $this->limits->payloadSizeError);
            $this->assert('memo', $sizes['memo'], $this->limits->memoSizeError);
        }

        return $result;
    }

    /**
     * @param non-empty-string $kind
     *
     * @throws PayloadSizeExceededException
     */
    private function assert(string $kind, int $size, ?int $limit): void
    {
        if ($limit !== null && $size > $limit) {
            throw new PayloadSizeExceededException($kind, $size, $limit);
        }
    }
}
