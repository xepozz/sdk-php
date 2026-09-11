<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Client;

use Google\Protobuf\Internal\Message;
use Psr\Log\LoggerInterface;
use Temporal\Api\Common\V1\Memo;
use Temporal\Api\Common\V1\Payloads;
use Temporal\Api\Common\V1\SearchAttributes;
use Temporal\Api\Schedule\V1\Schedule;
use Temporal\Api\Workflowservice\V1\CreateScheduleRequest;
use Temporal\Api\Workflowservice\V1\QueryWorkflowRequest;
use Temporal\Api\Workflowservice\V1\RecordActivityTaskHeartbeatByIdRequest;
use Temporal\Api\Workflowservice\V1\RecordActivityTaskHeartbeatRequest;
use Temporal\Api\Workflowservice\V1\RespondActivityTaskCanceledByIdRequest;
use Temporal\Api\Workflowservice\V1\RespondActivityTaskCanceledRequest;
use Temporal\Api\Workflowservice\V1\RespondActivityTaskCompletedByIdRequest;
use Temporal\Api\Workflowservice\V1\RespondActivityTaskCompletedRequest;
use Temporal\Api\Workflowservice\V1\RespondActivityTaskFailedByIdRequest;
use Temporal\Api\Workflowservice\V1\RespondActivityTaskFailedRequest;
use Temporal\Api\Workflowservice\V1\SignalWithStartWorkflowExecutionRequest;
use Temporal\Api\Workflowservice\V1\SignalWorkflowExecutionRequest;
use Temporal\Api\Workflowservice\V1\StartWorkflowExecutionRequest;
use Temporal\Api\Workflowservice\V1\TerminateWorkflowExecutionRequest;
use Temporal\Api\Workflowservice\V1\UpdateScheduleRequest;
use Temporal\Api\Workflowservice\V1\UpdateWorkflowExecutionRequest;
use Temporal\Common\PayloadLimitOptions;

/**
 * Warns when an outgoing gRPC request carries payloads larger than the configured limits.
 *
 * Sizes are measured the same way the server measures them: a `Payloads` or `Memo` message is
 * measured as a whole, while `map<string, Payload>` fields are measured as the sum of key lengths
 * and payload data lengths.
 *
 * Requests are inspected field by field rather than by walking protobuf descriptors: the
 * descriptor API differs between the pure PHP implementation and the `protobuf` extension.
 *
 * @internal
 */
final class PayloadSizeChecker
{
    /**
     * Message code used by all the SDKs for the payload size warning.
     */
    private const MESSAGE_CODE = 'TMPRL1103';

    public function __construct(
        private readonly PayloadLimitOptions $limits,
        private readonly LoggerInterface $logger,
    ) {}

    /**
     * @param non-empty-string $method RPC method name.
     */
    public function check(string $method, object $request): void
    {
        if (!$this->limits->isEnabled()) {
            return;
        }

        try {
            $this->inspect($method, $request);
        } catch (\Throwable) {
            // Measuring is an observability feature: it must never break the RPC call
        }
    }

    /**
     * Size of a message as the server sees it on the wire.
     */
    private static function sizeOf(?Message $message): int
    {
        return $message === null ? 0 : \strlen($message->serializeToString());
    }

    /**
     * The server measures `map<string, Payload>` as the sum of key and payload data lengths.
     */
    private static function sizeOfSearchAttributes(?SearchAttributes $attributes): int
    {
        $size = 0;
        foreach ($attributes?->getIndexedFields() ?? [] as $key => $payload) {
            $size += \strlen((string) $key) + \strlen($payload->getData());
        }

        return $size;
    }

    private function inspect(string $method, object $request): void
    {
        switch (true) {
            case $request instanceof StartWorkflowExecutionRequest:
                $this->payloads($method, $request->getInput());
                $this->memo($method, $request->getMemo());
                $this->searchAttributes($method, $request->getSearchAttributes());
                return;

            case $request instanceof SignalWithStartWorkflowExecutionRequest:
                $this->payloads($method, $request->getInput());
                $this->payloads($method, $request->getSignalInput());
                $this->memo($method, $request->getMemo());
                $this->searchAttributes($method, $request->getSearchAttributes());
                return;

            case $request instanceof SignalWorkflowExecutionRequest:
                $this->payloads($method, $request->getInput());
                return;

            case $request instanceof UpdateWorkflowExecutionRequest:
                $this->payloads($method, $request->getRequest()?->getInput()?->getArgs());
                return;

            case $request instanceof QueryWorkflowRequest:
                $this->payloads($method, $request->getQuery()?->getQueryArgs());
                return;

            case $request instanceof RespondActivityTaskCompletedRequest:
            case $request instanceof RespondActivityTaskCompletedByIdRequest:
                $this->payloads($method, $request->getResult());
                return;

            case $request instanceof RespondActivityTaskFailedRequest:
            case $request instanceof RespondActivityTaskFailedByIdRequest:
                // A failure carries payloads in its details; it is measured as a whole
                $this->warn(
                    $method,
                    'payloads',
                    self::sizeOf($request->getFailure()),
                    $this->limits->payloadSizeWarning,
                );
                return;

            case $request instanceof RespondActivityTaskCanceledRequest:
            case $request instanceof RespondActivityTaskCanceledByIdRequest:
            case $request instanceof RecordActivityTaskHeartbeatRequest:
            case $request instanceof RecordActivityTaskHeartbeatByIdRequest:
            case $request instanceof TerminateWorkflowExecutionRequest:
                $this->payloads($method, $request->getDetails());
                return;

            case $request instanceof CreateScheduleRequest:
                // The server checks the memo and the workflow input of a schedule as one value
                $this->warn(
                    $method,
                    'payloads',
                    self::sizeOf($request->getMemo()) + $this->scheduleInputSize($request->getSchedule()),
                    $this->limits->payloadSizeWarning,
                );
                $this->searchAttributes($method, $request->getSearchAttributes());
                return;

            case $request instanceof UpdateScheduleRequest:
                $this->warn(
                    $method,
                    'payloads',
                    $this->scheduleInputSize($request->getSchedule()),
                    $this->limits->payloadSizeWarning,
                );
                return;
        }
    }

    private function scheduleInputSize(?Schedule $schedule): int
    {
        $action = $schedule?->getAction()?->getStartWorkflow();

        return self::sizeOf($action?->getInput()) + self::sizeOf($action?->getMemo());
    }

    private function payloads(string $method, ?Payloads $payloads): void
    {
        $this->warn($method, 'payloads', self::sizeOf($payloads), $this->limits->payloadSizeWarning);
    }

    private function memo(string $method, ?Memo $memo): void
    {
        $this->warn($method, 'memo', self::sizeOf($memo), $this->limits->memoSizeWarning);
    }

    private function searchAttributes(string $method, ?SearchAttributes $attributes): void
    {
        $this->warn(
            $method,
            'payloads',
            self::sizeOfSearchAttributes($attributes),
            $this->limits->payloadSizeWarning,
        );
    }

    /**
     * @param non-empty-string $kind
     */
    private function warn(string $method, string $kind, int $size, ?int $limit): void
    {
        if ($limit === null || $size <= $limit) {
            return;
        }

        $this->logger->warning(
            \sprintf(
                '[%s] Attempted to send a %s with size that exceeded the warning limit.',
                self::MESSAGE_CODE,
                $kind,
            ),
            ['method' => $method, 'size' => $size, 'limit' => $limit],
        );
    }
}
