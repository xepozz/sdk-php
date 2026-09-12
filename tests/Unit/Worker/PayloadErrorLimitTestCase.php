<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\Worker;

use PHPUnit\Framework\TestCase;
use Temporal\Common\Uuid;
use Temporal\DataConverter\DataConverter;
use Temporal\DataConverter\EncodedValues;
use Temporal\Exception\PayloadSizeExceededException;
use Temporal\Api\PBNamespace\V1\NamespaceInfo;
use Temporal\Api\Workflowservice\V1\DescribeNamespaceRequest;
use Temporal\Api\Workflowservice\V1\DescribeNamespaceResponse;
use Temporal\Api\Workflowservice\V1\WorkflowServiceClient;
use Temporal\Client\GRPC\Connection\ConnectionState;
use Temporal\Client\GRPC\ServiceClient;
use Temporal\Client\WorkflowClient;
use Temporal\Worker\Transport\CommandBatch;
use Temporal\Worker\Transport\HostConnectionInterface;
use Temporal\Worker\Transport\RPCConnectionInterface;
use Temporal\Worker\WorkerOptions;
use Temporal\WorkerFactory;
use Temporal\Tests\Unit\Worker\Stub\BigResultWorkflow;

/**
 * The limits are enforced on the way out of a Worker, so what a Worker sends is what is asserted.
 */
final class PayloadErrorLimitTestCase extends TestCase
{
    public function testAnOversizedCommandFailsTheTask(): void
    {
        $host = $this->runWorkflow($this->factory(payloadSize: 1024));

        self::assertSame([], $host->sent, 'Nothing of the task is sent.');
        self::assertInstanceOf(PayloadSizeExceededException::class, $host->failure);
        self::assertStringContainsString('[TMPRL1103]', $host->failure->getMessage());
        self::assertStringContainsString('CompleteWorkflow', $host->failure->getMessage());
        self::assertSame(1024, $host->failure->limit, 'The limit is the one the namespace reports.');
    }

    public function testACommandBelowTheLimitIsSent(): void
    {
        $host = $this->runWorkflow($this->factory(payloadSize: 1024 * 1024));

        self::assertNull($host->failure);
        self::assertCount(1, $host->sent);
        self::assertStringContainsString('CompleteWorkflow', $host->sent[0]);
    }

    public function testWithoutLimitsTheCommandIsSent(): void
    {
        $host = $this->runWorkflow($this->factory(payloadSize: 0));

        self::assertNull($host->failure);
        self::assertCount(1, $host->sent);
    }

    public function testAWorkerCanLeaveTheEnforcementToRoadRunner(): void
    {
        $host = $this->runWorkflow(
            $this->factory(payloadSize: 1024),
            WorkerOptions::new()->withDisablePayloadErrorLimit(true),
        );

        self::assertNull($host->failure);
        self::assertCount(1, $host->sent);
    }

    public function testAReplayedTaskIsNotMeasured(): void
    {
        $host = $this->runWorkflow($this->factory(payloadSize: 1024), replaying: true);

        self::assertNull($host->failure);
        self::assertCount(1, $host->sent);
    }

    public function testABatchOfNoWorkerIsNotMeasured(): void
    {
        $host = $this->runWorkflow($this->factory(payloadSize: 1024), taskQueue: 'not-registered');

        self::assertNull($host->failure);
    }

    /**
     * Runs one Workflow Task through the Worker loop and returns what the Worker did with it.
     */
    private function runWorkflow(
        WorkerFactory $factory,
        ?WorkerOptions $options = null,
        bool $replaying = false,
        string $taskQueue = 'default',
    ): object {
        $factory->newWorker('default', $options)->registerWorkflowTypes(BigResultWorkflow::class);

        $host = $this->host($this->startWorkflow(), $replaying, $taskQueue);
        $factory->run($host);

        return $host;
    }

    /**
     * One `StartWorkflow` message, as RoadRunner sends it.
     */
    private function startWorkflow(): string
    {
        $runId = Uuid::v4();
        $payloads = EncodedValues::fromValues([]);
        $payloads->setDataConverter(DataConverter::createDefault());

        return (string) \json_encode([[
            'command' => 'StartWorkflow',
            'options' => [
                'info' => [
                    'WorkflowExecution' => ['ID' => Uuid::v4(), 'RunID' => $runId],
                    'WorkflowType' => ['Name' => 'BigResultWorkflow'],
                    'TaskQueueName' => 'default',
                ],
            ],
            'payloads' => \base64_encode($payloads->toPayloads()->serializeToString()),
        ]]);
    }

    private function host(string $batch, bool $replaying, string $taskQueue): object
    {
        return new class($batch, $replaying, $taskQueue) implements HostConnectionInterface {
            /** @var list<string> */
            public array $sent = [];
            public ?\Throwable $failure = null;
            private bool $delivered = false;

            public function __construct(
                private readonly string $batch,
                private readonly bool $replaying,
                private readonly string $taskQueue,
            ) {}

            public function waitBatch(): ?CommandBatch
            {
                if ($this->delivered) {
                    return null;
                }

                $this->delivered = true;

                return new CommandBatch($this->batch, [
                    'taskQueue' => $this->taskQueue,
                    'replay' => $this->replaying,
                    'tickTime' => (new \DateTimeImmutable())->format(\DateTimeInterface::RFC3339),
                ]);
            }

            public function send(string $frame): void
            {
                $this->sent[] = $frame;
            }

            public function error(\Throwable $error): void
            {
                $this->failure = $error;
            }
        };
    }

    /**
     * A Worker factory whose namespace reports the given limit.
     */
    private function factory(int $payloadSize): WorkerFactory
    {
        return WorkerFactory::create(
            converter: DataConverter::createDefault(),
            rpc: $this->createMock(RPCConnectionInterface::class),
            client: new WorkflowClient($this->serviceClient($payloadSize)),
        );
    }

    /**
     * A service client that answers `DescribeNamespace` with the given limit and nothing else.
     */
    private function serviceClient(int $payloadSize): ServiceClient
    {
        $stub = static fn() => new class($payloadSize) extends WorkflowServiceClient {
            public function __construct(private readonly int $payloadSize) {}

            public function getConnectivityState($try_to_connect = false): int
            {
                return ConnectionState::Ready->value;
            }

            public function DescribeNamespace(DescribeNamespaceRequest $argument, $metadata = [], $options = [])
            {
                $info = (new NamespaceInfo())->setLimits(
                    (new NamespaceInfo\Limits())->setBlobSizeLimitError($this->payloadSize),
                );

                return new class((new DescribeNamespaceResponse())->setNamespaceInfo($info)) {
                    public function __construct(private readonly DescribeNamespaceResponse $response) {}

                    public function wait(): array
                    {
                        return [$this->response, (object) ['code' => 0]];
                    }
                };
            }

            public function close(): void {}
        };

        return new class($stub) extends ServiceClient {};
    }
}
