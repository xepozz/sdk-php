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
use Temporal\Internal\Transport\PayloadSizeLimiter;
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

    /**
     * Runs one Workflow Task through the Worker loop and returns what the Worker did with it.
     */
    private function runWorkflow(
        WorkerFactory $factory,
        ?WorkerOptions $options = null,
        bool $replaying = false,
    ): object {
        $factory->newWorker('default', $options)->registerWorkflowTypes(BigResultWorkflow::class);

        $host = $this->host($this->startWorkflow(), $replaying);
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
            'runId' => $runId,
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

    private function host(string $batch, bool $replaying): object
    {
        return new class($batch, $replaying) implements HostConnectionInterface {
            /** @var list<string> */
            public array $sent = [];
            public ?\Throwable $failure = null;
            private bool $delivered = false;

            public function __construct(
                private readonly string $batch,
                private readonly bool $replaying,
            ) {}

            public function waitBatch(): ?CommandBatch
            {
                if ($this->delivered) {
                    return null;
                }

                $this->delivered = true;

                return new CommandBatch($this->batch, [
                    'taskQueue' => 'default',
                    'replay' => $this->replaying,
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
        return new class(
            DataConverter::createDefault(),
            $this->createMock(RPCConnectionInterface::class),
            $payloadSize,
        ) extends WorkerFactory {
            public function __construct(
                DataConverter $converter,
                RPCConnectionInterface $rpc,
                private readonly int $payloadSize,
            ) {
                parent::__construct($converter, $rpc);
            }

            protected function createPayloadSizeLimiter(): ?PayloadSizeLimiter
            {
                return $this->payloadSize > 0
                    ? new PayloadSizeLimiter($this->payloadSize, DataConverter::createDefault())
                    : null;
            }
        };
    }
}
