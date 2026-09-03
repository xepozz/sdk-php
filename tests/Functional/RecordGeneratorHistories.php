<?php

declare(strict_types=1);

namespace Temporal\Tests\Functional;

use PHPUnit\Framework\Attributes\DataProvider;
use Ramsey\Uuid\Uuid;
use Temporal\Api\Enums\V1\EventType;
use Temporal\Client\GRPC\ServiceClient;
use Temporal\Client\WorkflowClient;
use Temporal\Client\WorkflowOptions;
use Temporal\Client\WorkflowStubInterface;
use Temporal\Testing\Replay\WorkflowReplayer;
use Temporal\Testing\TemporalServer;
use Temporal\Tests\TestCase;
use Temporal\Workflow\WorkflowExecution;

/**
 * Records the histories replayed by {@see GeneratorHistoryReplayTestCase}.
 *
 * Not part of any test suite (no TestCase suffix). Run it explicitly inside a checkout of the
 * generator-based SDK (upstream master before the Fiber runtime), with the functional test
 * environment (temporal-test-server and RoadRunner) started by the Functional bootstrap:
 *
 *   RECORD_HISTORY_DIR=/path/to/tests/Fixtures/history/generator \\
 *     vendor/bin/phpunit --testsuite=Functional tests/Functional/RecordGeneratorHistories.php
 *
 * Output directory is taken from the RECORD_HISTORY_DIR environment variable.
 */
final class RecordGeneratorHistories extends TestCase
{
    private WorkflowClient $client;
    private string $dir;

    protected function setUp(): void
    {
        $this->client = new WorkflowClient(ServiceClient::create(TemporalServer::address()));
        $this->dir = \getenv('RECORD_HISTORY_DIR') ?: throw new \RuntimeException('RECORD_HISTORY_DIR is not set');
        \is_dir($this->dir) or \mkdir($this->dir, recursive: true);
        parent::setUp();
    }

    public static function scenarios(): iterable
    {
        yield 'SimpleWorkflow' => ['SimpleWorkflow', ['hello'], null];
        yield 'WorkflowWithSequence' => ['WorkflowWithSequence', ['hello'], null];
        yield 'TimerWorkflow' => ['TimerWorkflow', ['hello'], null];
        yield 'SideEffectWorkflow' => ['SideEffectWorkflow', ['hello'], null];
        yield 'VersionedWorkflow' => ['VersionedWorkflow', [], null];
        yield 'SimpleUuidWorkflow' => ['SimpleUuidWorkflow', [Uuid::uuid4()], null];
        yield 'SagaWorkflow' => ['SagaWorkflow', [], null];
        yield 'WithChildWorkflow' => ['WithChildWorkflow', ['hello'], null];
        yield 'ParentWithChildAndTimerWorkflow' => ['ParentWithChildAndTimerWorkflow', [], null];
        yield 'ContinuableWorkflow' => ['ContinuableWorkflow', [1], null];
        yield 'ParallelScopesWorkflow' => ['ParallelScopesWorkflow', ['hello'], null];
        yield 'CancelledScopeWorkflow' => ['CancelledScopeWorkflow', [], null];
        yield 'CancelledMidflightWorkflow' => ['CancelledMidflightWorkflow', [], null];
        yield 'AsyncActivityWorkflow' => ['AsyncActivityWorkflow', [], null];
        yield 'LocalActivityWorkflow' => ['LocalActivityWorkflow', [], null];
        yield 'SimpleSignalledWorkflow' => ['SimpleSignalledWorkflow', [], static function (WorkflowStubInterface $stub): void {
            $stub->signal('add', 5);
            $stub->signal('add', 3);
        }];
        yield 'WaitWorkflow' => ['WaitWorkflow', [], static function (WorkflowStubInterface $stub): void {
            $stub->signal('unlock', 'unlock the condition');
        }];
        yield 'LoopWithSignalCoroutinesWorkflow' => ['LoopWithSignalCoroutinesWorkflow', [4], static function (WorkflowStubInterface $stub): void {
            foreach (['test1', 'test2', 'test3', 'test4'] as $v) {
                $stub->signal('addValue', $v);
            }
        }];
        yield 'Update.greet' => ['Update.greet', [], static function (WorkflowStubInterface $stub): void {
            $stub->update('addName', 'John');
            $stub->update('addNameViaActivity', 'Doe');
            $stub->signal('exit');
        }];
        yield 'AwaitsUpdate.greet' => ['AwaitsUpdate.greet', [], static function (WorkflowStubInterface $stub): void {
            $stub->startUpdate('await', 'key');
            $stub->update('resolveValue', 'key', 'resolved');
            $stub->signal('exit');
        }];
        yield 'CancelledWorkflow' => ['CancelledWorkflow', [], self::cancelAfterActivityScheduled(...)];
        yield 'CancelledWithCompensationWorkflow' => ['CancelledWithCompensationWorkflow', [], self::cancelAfterActivityScheduled(...)];
        yield 'CancelledNestedWorkflow' => ['CancelledNestedWorkflow', [], self::cancelAfterTimerStarted(...)];
        yield 'SimpleSignalledWorkflowWithSleep' => ['SimpleSignalledWorkflowWithSleep', [-1], self::cancelAfterTimerStarted(...)];
    }

    #[DataProvider('scenarios')]
    public function testRecord(string $type, array $args, ?\Closure $driver): void
    {
        $stub = $this->client->newUntypedWorkflowStub(
            $type,
            WorkflowOptions::new()->withWorkflowExecutionTimeout('2 minutes'),
        );
        $run = $this->client->start($stub, ...$args);

        if ($driver !== null) {
            $driver($stub, $this->client, $run->getExecution());
        }

        try {
            $stub->getResult(timeout: 60);
        } catch (\Throwable $e) {
            // Failures and cancellations are part of the recorded history.
        }

        $file = $this->dir . '/' . $type . '.json';
        \is_file($file) and \unlink($file);
        (new WorkflowReplayer())->downloadHistory($type, $run->getExecution(), $file);
        self::assertFileExists($file);
    }

    private static function cancelAfterActivityScheduled(
        WorkflowStubInterface $stub,
        WorkflowClient $client,
        WorkflowExecution $execution,
    ): void {
        self::waitForEvent($client, $execution, EventType::EVENT_TYPE_ACTIVITY_TASK_SCHEDULED);
        $stub->cancel();
    }

    private static function cancelAfterTimerStarted(
        WorkflowStubInterface $stub,
        WorkflowClient $client,
        WorkflowExecution $execution,
    ): void {
        self::waitForEvent($client, $execution, EventType::EVENT_TYPE_TIMER_STARTED);
        $stub->cancel();
    }

    private static function waitForEvent(WorkflowClient $client, WorkflowExecution $execution, int $eventType): void
    {
        $deadline = \microtime(true) + 15;
        do {
            foreach ($client->getWorkflowHistory($execution)->getEvents() as $event) {
                if ($event->getEventType() === $eventType) {
                    return;
                }
            }
            \usleep(100_000);
        } while (\microtime(true) < $deadline);

        throw new \RuntimeException("Event $eventType was not observed in time.");
    }
}
