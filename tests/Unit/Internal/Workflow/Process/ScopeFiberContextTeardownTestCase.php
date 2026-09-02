<?php

declare(strict_types=1);

namespace Temporal\Tests\Unit\Internal\Workflow\Process;

use Internal\Destroy\Destroyable;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\UuidInterface;
use React\Promise\Deferred;
use Temporal\DataConverter\DataConverter;
use Temporal\DataConverter\EncodedValues;
use Temporal\DataConverter\ValuesInterface;
use Temporal\Exception\DestructMemorizedInstanceException;
use Temporal\Exception\ExceptionInterceptor;
use Temporal\Exception\Failure\CanceledFailure;
use Temporal\Interceptor\SimplePipelineProvider;
use Temporal\Internal\Declaration\Prototype\WorkflowPrototype;
use Temporal\Internal\Declaration\WorkflowInstance\QueryDispatcher;
use Temporal\Internal\Declaration\WorkflowInstance\SignalDispatcher;
use Temporal\Internal\Declaration\WorkflowInstance\UpdateDispatcher;
use Temporal\Internal\Declaration\WorkflowInstanceInterface;
use Temporal\Internal\ServiceContainer;
use Temporal\Internal\Transport\Request\GetVersion;
use Temporal\Internal\Transport\Request\SideEffect;
use Temporal\Internal\Workflow\Input;
use Temporal\Internal\Workflow\Process\Scope;
use Temporal\Internal\Workflow\ScopeContext;
use Temporal\Internal\Workflow\WorkflowContext;
use Temporal\Tests\Unit\Framework\WorkerFactoryMock;
use Temporal\Worker\Logger\StderrLogger;
use Temporal\Worker\Transport\Command\Server\SuccessResponse;
use Temporal\Worker\Transport\Command\Server\TickInfo;
use Temporal\Worker\FeatureFlags;
use Temporal\Workflow;
use Temporal\Workflow\CancellationScopeInterface;
use Temporal\Workflow\ChildWorkflowOptions;
use Temporal\Workflow\ParentClosePolicy;

final class ScopeFiberContextTeardownTestCase extends TestCase
{
    private WorkerFactoryMock $factory;
    private ContextTeardownRootScope $root;
    private ScopeContext $scopeContext;

    public function testResolvingAPromiseAwaitedByAnotherScopeKeepsTheCallerContext(): void
    {
        $gate = new Deferred();
        $before = $after = null;
        $childDone = false;

        $this->startRoot(static function () use ($gate, &$before, &$after, &$childDone): string {
            Workflow::async(static function () use ($gate, &$childDone): void {
                Workflow::await($gate->promise());
                $childDone = true;
            });

            $before = Workflow::getCurrentContext();
            $gate->resolve(null);
            $after = Workflow::getCurrentContext();

            return 'done';
        });
        $this->flush();

        self::assertTrue($childDone);
        self::assertSame($before, $after, 'Context leaked from the resumed child scope into the caller.');
    }

    public function testCancelledParentAwaitingDetachedChildGetsChildResult(): void
    {
        $gate = new Deferred();
        $result = null;
        $failure = null;

        $this->startRoot(static function () use ($gate, &$result, &$failure): string {
            $scope = Workflow::async(static function () use ($gate): string {
                return Workflow::asyncDetached(static function () use ($gate): string {
                    Workflow::await($gate->promise());
                    return 'detached';
                })->await();
            });

            $scope->cancel();

            try {
                $result = $scope->await();
            } catch (\Throwable $e) {
                $failure = $e;
            }

            return 'done';
        });
        $this->flush();

        self::assertNull($result);
        self::assertNull($failure);

        $gate->resolve(null);
        $this->flush();

        self::assertSame('detached', $result, 'Failure: ' . ($failure ? $failure::class . ' ' . $failure->getMessage() : 'none'));
    }

    public function testAwaitTrueConditionInCancelledScopeThrows(): void
    {
        $caught = null;

        $this->startRoot(static function () use (&$caught): string {
            $scope = Workflow::async(static function () use (&$caught): void {
                try {
                    Workflow::await(static fn(): bool => false);
                } catch (CanceledFailure) {
                    try {
                        Workflow::await(static fn(): bool => true);
                    } catch (\Throwable $e) {
                        $caught = $e;
                    }
                }
            });

            $scope->cancel();

            return 'done';
        });
        $this->flush();

        self::assertInstanceOf(CanceledFailure::class, $caught);
    }

    public function testAwaitingACancelledScopeDeliversTheValueItRecoveredWith(): void
    {
        $result = null;

        $this->startRoot(static function () use (&$result): string {
            $scope = Workflow::async(static function (): string {
                try {
                    Workflow::await(static fn(): bool => false);
                } catch (CanceledFailure) {
                    return 'recovered';
                }

                return 'unreachable';
            });

            $scope->cancel();
            $result = Workflow::await($scope);

            return 'done';
        });
        $this->flush();

        self::assertSame('recovered', $result);
    }

    public function testExecuteChildWorkflowIsInterruptedByScopeCancellationForAnAbandonedChild(): void
    {
        $flag = FeatureFlags::$cancelAbandonedChildWorkflows;
        FeatureFlags::$cancelAbandonedChildWorkflows = false;
        $scope = null;
        $failure = null;

        try {
            $this->startRoot(static function () use (&$scope, &$failure): void {
                $scope = Workflow::async(static function () use (&$failure): void {
                    try {
                        Workflow::executeChildWorkflow(
                            'child',
                            [],
                            ChildWorkflowOptions::new()->withParentClosePolicy(ParentClosePolicy::Abandon),
                        );
                    } catch (CanceledFailure $e) {
                        $failure = $e;
                    }
                });

                Workflow::await(static fn(): bool => false);
            });

            // The child start commands have been sent to the server: they are no longer queued.
            foreach ($this->factory->getQueue() as $command) {
            }

            self::assertInstanceOf(CancellationScopeInterface::class, $scope);
            $scope->cancel();
            $this->flush();

            self::assertInstanceOf(CanceledFailure::class, $failure);
        } finally {
            FeatureFlags::$cancelAbandonedChildWorkflows = $flag;
        }
    }

    public function testSideEffectGetVersionAndUuidAreNotSubjectToScopeCancellation(): void
    {
        $calls = [
            'sideEffect' => static fn(): mixed => Workflow::sideEffect(static fn(): int => 7),
            'getVersion' => static fn(): mixed => Workflow::getVersion('change', 1, 2),
            'uuid' => static fn(): mixed => Workflow::uuid(),
        ];
        $results = [];
        $errors = [];

        $this->startRoot(static function () use ($calls, &$results, &$errors): void {
            foreach ($calls as $name => $call) {
                $scope = Workflow::async(static function () use ($name, $call, &$results, &$errors): void {
                    try {
                        Workflow::await(static fn(): bool => false);
                    } catch (CanceledFailure) {
                        // The scope is cancelled from now on.
                    }

                    try {
                        $results[$name] = $call();
                    } catch (\Throwable $e) {
                        $errors[$name] = $e;
                    }
                });

                $scope->cancel();
            }

            Workflow::await(static fn(): bool => false);
        });

        self::assertSame([], $errors, 'Marker commands must not fail in a cancelled scope.');

        // The commands reached the server; answer them the way the server does.
        $commands = \iterator_to_array($this->factory->getQueue(), false);
        self::assertCount(3, $commands);
        self::assertInstanceOf(SideEffect::class, $commands[0]);
        self::assertInstanceOf(GetVersion::class, $commands[1]);
        self::assertInstanceOf(SideEffect::class, $commands[2]);

        $client = $this->factory->getClient();
        $tick = new TickInfo(new \DateTimeImmutable());
        $client->dispatch(new SuccessResponse($commands[0]->getPayloads(), $commands[0]->getID(), $tick));
        $client->dispatch(new SuccessResponse(EncodedValues::fromValues([2]), $commands[1]->getID(), $tick));
        $client->dispatch(new SuccessResponse($commands[2]->getPayloads(), $commands[2]->getID(), $tick));
        $this->flush();

        self::assertSame(7, $results['sideEffect'] ?? null);
        self::assertSame(2, $results['getVersion'] ?? null);
        self::assertInstanceOf(UuidInterface::class, $results['uuid'] ?? null);
    }

    public function testNonCancellableQueuedCommandSurvivesALaterScopeCancellation(): void
    {
        $scope = null;
        $error = null;

        $this->startRoot(static function () use (&$scope, &$error): void {
            $scope = Workflow::async(static function () use (&$error): void {
                try {
                    Workflow::sideEffect(static fn(): int => 1);
                } catch (\Throwable $e) {
                    $error = $e;
                }
            });

            Workflow::await(static fn(): bool => false);
        });

        self::assertSame(1, $this->factory->getQueue()->count());
        self::assertInstanceOf(CancellationScopeInterface::class, $scope);

        $scope->cancel();
        $this->flush();

        self::assertNull($error);
        self::assertSame(1, $this->factory->getQueue()->count(), 'The queued marker command was dropped.');
    }

    public function testCancellingACompletedScopeCancelsItsRunningChildren(): void
    {
        $childCancelled = false;
        $parentResult = null;

        $this->startRoot(static function () use (&$childCancelled, &$parentResult): string {
            $scope = Workflow::async(static function () use (&$childCancelled): string {
                Workflow::async(static function () use (&$childCancelled): void {
                    try {
                        Workflow::await(static fn(): bool => false);
                    } catch (CanceledFailure) {
                        $childCancelled = true;
                    }
                });

                return 'parent done';
            });

            $parentResult = $scope->await();
            $scope->cancel();

            return 'done';
        });
        $this->flush();

        self::assertSame('parent done', $parentResult);
        self::assertTrue($childCancelled, 'A child scope still running after its parent completed was not cancelled.');
    }

    public function testDestroyUnwindsAFinallyBlockThatSuspendsAgain(): void
    {
        $log = [];
        $gcWasEnabled = \gc_enabled();
        \gc_disable();

        try {
            $this->startRoot(static function () use (&$log): void {
                try {
                    Workflow::await(static fn(): bool => false);
                } finally {
                    $log[] = 'cleanup started';

                    try {
                        Workflow::await(static fn(): bool => false);
                        $log[] = 'cleanup resumed';
                    } catch (\Throwable $e) {
                        $log[] = 'cleanup caught ' . $e::class;
                    }

                    $log[] = 'cleanup finished';
                }
            });

            $this->root->destroy();
        } finally {
            $gcWasEnabled and \gc_enable();
        }

        self::assertSame(
            [
                'cleanup started',
                'cleanup caught ' . DestructMemorizedInstanceException::class,
                'cleanup finished',
            ],
            $log,
        );
    }

    public function testDestroySwallowsFailuresThrownWhileUnwinding(): void
    {
        $gcWasEnabled = \gc_enabled();
        \gc_disable();

        try {
            $this->startRoot(static function (): void {
                try {
                    Workflow::await(static fn(): bool => false);
                } finally {
                    // Suspend once more so the engine has to force-close the fiber,
                    // then fail from the finally block.
                    try {
                        Workflow::await(static fn(): bool => false);
                    } finally {
                        throw new \RuntimeException('cleanup failed');
                    }
                }
            });

            $this->root->destroy();
        } finally {
            $gcWasEnabled and \gc_enable();
        }

        self::assertTrue(true);
    }

    protected function setUp(): void
    {
        $this->factory = new WorkerFactoryMock(DataConverter::createDefault());
        $services = ServiceContainer::fromWorkerFactory(
            $this->factory,
            ExceptionInterceptor::createDefault(),
            new SimplePipelineProvider(),
            new StderrLogger(),
        );

        $workflow = new \stdClass();
        $prototype = new WorkflowPrototype('scope-fiber-context-teardown-test', null, new \ReflectionClass($workflow));
        $instance = $this->createMockForIntersectionOfInterfaces([
            WorkflowInstanceInterface::class,
            Destroyable::class,
        ]);
        $instance->method('getQueryDispatcher')
            ->willReturn(new QueryDispatcher($prototype, $workflow));
        $instance->method('getSignalDispatcher')
            ->willReturn(new SignalDispatcher($prototype, $workflow));
        $instance->method('getUpdateDispatcher')
            ->willReturn(new UpdateDispatcher($prototype, $workflow));

        $context = new WorkflowContext(
            $services,
            $services->client,
            $instance,
            new Input(),
            EncodedValues::empty(),
        );
        $context->setReadonly(false);
        $this->root = new ContextTeardownRootScope($services);
        $this->scopeContext = $this->root->bind($context);
    }

    protected function tearDown(): void
    {
        Workflow::setCurrentContext(null);
    }

    private function startRoot(callable $handler): void
    {
        $this->root->start(
            static fn(ValuesInterface $values): mixed => $handler(),
            EncodedValues::empty(),
            false,
        );
    }

    private function flush(): void
    {
        for ($i = 0; $i < 8; ++$i) {
            $this->factory->tick();
        }
    }
}

final class ContextTeardownRootScope extends Scope
{
    public function bind(WorkflowContext $context): ScopeContext
    {
        $this->setContext($context);

        return $this->scopeContext;
    }
}
