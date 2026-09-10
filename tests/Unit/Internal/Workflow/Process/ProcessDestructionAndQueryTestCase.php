<?php

declare(strict_types=1);

namespace Temporal\Tests\Unit\Internal\Workflow\Process;

use PHPUnit\Framework\TestCase;
use Temporal\DataConverter\DataConverter;
use Temporal\DataConverter\EncodedValues;
use Temporal\Exception\ExceptionInterceptor;
use Temporal\Interceptor\SimplePipelineProvider;
use Temporal\Interceptor\WorkflowInbound\QueryInput;
use Temporal\Internal\Declaration\Prototype\QueryDefinition;
use Temporal\Internal\Declaration\Prototype\WorkflowPrototype;
use Temporal\Internal\Declaration\WorkflowInstance;
use Temporal\Internal\ServiceContainer;
use Temporal\Internal\Workflow\Input;
use Temporal\Internal\Workflow\Process\Process;
use Temporal\Internal\Workflow\WorkflowContext;
use Temporal\Tests\Unit\Framework\WorkerFactoryMock;
use Temporal\Worker\Logger\StderrLogger;
use Temporal\Workflow;
use Temporal\Workflow\QueryMethod;
use Temporal\Workflow\WorkflowContextInterface;
use Temporal\Workflow\WorkflowInterface;
use Temporal\Workflow\WorkflowMethod;

/**
 * Both cases need the real {@see Process} wiring: the completion path that turns a workflow
 * failure into a command, and the query executor installed by its constructor.
 */
final class ProcessDestructionAndQueryTestCase extends TestCase
{
    private WorkerFactoryMock $factory;

    public function testAThrowableEscapingAFinallyDuringDestructionSendsNoCommand(): void
    {
        [$process] = $this->start(new WorkflowWithThrowingCleanup());

        // The workflow is suspended; nothing has been sent yet.
        self::assertSame([], \iterator_to_array($this->factory->getQueue(), false));

        $process->destroy();
        $this->factory->tick();

        self::assertSame(
            [],
            \iterator_to_array($this->factory->getQueue(), false),
            'A workflow being evicted emitted a command from its destroy activation.',
        );
    }

    public function testAQueryHandlerCannotSendACommandThroughAContextItStashed(): void
    {
        $workflow = new WorkflowStashingItsContext();
        [$process, $instance] = $this->start($workflow);

        self::assertInstanceOf(WorkflowContextInterface::class, $workflow->stashed);
        $queued = $this->factory->getQueue()->count();

        $handler = $instance->getQueryDispatcher()->findQueryHandler('stashed');
        self::assertNotNull($handler);

        $error = null;

        try {
            $handler(new QueryInput('stashed', EncodedValues::empty(), $process->getContext()->getInfo()));
        } catch (\Throwable $e) {
            $error = $e;
        } finally {
            Workflow::setCurrentContext(null);
        }

        self::assertSame(
            $queued,
            $this->factory->getQueue()->count(),
            'A query handler created a command through a context stashed by the workflow.',
        );
        self::assertInstanceOf(\RuntimeException::class, $error);
        self::assertStringContainsString('a query handler', $error->getMessage());

        // The guard is lifted once the query is over: the workflow itself stays writable.
        self::assertFalse($process->getContext()->isReadonly());
    }

    protected function tearDown(): void
    {
        Workflow::setCurrentContext(null);
    }

    /**
     * @return array{Process, WorkflowInstance}
     */
    private function start(object $workflow): array
    {
        $this->factory = new WorkerFactoryMock(DataConverter::createDefault());
        $services = ServiceContainer::fromWorkerFactory(
            $this->factory,
            ExceptionInterceptor::createDefault(),
            new SimplePipelineProvider(),
            new StderrLogger(),
        );

        $reflection = new \ReflectionClass($workflow);
        $prototype = new WorkflowPrototype(
            $reflection->getShortName(),
            $reflection->getMethod('handle'),
            $reflection,
        );

        foreach ($reflection->getMethods() as $method) {
            foreach ($method->getAttributes(QueryMethod::class) as $attribute) {
                $prototype->addQueryHandler(new QueryDefinition(
                    $attribute->newInstance()->name ?? $method->getName(),
                    $method->getReturnType()?->getName() ?? 'string',
                    $method,
                    '',
                ));
            }
        }

        $instance = new WorkflowInstance($prototype, $workflow);
        $context = new WorkflowContext(
            $services,
            $services->client,
            $instance,
            new Input(),
            EncodedValues::empty(),
        );
        $process = new Process($services, 'run-id', $instance);
        $process->initAndStart($context, $instance, false);
        $this->factory->tick();

        return [$process, $instance];
    }
}

#[WorkflowInterface]
final class WorkflowWithThrowingCleanup
{
    #[WorkflowMethod(name: 'WorkflowWithThrowingCleanup')]
    public function handle(): string
    {
        try {
            Workflow::await(static fn(): bool => false);
        } finally {
            throw new \RuntimeException('cleanup failed');
        }
    }
}

#[WorkflowInterface]
final class WorkflowStashingItsContext
{
    public ?WorkflowContextInterface $stashed = null;

    #[WorkflowMethod(name: 'WorkflowStashingItsContext')]
    public function handle(): string
    {
        $this->stashed = Workflow::getCurrentContext();
        Workflow::await(static fn(): bool => false);

        return 'done';
    }

    #[QueryMethod(name: 'stashed')]
    public function stashed(): string
    {
        $this->stashed?->timer(5);

        return 'unreachable';
    }
}
