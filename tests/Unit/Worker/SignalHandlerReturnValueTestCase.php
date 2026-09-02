<?php

declare(strict_types=1);

namespace Temporal\Tests\Unit\Worker;

use Temporal\Tests\Unit\AbstractUnit;
use Temporal\Tests\Unit\Framework\WorkerFactoryMock;
use Temporal\Tests\Unit\Framework\WorkerMock;
use Temporal\Worker\WorkerFactoryInterface;
use Temporal\Workflow;
use Temporal\Workflow\SignalMethod;
use Temporal\Workflow\WorkflowMethod;

use function PHPUnit\Framework\assertTrue;

final class SignalHandlerReturnValueTestCase extends AbstractUnit
{
    private WorkerFactoryInterface $factory;
    private WorkerMock $worker;

    protected function setUp(): void
    {
        $this->factory = WorkerFactoryMock::create();
        $this->worker = $this->factory->newWorker();

        parent::setUp();
    }

    public function testSignalHandlerReturningAValueDoesNotFailTheWorkflow(): void
    {
        $this->worker->registerWorkflowObject(
            new
            #[Workflow\WorkflowInterface]
            class {
                private bool $signalled = false;

                #[WorkflowMethod(name: 'SignalReturnWorkflow')]
                public function handler(): string
                {
                    Workflow::await(fn(): bool => $this->signalled);
                    assertTrue($this->signalled);
                    return 'done';
                }

                #[SignalMethod(name: 'go')]
                public function go(): string
                {
                    $this->signalled = true;
                    return 'signal result';
                }
            }
        );

        $this->worker->runWorkflow('SignalReturnWorkflow');
        $this->worker->sendSignal('SignalReturnWorkflow', 'go');
        $this->worker->assertWorkflowReturns('done');
        $this->factory->run($this->worker);
    }
}
