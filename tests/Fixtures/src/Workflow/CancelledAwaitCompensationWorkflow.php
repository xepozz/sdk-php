<?php

declare(strict_types=1);

namespace Temporal\Tests\Workflow;

use Temporal\Activity\ActivityOptions;
use Temporal\Exception\Failure\CanceledFailure;
use Temporal\Tests\Activity\SimpleActivity;
use Temporal\Workflow;
use Temporal\Workflow\WorkflowMethod;

/**
 * A scope with an activity in flight is suspended on a condition when it is cancelled;
 * the compensation it schedules must keep its place relative to the Cancel command.
 */
#[Workflow\WorkflowInterface]
class CancelledAwaitCompensationWorkflow
{
    #[WorkflowMethod(name: 'CancelledAwaitCompensationWorkflow')]
    public function handler(): string
    {
        $simple = Workflow::newActivityStub(
            SimpleActivity::class,
            ActivityOptions::new()->withStartToCloseTimeout(5),
        );

        $scope = Workflow::async(static function () use ($simple): string {
            Workflow::async(static fn() => $simple->slow('in flight'));

            try {
                Workflow::await(static fn(): bool => false);
            } catch (CanceledFailure) {
                Workflow::asyncDetached(static fn() => $simple->echo('compensate'))->await();
            }

            return 'compensated';
        });

        Workflow::timer(1);
        $scope->cancel();

        return $scope->await();
    }
}
