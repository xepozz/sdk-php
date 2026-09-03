<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Workflow;

use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use Temporal\DataConverter\EncodedValues;
use Temporal\DataConverter\Type;
use Temporal\DataConverter\ValuesInterface;
use Temporal\Interceptor\Header;
use Temporal\Interceptor\HeaderInterface;
use Temporal\Internal\Marshaller\MarshallerInterface;
use Temporal\Internal\Transport\Request\ExecuteChildWorkflow;
use Temporal\Internal\Transport\Request\GetChildWorkflowExecution;
use Temporal\Internal\Transport\Request\SignalExternalWorkflow;
use Temporal\Internal\Workflow\Process\Awaiter;
use Temporal\Worker\FeatureFlags;
use Temporal\Worker\Transport\Command\RequestInterface;
use Temporal\Workflow;
use Temporal\Workflow\ChildWorkflowOptions;
use Temporal\Workflow\ChildWorkflowStubInterface;
use Temporal\Workflow\ParentClosePolicy;
use Temporal\Workflow\WorkflowExecution;

use function React\Promise\reject;

/**
 * @psalm-import-type TType from Type
 */
final class ChildWorkflowStub implements ChildWorkflowStubInterface
{
    private Deferred $execution;
    private ?PromiseInterface $result = null;
    private bool $started = false;
    private bool $cancellable = true;
    private bool $executionSettled = false;
    private ?\Throwable $startFailure = null;
    private HeaderInterface $header;

    /**
     * @param MarshallerInterface<array> $marshaller
     */
    public function __construct(
        private readonly MarshallerInterface $marshaller,
        private readonly string $workflow,
        private readonly ChildWorkflowOptions $options,
        HeaderInterface|array $header,
    ) {
        $this->execution = new Deferred();
        $this->header = \is_array($header) ? Header::fromValues($header) : $header;
    }

    /**
     * Whether the child workflow request can be cancelled through the server. When it cannot
     * (an abandoned child while {@see FeatureFlags::$cancelAbandonedChildWorkflows} is off), a
     * scope cancellation must interrupt the result wait locally, or it would never settle.
     *
     * @internal
     */
    public static function isCancellable(?ChildWorkflowOptions $options): bool
    {
        return FeatureFlags::$cancelAbandonedChildWorkflows
            || $options === null
            || $options->parentClosePolicy !== ParentClosePolicy::Abandon->value;
    }

    public function getChildWorkflowType(): string
    {
        return $this->workflow;
    }

    public function getExecution(): WorkflowExecution
    {
        $this->assertStarted();
        Awaiter::assertManaged();

        return Awaiter::await($this->getExecutionAsync(), interruptOnCancel: false);
    }

    public function getExecutionAsync(): PromiseInterface
    {
        $this->assertStarted();

        return $this->execution->promise();
    }

    public function start(...$args): WorkflowExecution
    {
        Awaiter::assertManaged();

        return Awaiter::await($this->startAsync(...$args), interruptOnCancel: false);
    }

    public function startAsync(...$args): PromiseInterface
    {
        if ($this->started) {
            throw new \LogicException('Child workflow already has been executed');
        }

        $this->started = true;

        try {
            $request = new ExecuteChildWorkflow(
                $this->workflow,
                EncodedValues::fromValues($args),
                $this->getOptionArray(),
                $this->header,
            );

            $this->cancellable = self::isCancellable($this->options);

            $this->result = $this->request($request, cancellable: $this->cancellable);

            $started = $this->request(new GetChildWorkflowExecution($request))
                ->then(
                    function (ValuesInterface $values): mixed {
                        try {
                            $execution = $values->getValue(0, WorkflowExecution::class);
                        } catch (\Throwable $error) {
                            $this->failStart($error);
                            throw $error;
                        }

                        $this->resolveExecution($execution);

                        return $execution;
                    },
                    function (\Throwable $error): never {
                        $this->failStart($error);
                        throw $error;
                    },
                );
        } catch (\Throwable $error) {
            $this->failStart($error);
            throw $error;
        }

        return EncodedValues::decodePromise($started);
    }

    public function getResult($returnType = null): mixed
    {
        $this->assertStarted();
        Awaiter::assertManaged();

        return Awaiter::await($this->getResultAsync($returnType), interruptOnCancel: !$this->cancellable);
    }

    public function getResultAsync($returnType = null): PromiseInterface
    {
        $this->assertStarted();

        if ($this->startFailure !== null) {
            return reject($this->startFailure);
        }

        \assert($this->result instanceof PromiseInterface);

        return EncodedValues::decodePromise($this->result, $returnType);
    }

    public function execute(array $args = [], $returnType = null): mixed
    {
        Awaiter::assertManaged();

        // The start is issued synchronously, so the request's cancellability is known here.
        $result = $this->executeAsync($args, $returnType);

        return Awaiter::await($result, interruptOnCancel: !$this->cancellable);
    }

    public function executeAsync(array $args = [], $returnType = null): PromiseInterface
    {
        return $this->startAsync(...$args)->then(fn() => $this->getResultAsync($returnType));
    }

    public function getOptions(): ChildWorkflowOptions
    {
        return $this->options;
    }

    public function signal(string $name, array $args = []): void
    {
        $this->assertStarted();
        Awaiter::assertManaged();

        Awaiter::await($this->signalAsync($name, $args), interruptOnCancel: false);
    }

    public function signalAsync(string $name, array $args = []): PromiseInterface
    {
        $this->assertStarted();

        return $this->execution->promise()->then(
            function (WorkflowExecution $execution) use ($name, $args) {
                $request = new SignalExternalWorkflow(
                    $this->getOptions()->namespace,
                    $execution->getID(),
                    null,
                    $name,
                    EncodedValues::fromValues($args),
                    true,
                );

                return $this->request($request);
            },
        );
    }

    protected function request(RequestInterface $request, bool $cancellable = true): PromiseInterface
    {
        return Workflow::getCurrentContext()->request($request, cancellable: $cancellable);
    }

    private function getOptionArray(): array
    {
        return $this->marshaller->marshal($this->getOptions());
    }

    private function assertStarted(): void
    {
        if (!$this->started) {
            throw new \LogicException('Child workflow has not been started');
        }
    }

    private function resolveExecution(WorkflowExecution $execution): void
    {
        if ($this->executionSettled) {
            return;
        }

        $this->executionSettled = true;
        $this->execution->resolve($execution);
    }

    private function failStart(\Throwable $error): void
    {
        $this->startFailure ??= $error;

        if ($this->executionSettled) {
            return;
        }

        $this->executionSettled = true;
        $this->execution->reject($error);
    }
}
