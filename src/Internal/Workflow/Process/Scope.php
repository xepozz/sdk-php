<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Workflow\Process;

use Internal\Destroy\Destroyable;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use Temporal\DataConverter\EncodedValues;
use Temporal\DataConverter\ValuesInterface;
use Temporal\Exception\DestructMemorizedInstanceException;
use Temporal\Exception\Failure\CanceledFailure;
use Temporal\Exception\Failure\TemporalFailure;
use Temporal\Exception\InvalidArgumentException;
use Temporal\Exception\InvalidSuspendException;
use Temporal\Interceptor\WorkflowInbound\UpdateInput;
use Temporal\Internal\Declaration\MethodHandler;
use Temporal\Internal\ServiceContainer;
use Temporal\Internal\Support\Facade;
use Temporal\Internal\Transport\Request\Cancel;
use Temporal\Internal\Transport\Request\GetVersion;
use Temporal\Internal\Transport\Request\SideEffect;
use Temporal\Internal\Workflow\ScopeContext;
use Temporal\Internal\Workflow\WorkflowContext;
use Temporal\Worker\FeatureFlags;
use Temporal\Worker\LoopInterface;
use Temporal\Worker\Transport\Command\RequestInterface;
use Temporal\Workflow;
use Temporal\Workflow\CancellationScopeInterface;

/**
 * @internal CoroutineScope is an internal library class, please do not use it in your code.
 * @psalm-internal Temporal\Internal
 * @implements CancellationScopeInterface<mixed>
 */
class Scope implements CancellationScopeInterface, Destroyable
{
    /** How many times the destruct failure is redelivered to a Fiber that keeps suspending while unwinding. */
    private const MAX_UNWIND_ATTEMPTS = 64;

    protected ServiceContainer $services;

    /** @psalm-suppress PropertyNotSetInConstructor */
    protected WorkflowContext $context;

    /** @psalm-suppress PropertyNotSetInConstructor */
    protected ScopeContext $scopeContext;

    protected Deferred $deferred;
    protected DeferredFiber $coroutine;

    /** @var non-empty-string */
    private string $layer = LoopInterface::ON_TICK;

    private int $cancelID = 0;

    /** @var array<callable> */
    private array $onCancel = [];

    /** @var array<callable(mixed): mixed> */
    private array $onClose = [];

    /** @var array<int, self> */
    private array $children = [];

    private bool $detached = false;
    private bool $cancelled = false;
    private bool $closed = false;
    private bool $destroyed = false;

    /** @var array<int, true> Handlers that link a child scope or a pending request (kept after close). */
    private array $internalCancelIDs = [];

    /** @var array<int, true> Links to detached children: only a destruct cancellation reaches them. */
    private array $detachedLinkIDs = [];

    private bool $ownsContext = true;
    private bool $skipInvalidArguments = false;
    private ?\Throwable $cancelReason = null;
    private ?int $suspensionCancelID = null;

    /** Removes this scope from its parent; kept until the scope and all its children are done. */
    private ?\Closure $parentUnlink = null;

    public function __construct(
        ServiceContainer $services,
    ) {
        $this->services = $services;
        $this->deferred = new Deferred();
    }

    /**
     * @return non-empty-string
     */
    public function getLayer(): string
    {
        return $this->layer;
    }

    public function isDetached(): bool
    {
        return $this->detached;
    }

    public function isCancelled(): bool
    {
        return $this->cancelled;
    }

    public function getContext(): WorkflowContext
    {
        return $this->context;
    }

    /**
     * @param MethodHandler|\Closure(ValuesInterface): mixed $handler
     */
    public function start(MethodHandler|\Closure $handler, ValuesInterface $values, bool $deferred): void
    {
        $this->coroutine = DeferredFiber::fromHandler($handler, $values, $this->scopeContext)
            ->catch($this->onException(...));

        $deferred
            ? $this->services->loop->once($this->layer, $this->next(...))
            : $this->next();
    }

    /**
     * @param callable(ValuesInterface): mixed $handler Update method handler.
     * @param Deferred $resolver Update method promise resolver.
     */
    public function startUpdate(callable $handler, UpdateInput $input, Deferred $resolver): void
    {
        $id = $this->context->getHandlerState()->addUpdate($input->updateId, $input->updateName);
        $this->then(
            fn() => $this->context->getHandlerState()->removeUpdate($id),
            fn() => $this->context->getHandlerState()->removeUpdate($id),
        );

        $this->then(
            $resolver->resolve(...),
            function (\Throwable $error) use ($resolver): void {
                $this->services->exceptionInterceptor->isRetryable($error)
                    ? $this->scopeContext->panic($error)
                    : $resolver->reject($error);
            },
        );

        $this->coroutine = $this->callSignalOrUpdateHandler($handler, $input->arguments);
        $this->next();
    }

    /**
     * @param callable(ValuesInterface): mixed $handler
     * @param non-empty-string $name
     */
    public function startSignal(callable $handler, ValuesInterface $values, string $name): void
    {
        $id = $this->context->getHandlerState()->addSignal($name);
        $this->then(
            fn() => $this->context->getHandlerState()->removeSignal($id),
            fn() => $this->context->getHandlerState()->removeSignal($id),
        );

        $this->coroutine = $this->callSignalOrUpdateHandler($handler, $values);
        $this->next();
    }

    public function onCancel(callable $then): self
    {
        $this->addOnCancel($then);
        return $this;
    }

    /**
     * @param callable(mixed): mixed $then An exception instance is passed in case of error.
     * @return $this
     */
    public function onClose(callable $then): self
    {
        $this->onClose[] = $then;
        return $this;
    }

    public function cancel(?\Throwable $reason = null): void
    {
        if ($this->closed) {
            // The scope has settled, but scopes it started and requests it sent without awaiting
            // may still be pending: forward the cancellation to them without changing the scope.
            $this->runCancelHandlers($reason);
            return;
        }

        if ($this->cancelled) {
            // A destruct cancellation still has to reach the detached children and pending
            // requests a cancelled scope keeps.
            if ($reason instanceof DestructMemorizedInstanceException) {
                $this->runCancelHandlers($reason);
            }

            return;
        }

        $this->cancelled = true;
        $this->cancelReason = $reason;
        $this->runCancelHandlers($reason);
    }

    /**
     * @param non-empty-string|null $layer
     */
    public function startScope(callable $handler, bool $detached, ?string $layer = null): CancellationScopeInterface
    {
        $savedContext = Facade::getCurrentContext();
        $scope = $this->createScope($detached, $layer);

        try {
            $scope->start($handler(...), EncodedValues::empty(), false);
        } finally {
            Workflow::setCurrentContext($savedContext);
        }

        return $scope;
    }

    public function promise(): PromiseInterface
    {
        return $this->deferred->promise();
    }

    public function await(): mixed
    {
        return Awaiter::await($this, interruptOnCancel: false);
    }

    public function then(
        ?callable $onFulfilled = null,
        ?callable $onRejected = null,
        ?callable $onProgress = null,
    ): PromiseInterface {
        return $this->deferred->promise()->then($onFulfilled, $onRejected);
    }

    public function catch(callable $onRejected): PromiseInterface
    {
        return $this->deferred->promise()->catch($onRejected);
    }

    public function finally(callable $onFulfilledOrRejected): PromiseInterface
    {
        return $this->deferred->promise()->finally($onFulfilledOrRejected);
    }

    /**
     * @deprecated use {@see catch()} instead
     */
    public function otherwise(callable $onRejected): PromiseInterface
    {
        return $this->catch($onRejected);
    }

    /**
     * @deprecated use {@see finally()} instead
     */
    public function always(callable $onFulfilledOrRejected): PromiseInterface
    {
        return $this->finally($onFulfilledOrRejected);
    }

    /**
     * Connects promise to scope context to be cancelled on promise cancel.
     */
    /**
     * @param non-empty-string $conditionGroupId
     */
    public function onAwait(Deferred $deferred, string $conditionGroupId): void
    {
        $cancelID = $this->addOnCancel(function (?\Throwable $e = null) use ($deferred, $conditionGroupId): void {
            // The condition is not pending any more: stop re-evaluating it.
            $this->context->rejectConditionGroup($conditionGroupId);
            $deferred->reject($e ?? new CanceledFailure(''));
        }, internal: true);

        $cleanup = function () use ($cancelID): void {
            $this->forgetCancelHandler($cancelID);
            $this->resolveConditionsInScope();
            $this->unlinkFromParentIfIdle();
        };

        $deferred->promise()->then($cleanup, $cleanup);
    }

    public function destroy(): void
    {
        $this->destroyed = true;
        $this->destroyChildren();
        $this->unwindCoroutine();
        // Unwinding may have started new scopes from finally blocks.
        $this->destroyChildren();

        if ($this->ownsContext) {
            // A Destroyable workflow instance may use the Workflow facade from destroy().
            $savedContext = Facade::getCurrentContext();

            try {
                $this->makeCurrent();
                $this->context?->destroy();
                $this->scopeContext?->destroy();
            } finally {
                Workflow::setCurrentContext($savedContext);
            }
        }

        $this->parentUnlink = null;
        $this->internalCancelIDs = [];
        $this->detachedLinkIDs = [];

        unset(
            $this->context,
            $this->scopeContext,
            $this->deferred,
            $this->services,
            $this->onCancel,
            $this->onClose,
        );
    }

    /**
     * @param non-empty-string|null $layer
     */
    protected function createScope(
        bool $detached,
        ?string $layer = null,
        ?WorkflowContext $context = null,
        ?Workflow\UpdateContext $updateContext = null,
    ): self {
        $scope = new Scope($this->services);
        $scope->setContext($context ?? $this->context, $updateContext);
        $scope->detached = $detached;
        $scope->ownsContext = false;

        if ($layer !== null) {
            $scope->layer = $layer;
        }

        $cancelID = $this->addOnCancel($scope->cancelFromParent(...), cancellable: !$detached, internal: true);
        $this->children[$cancelID] = $scope;
        $detached and $this->detachedLinkIDs[$cancelID] = true;

        $scope->parentUnlink = function () use ($cancelID): void {
            unset($this->onCancel[$cancelID], $this->children[$cancelID]);

            // The last running child of a settled scope releases the settled scope as well.
            if ($this->closed) {
                $this->unlinkFromParentIfIdle();
            }
        };
        // A settled scope stays linked to its parent while scopes it started are still running,
        // so cancellation and destruction keep reaching them through the parent chain.
        $scope->onClose(static fn() => $scope->unlinkFromParentIfIdle());

        return $scope;
    }

    protected function setContext(WorkflowContext $ctx, ?Workflow\UpdateContext $updateContext = null): void
    {
        $this->context = $ctx;
        $this->scopeContext = ScopeContext::fromWorkflowContext(
            $this->context,
            $this,
            $this->onRequest(...),
            $updateContext,
        );
    }

    /**
     * Call a Signal or Update method. In this case deserialization errors are skipped.
     *
     * @param callable(ValuesInterface): mixed $handler
     */
    protected function callSignalOrUpdateHandler(callable $handler, ValuesInterface $values): DeferredFiber
    {
        $this->skipInvalidArguments = true;

        return DeferredFiber::fromHandler($handler(...), $values, $this->scopeContext)
            ->catch($this->onSignalOrUpdateException(...));
    }

    protected function onRequest(RequestInterface $request, PromiseInterface $promise, bool $cancellable = true): void
    {
        // A marker (side effect, version) is recorded regardless of scope cancellation.
        $marker = $request instanceof SideEffect || $request instanceof GetVersion;

        $cancelID = $this->addOnCancel(function (?\Throwable $reason = null) use ($request, $cancellable, $marker): void {
            $client = $this->context->getClient();
            if ($reason instanceof DestructMemorizedInstanceException) {
                $client->reject($request, $reason);
                return;
            }

            if ($marker) {
                return;
            }

            if ($client->isQueued($request)) {
                $client->cancel($request);
                return;
            }

            if (!$cancellable) {
                return;
            }

            $client->request(new Cancel($request->getID()), $this->scopeContext);
        }, $cancellable, internal: true);

        $cleanup = function () use ($cancelID): void {
            $this->forgetCancelHandler($cancelID);
            $this->resolveConditionsInScope();
            $this->unlinkFromParentIfIdle();
        };

        $promise->then($cleanup, $cleanup);
    }

    protected function makeCurrent(): void
    {
        Workflow::setCurrentContext($this->scopeContext);
    }

    protected function next(): void
    {
        $this->makeCurrent();
        $this->context->resolveConditions();

        try {
            $suspended = $this->coroutine->start();
        } catch (\Throwable) {
            return;
        }

        $this->advance($suspended);
    }

    private function runCancelHandlers(?\Throwable $reason): void
    {
        $savedContext = Facade::getCurrentContext();

        try {
            foreach ($this->orderedCancelHandlers() as $i => $handler) {
                if (isset($this->detachedLinkIDs[$i]) && !$reason instanceof DestructMemorizedInstanceException) {
                    // A detached child ignores this cancellation; keep its link for a destruct one.
                    continue;
                }

                $this->makeCurrent();
                $this->forgetCancelHandler($i);
                $handler($reason);
            }
        } finally {
            Workflow::setCurrentContext($savedContext);
        }
    }

    private function destroyChildren(): void
    {
        while ($this->children !== []) {
            $children = $this->children;
            $this->children = [];

            foreach ($children as $child) {
                $child->destroy();
            }
        }
    }

    /**
     * Terminate the workflow Fiber of this scope.
     *
     * The destruct failure is delivered again each time a finally block suspends while unwinding,
     * so the Fiber terminates here instead of being force-closed by the engine at an arbitrary
     * later point (a cycle through the scope context keeps it alive until a GC run).
     */
    private function unwindCoroutine(): void
    {
        /** @psalm-suppress RedundantPropertyInitializationCheck, RedundantCondition */
        if (!isset($this->coroutine)) {
            return;
        }

        for ($attempt = 0; $attempt < self::MAX_UNWIND_ATTEMPTS; ++$attempt) {
            /** @psalm-suppress RedundantPropertyInitializationCheck, RedundantCondition, TypeDoesNotContainType */
            if (!isset($this->coroutine) || !$this->coroutine->isSuspended()) {
                break;
            }

            try {
                $this->coroutine->throw(new DestructMemorizedInstanceException());
            } catch (\Throwable) {
                break;
            }
        }

        try {
            // A Fiber that is still suspended is force-closed by the engine here;
            // its finally blocks run and may throw.
            unset($this->coroutine);
        } catch (\Throwable) {
        }
    }

    private function advance(mixed $suspended): void
    {
        $this->skipInvalidArguments = false;
        $this->makeCurrent();
        $this->context->resolveConditions();

        if ($this->coroutine->isTerminated()) {
            try {
                $this->onResult($this->coroutine->getReturn());
            } catch (\Throwable $e) {
                $this->onException($e);
            }
            return;
        }

        if (!$suspended instanceof FiberSuspension) {
            $type = \get_debug_type($suspended);
            $this->onException(new InvalidSuspendException(
                "A workflow Fiber suspended with a value of type `$type` that is not part of the workflow " .
                'suspension protocol. This usually means a non-workflow asynchronous API was called inside ' .
                'the workflow body. Use the Temporal workflow API instead.',
            ));
            return;
        }

        $this->nextPromise($suspended->promise, $suspended->interruptOnCancel);
    }

    private function orderedCancelHandlers(): array
    {
        $handlers = $this->onCancel;
        $suspensionID = $this->suspensionCancelID;

        if ($suspensionID === null || !isset($handlers[$suspensionID])) {
            return $handlers;
        }

        return [$suspensionID => $handlers[$suspensionID]] + $handlers;
    }

    private function addOnCancel(callable $handler, bool $cancellable = true, bool $internal = false): int
    {
        $id = ++$this->cancelID;

        // Sticky cancellation comes first: a handler attached to a scope that was cancelled,
        // settled or not, is notified at once.
        if (FeatureFlags::$propagateCancellationToNewScopes && $this->cancelled && $cancellable) {
            $savedContext = Facade::getCurrentContext();

            try {
                $this->makeCurrent();
                $handler($this->cancelReason);
            } finally {
                Workflow::setCurrentContext($savedContext);
            }

            return $id;
        }

        if ($this->closed && !$internal) {
            // A user callback attached after the scope settled never fires; the links of
            // requests and scopes started from a settled scope are still kept.
            return $id;
        }

        $this->onCancel[$id] = $handler;
        $internal and $this->internalCancelIDs[$id] = true;
        return $id;
    }

    private function forgetCancelHandler(int $id): void
    {
        unset($this->onCancel[$id], $this->internalCancelIDs[$id], $this->detachedLinkIDs[$id]);
    }

    private function nextPromise(PromiseInterface $promise, bool $interruptOnCancel): void
    {
        $settled = false;
        $cancelID = null;

        if ($interruptOnCancel) {
            $cancelID = $this->addOnCancel(function (?\Throwable $reason = null) use (&$settled): void {
                if ($settled) {
                    return;
                }

                $settled = true;
                $this->handleError($reason ?? new CanceledFailure(''));
            });
            $this->suspensionCancelID = $cancelID;
        }

        $cleanup = function () use (&$cancelID): void {
            if ($cancelID !== null) {
                $this->forgetCancelHandler($cancelID);
                if ($this->suspensionCancelID === $cancelID) {
                    $this->suspensionCancelID = null;
                }
                $cancelID = null;
            }
        };

        $onFulfilled = function (mixed $result) use (&$settled, $cleanup): mixed {
            if ($settled) {
                return $result;
            }

            $settled = true;
            $cleanup();
            $this->defer(
                function () use ($result): void {
                    if ($this->destroyed) {
                        return;
                    }

                    $this->makeCurrent();

                    try {
                        $suspended = $this->coroutine->resume($result);
                    } catch (\Throwable) {
                        return;
                    }

                    $this->advance($suspended);
                },
            );

            return $result;
        };

        $onRejected = function (\Throwable $e) use (&$settled, $cleanup): void {
            if ($settled) {
                throw $e;
            }

            $settled = true;
            $cleanup();
            $this->defer(
                function () use ($e): void {
                    if ($this->destroyed) {
                        return;
                    }

                    if ($e instanceof TemporalFailure && !$e->hasOriginalStackTrace()) {
                        $e->setOriginalStackTrace($this->context->getStackTrace());
                    }

                    $this->handleError($e);
                },
            );

            throw $e;
        };

        $promise
            ->then($onFulfilled, $onRejected)
            ->then(null, static fn(\Throwable $e) => null);
    }

    /**
     * Send error into the coroutine. If the code inside handles exception
     * we continue the flow. If the exception is bubbled up - the scope
     * itself handles it.
     */
    private function handleError(\Throwable $e): void
    {
        $this->makeCurrent();

        try {
            $suspended = $this->coroutine->throw($e);
        } catch (\Throwable) {
            return;
        }

        $this->advance($suspended);
    }

    private function onSignalOrUpdateException(\Throwable $e): void
    {
        if ($this->skipInvalidArguments && $e instanceof InvalidArgumentException) {
            $this->skipInvalidArguments = false;
            $this->onResult(null);
            return;
        }

        $this->onException($e);
    }

    private function onException(\Throwable $e): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->makeCurrent();
        $this->deferred->reject($e);
        $this->context->resolveConditions();

        $this->releaseExecutionState($e);
    }

    private function onResult(mixed $result): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->makeCurrent();
        $this->deferred->resolve($result);
        $this->context->resolveConditions();

        $this->releaseExecutionState($result);
    }

    private function releaseExecutionState(mixed $result): void
    {
        $onClose = $this->onClose;
        $this->onClose = [];
        // Links of pending requests and running children stay: cancel() of a settled scope
        // forwards to them, and they remove themselves when the request or child settles.
        $this->onCancel = \array_intersect_key($this->onCancel, $this->internalCancelIDs);
        unset($this->coroutine);

        try {
            foreach ($onClose as $close) {
                $close($result);
            }
        } finally {
            if (!$this->ownsContext) {
                $this->scopeContext->releaseScope();
            }
        }
    }

    /**
     * Evaluate await conditions with this scope as the current context, then restore the caller's context.
     *
     * The caller may be a user fiber of another scope (e.g. a Mutex release or a manually resolved
     * promise), so the context must not leak into it.
     */
    private function resolveConditionsInScope(): void
    {
        $savedContext = Facade::getCurrentContext();

        try {
            $this->makeCurrent();
            $this->context->resolveConditions();
        } finally {
            Workflow::setCurrentContext($savedContext);
        }
    }

    private function defer(\Closure $tick): void
    {
        if ($this->destroyed) {
            return;
        }

        $this->services->loop->once($this->layer, $tick);

        if ($this->services->queue->count() !== 0) {
            return;
        }

        // The synchronous tick may resume other fibers (nested in the current one); their contexts
        // must not leak into the caller.
        $savedContext = Facade::getCurrentContext();

        try {
            $this->services->loop->tick();
        } finally {
            Workflow::setCurrentContext($savedContext);
        }
    }

    private function unlinkFromParentIfIdle(): void
    {
        // A settled scope stays linked while it has running children or pending requests.
        if (!$this->closed || $this->children !== [] || $this->internalCancelIDs !== [] || $this->parentUnlink === null) {
            return;
        }

        $unlink = $this->parentUnlink;
        $this->parentUnlink = null;
        $unlink();
    }

    private function cancelFromParent(?\Throwable $reason = null): void
    {
        if ($this->detached && !$reason instanceof DestructMemorizedInstanceException) {
            return;
        }

        $this->cancel($reason);
    }
}
