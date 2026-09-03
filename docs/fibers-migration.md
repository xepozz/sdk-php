# Migrating workflow code to the Fiber runtime

Workflow handlers are plain PHP methods now. A call that used to return a promise
to `yield` blocks the workflow instead and returns the value. The runtime
suspends the workflow with PHP Fibers; there is nothing to yield, nothing to
`then()`, and a handler's declared return type is its real return type.

```php
// before
#[WorkflowMethod]
public function handle(string $input): \Generator
{
    $result = yield Workflow::executeActivity('process', [$input]);
    yield Workflow::timer(10);
    return $result;
}

// after
#[WorkflowMethod]
public function handle(string $input): string
{
    $result = Workflow::executeActivity('process', [$input]);
    Workflow::timer(10);
    return $result;
}
```

Requirements: PHP 8.1 or newer (already required by the SDK), RoadRunner with the
Temporal plugin as before. Nothing changes on the server side or in the workflow
history: a history recorded by the generator runtime replays on the Fiber runtime
(see `tests/Functional/GeneratorHistoryReplayTestCase.php`).

## Call mapping

| Generator runtime | Fiber runtime |
|---|---|
| `$r = yield Workflow::executeActivity(...)` | `$r = Workflow::executeActivity(...)` |
| `$r = yield $activities->method()` | `$r = $activities->method()` |
| `$r = yield Workflow::executeChildWorkflow(...)` | `$r = Workflow::executeChildWorkflow(...)` |
| `$r = yield $child->handle($x)` | `$r = $child->handle($x)` (blocks until the child completes, see below) |
| `yield $child->start($x)` / `yield $child->getResult()` | `$child->start($x)` / `$child->getResult()` |
| `yield $child->signalMethod()` / `yield $external->signal()` | `$child->signalMethod()` / `$external->signal()` |
| `yield Workflow::timer(10)` | `Workflow::timer(10)` |
| `yield Workflow::await(fn() => $this->ready)` | `Workflow::await(fn() => $this->ready)` |
| `$ok = yield Workflow::awaitWithTimeout(60, fn() => ...)` | `$ok = Workflow::awaitWithTimeout(60, fn() => ...)` |
| `$v = yield Workflow::sideEffect(fn() => ...)` | `$v = Workflow::sideEffect(fn() => ...)` |
| `$v = yield Workflow::getVersion('change', 1, 2)` | `$v = Workflow::getVersion('change', 1, 2)` |
| `$id = yield Workflow::uuid()` | `$id = Workflow::uuid()` |
| `return yield Workflow::continueAsNew(...)` | `return Workflow::continueAsNew(...)` |
| `$scope = Workflow::async(fn); $r = yield $scope` | `$r = Workflow::async(fn)->await()` or `Workflow::await($scope)` |
| `yield Promise::all([$p1, $p2])` | `Workflow::all([Workflow::async(fn() => ...), Workflow::async(fn() => ...)])` |
| `yield Promise::any([...])` / `yield Promise::race([...])` | `Workflow::any([...])` / `Workflow::race([...])` |
| `yield $mutex->lock()` | `$mutex->lock()` |
| `yield $mutex` | `Workflow::await($mutex)` |
| `yield $saga->compensate()` | `$saga->compensate()->await()` |
| `yield $this->helperGenerator()` | `$this->helper()` (a plain method) |
| `function handle(): \Generator` | the real return type |
| `->then(fn($r) => Workflow::executeActivity(...))` | run the follow-up in the same scope, or in `Workflow::async()`; a promise callback cannot suspend |

Every stub keeps a promise-returning twin for code that needs a promise:
`executeAsync()`, `startAsync()`, `getResultAsync()`, `getExecutionAsync()`,
`signalAsync()`, `cancelAsync()`. Any promise, including one obtained from
`Workflow::getCurrentContext()` (which keeps the promise-based
`WorkflowContextInterface`), is turned back into a value with
`Workflow::await($promise)`.

### Concurrency

Independent work runs in scopes. A scope starts a fiber, runs it until its first
suspension, and returns immediately.

```php
$first = Workflow::async(fn() => Workflow::executeActivity('first'));
$second = Workflow::async(fn() => Workflow::executeActivity('second'));

[$a, $b] = Workflow::all([$first, $second]);
```

### Typed child workflow stubs

Calling a workflow method on a typed child stub blocks until the child
completes. To interact with a running child, start it in a scope:

```php
$child = Workflow::newChildWorkflowStub(ChildWorkflow::class);
$result = Workflow::async(fn() => $child->handle($input));

$child->notify('payload'); // signal method, returns immediately

$value = $result->await();
```

`Workflow::newUntypedChildWorkflowStub()` offers the same through `start()`,
`signal()` and `getResult()`.

### Signals, updates and queries

Signal and update handlers are plain methods too. They may suspend; every
handler runs in its own scope. A query handler must not suspend: a suspending
call inside a query fails the query and leaves the workflow intact.

## Behaviour changes

- **Generator handlers are rejected.** A workflow, signal, update or scope
  handler that returns a `Generator` or a promise fails with
  `InvalidSuspendException`. There is no compatibility mode.
- **A promise callback cannot suspend.** `then()`, `catch()` and `finally()`
  callbacks run outside any workflow fiber. Calling `Workflow::async()`,
  `Workflow::executeActivity()` or any other suspending API from one throws
  `InvalidSuspendException`. Move the follow-up into the scope that awaits the
  promise.
- **Side effect callbacks and await conditions are read-only.** A suspending or
  command-sending call inside `Workflow::sideEffect(fn)` or a
  `Workflow::await(fn)` condition throws a `RuntimeException` naming the callback
  kind. A condition that throws fails only the await that owns it.
- **Awaiting inside a cancelled scope.** After a scope is cancelled every
  `Workflow::await()`, `Workflow::timer()`, activity, child workflow and signal
  call inside it throws `CanceledFailure` immediately, including
  `Workflow::await(fn() => true)`. This matches the Java, TypeScript, .NET and
  Ruby SDKs. `Workflow::sideEffect()`, `Workflow::getVersion()` and
  `Workflow::uuid()` keep working in a cancelled scope. Cleanup that must run
  after cancellation goes into `Workflow::asyncDetached()`.
- **`FeatureFlags::$propagateCancellationToNewScopes` defaults to `true`.** A
  scope or await created after the surrounding scope was cancelled is cancelled
  at once. Set it to `false` to restore the previous default; a workflow that
  awaits a promise while its scope is being cancelled may then hang.
- **`$scope->await()` versus `Workflow::await($scope)`.** `$scope->await()` is
  not interrupted when the awaiting scope is cancelled (like `Promise.get()` in
  Java); `Workflow::await($scope)` is (like `Workflow.await()` in Java). Both
  deliver the scope's real outcome, including a value the scope returned after
  catching its own `CanceledFailure`.
- **`cancel()` of a completed scope** cancels the scopes it started that are
  still running and is otherwise a no-op.
- **Foreign suspensions fail the scope.** A `Fiber::suspend()` that does not
  come from the SDK (an async HTTP client, a fiber-based event loop) fails the
  workflow task with `InvalidSuspendException`.
- **`WorkflowInboundCallsInterceptor::handleUpdate()`** now receives the
  handler's resolved value from `$next()` instead of a generator.
- **Workflow destruction.** When a workflow is evicted from the worker its
  suspended fibers are unwound with `DestructMemorizedInstanceException` until
  they terminate; `finally` blocks run, a `finally` that suspends again receives
  the exception again.
- **`Temporal\Experiments\Fibers`** and generator support classes are removed.

## Performance notes

Every scope is a PHP Fiber. Measured in the unit harness (one workflow starting
N scopes that each block on a `Workflow::await(fn)` condition, then releasing
all of them; PHP 8.4, single core shared with other work):

| N scopes | start, Fiber | start, generator | release all, Fiber | release all, generator | heap per scope |
|---|---|---|---|---|---|
| 1 000 | 0.46 s | 0.21 s | 0.04 s | 0.28 s | ~25 KB |
| 2 000 | 1.96 s | 1.04 s | 0.10 s | 1.25 s | ~22 KB |
| 5 000 | 16.3 s | 7.1 s | 0.37 s | 18.6 s | ~28 KB |

Starting is quadratic in both runtimes: every scheduler step re-evaluates every
pending callback condition, and the Fiber runtime takes two steps per scope
start where the generator runtime took one. Prefer awaiting promises or a single
aggregate condition when thousands of scopes are pending. Releasing is linear
now (the condition pass no longer recurses and the loop layers are queues);
peak heap for the 5 000 case dropped from 1.8 GB to 256 MB. The C stack of a
Fiber is reserved outside the PHP heap and is controlled by `fiber.stack_size`
(INI); the default is sufficient for workflow code.

## Checklist

1. Remove every `yield` in workflow, signal, update and scope handlers and
   replace `\Generator` return types with the real ones.
2. Replace `Promise::all/any/race` in workflow code with `Workflow::all/any/race`
   over scopes.
3. Move follow-up work out of `then()` callbacks.
4. Give typed child workflow interactions their own scope.
5. Keep side effect callbacks and await conditions free of suspending calls.
6. Run the workflow replayer against histories of running executions before
   deploying: `WorkflowReplayer::replayFromServer()`.
