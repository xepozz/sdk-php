# Histories recorded by the generator runtime

Every file here is a workflow history produced by the generator-based SDK
(`temporalio/sdk-php` master at `4bda0fec`, before the Fiber runtime) against
`temporal-test-server` with time skipping. `GeneratorHistoryReplayTestCase`
replays each of them on the Fiber runtime: the command sequence a workflow emits
must not depend on how its code is suspended.

The workflow types are the fixtures in `tests/Fixtures/src/Workflow`. The file
name is the workflow type. Scenarios that need client interaction:

| History | Driver |
|---|---|
| `SimpleSignalledWorkflow` | signals `add(5)`, `add(3)` |
| `WaitWorkflow` | signal `unlock('unlock the condition')` |
| `LoopWithSignalCoroutinesWorkflow` | started with `4`, signals `addValue(test1..test4)` |
| `Update.greet` | updates `addName('John')`, `addNameViaActivity('Doe')`, signal `exit` |
| `AwaitsUpdate.greet` | `startUpdate('await', 'key')`, update `resolveValue('key', 'resolved')`, signal `exit` |
| `CancelledWorkflow`, `CancelledWithCompensationWorkflow` | cancelled by the client after `ActivityTaskScheduled` |
| `CancelledNestedWorkflow`, `SimpleSignalledWorkflowWithSleep` (started with `-1`) | cancelled by the client after `TimerStarted` |

Everything else is started with the arguments listed in
`tests/Functional/RecordGeneratorHistories.php`, which is the recorder.
