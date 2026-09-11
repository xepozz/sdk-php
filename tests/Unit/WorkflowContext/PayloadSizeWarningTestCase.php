<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\WorkflowContext;

use Psr\Log\AbstractLogger;
use Temporal\Activity\ActivityOptions;
use Temporal\Common\PayloadLimitOptions;
use Temporal\DataConverter\DataConverter;
use Temporal\Api\Common\V1\Payload;
use Temporal\DataConverter\DataConverterInterface;
use Temporal\DataConverter\EncodedValues;
use Temporal\Exception\DataConverterException;
use Temporal\Interceptor\Header;
use Temporal\Internal\Transport\Request\ExecuteActivity;
use Temporal\Internal\Transport\Request\ExecuteLocalActivity;
use Temporal\Internal\Workflow\PayloadSizeWarner;
use Temporal\Tests\Activity\SimpleActivity;
use Temporal\Tests\Unit\AbstractUnit;
use Temporal\Tests\Unit\Framework\WorkerFactoryMock;
use Temporal\Tests\Unit\Framework\WorkerMock;
use Temporal\Worker\WorkerFactoryInterface;
use Temporal\Worker\Environment\Environment;
use Temporal\Worker\Transport\Command\Server\TickInfo;
use Temporal\Worker\WorkerInterface;
use Temporal\Worker\WorkerOptions;
use Temporal\Workflow;
use Temporal\Workflow\WorkflowMethod;

final class PayloadSizeWarningTestCase extends AbstractUnit
{
    /** @var array<array-key, array{string, array}> */
    private array $records = [];

    private WorkerFactoryInterface $factory;

    /** @var WorkerMock|WorkerInterface */
    private $worker;

    public function testWarnsOnOversizedActivityArgument(): void
    {
        $this->runWorkflowWithArgument(\str_repeat('x', 2000));

        self::assertCount(1, $this->records);
        [$message, $context] = $this->records[0];
        self::assertStringContainsString('[TMPRL1103]', $message);
        self::assertSame('ExecuteActivity', $context['command']);
        self::assertSame(1024, $context['limit']);
        self::assertGreaterThan(2000, $context['size']);
    }

    public function testKeepsSilentBelowTheLimit(): void
    {
        $this->runWorkflowWithArgument('small');

        self::assertSame([], $this->records);
    }

    public function testReplayedCommandIsNotMeasuredAtAll(): void
    {
        // A raw logger, so only the warner's own replay guard can keep it silent
        $environment = new Environment();
        $environment->update(new TickInfo(new \DateTimeImmutable(), isReplaying: true));

        $warner = new PayloadSizeWarner(
            new PayloadLimitOptions(1024, 1024),
            $this->throwingConverter(),
            $environment,
            $this->spyLogger(),
        );

        $warner->check($this->activityRequest(2000));

        self::assertSame([], $this->records);
    }

    public function testReplayIsSkippedEvenWithLoggingInReplayEnabled(): void
    {
        $this->runWorkflowWithArgument(
            \str_repeat('x', 2000),
            replaying: true,
            enableLoggingInReplay: true,
        );

        // A replayed command is never sent, so it is not reported regardless of the logger settings
        self::assertSame([], $this->records);
    }

    public function testUnconvertibleValueIsNotReportedAndDoesNotThrow(): void
    {
        // The converter fails on the value; the check must stay silent and let the codec fail
        // later, exactly as it did before the check existed.
        $warner = new PayloadSizeWarner(
            new PayloadLimitOptions(1024, 1024),
            $this->throwingConverter(),
            new Environment(),
            $this->spyLogger(),
        );

        $warner->check($this->activityRequest(2000));

        self::assertSame([], $this->records);
    }

    public function testLocalActivityArgumentsAreNotReported(): void
    {
        $request = new ExecuteLocalActivity(
            'SimpleActivity.echo',
            EncodedValues::fromValues([\str_repeat('x', 2000)]),
            [],
            Header::empty(),
        );

        $this->warner()->check($request);

        self::assertSame([], $this->records, 'Local Activity input never reaches the server.');
    }

    public function testActivityArgumentsAreReported(): void
    {
        $request = new ExecuteActivity(
            'SimpleActivity.echo',
            EncodedValues::fromValues([\str_repeat('x', 2000)]),
            [],
            Header::empty(),
        );

        $this->warner()->check($request);

        self::assertCount(1, $this->records);
        self::assertSame('ExecuteActivity', $this->records[0][1]['command']);
    }

    public function testWarningCanBeDisabled(): void
    {
        $this->runWorkflowWithArgument(\str_repeat('x', 2000), null);

        self::assertSame([], $this->records);
    }

    protected function setUp(): void
    {
        $this->records = [];
        $this->factory = WorkerFactoryMock::create();

        parent::setUp();
    }

    private function activityRequest(int $size): ExecuteActivity
    {
        return new ExecuteActivity(
            'SimpleActivity.echo',
            EncodedValues::fromValues([\str_repeat('x', $size)]),
            [],
            Header::empty(),
        );
    }

    private function throwingConverter(): DataConverterInterface
    {
        return new class implements DataConverterInterface {
            public function fromPayload(Payload $payload, $type): mixed
            {
                throw new DataConverterException('Not supported');
            }

            public function toPayload($value): Payload
            {
                throw new DataConverterException('Not supported');
            }
        };
    }

    private function warner(): PayloadSizeWarner
    {
        return new PayloadSizeWarner(
            new PayloadLimitOptions(1024, 1024),
            DataConverter::createDefault(),
            new Environment(),
            $this->spyLogger(),
        );
    }

    private function spyLogger(): AbstractLogger
    {
        return new class($this->records) extends AbstractLogger {
            public function __construct(private array &$records) {}

            public function log($level, \Stringable|string $message, array $context = []): void
            {
                $this->records[] = [(string) $message, $context];
            }
        };
    }

    private function runWorkflowWithArgument(
        string $argument,
        ?PayloadLimitOptions $limits = new PayloadLimitOptions(1024, 1024),
        bool $replaying = false,
        bool $enableLoggingInReplay = false,
    ): void {
        $logger = $this->spyLogger();

        $options = WorkerOptions::new()
            ->withEnableLoggingInReplay($enableLoggingInReplay)
            ->withPayloadLimits($limits ?? PayloadLimitOptions::disabled());

        $this->worker = $this->factory->newWorker(options: $options, logger: $logger);
        $this->worker->registerWorkflowObject(
            new
            #[Workflow\WorkflowInterface]
            class {
                #[WorkflowMethod(name: 'PayloadSizeWorkflow')]
                public function handler(string $argument): iterable
                {
                    return yield Workflow::executeActivity(
                        'SimpleActivity.echo',
                        [$argument],
                        ActivityOptions::new()->withStartToCloseTimeout(5),
                    );
                }
            }
        );

        $replaying
            ? $this->worker->replayWorkflow('PayloadSizeWorkflow', $argument)
            : $this->worker->runWorkflow('PayloadSizeWorkflow', $argument);
        $this->worker->expectActivityCall(SimpleActivity::class, 'echo', 'done');
        $this->worker->assertWorkflowReturns('done');
        $this->factory->run($this->worker);
    }
}
