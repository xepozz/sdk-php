<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\Client;

use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Temporal\Api\Common\V1\Memo;
use Temporal\Api\Common\V1\Payload;
use Temporal\Api\Common\V1\Payloads;
use Temporal\Api\Common\V1\SearchAttributes;
use Temporal\Api\Failure\V1\ApplicationFailureInfo;
use Temporal\Api\Failure\V1\Failure;
use Temporal\Api\Workflowservice\V1\RespondActivityTaskFailedRequest;
use Temporal\Api\Workflowservice\V1\SignalWithStartWorkflowExecutionRequest;
use Temporal\Api\Workflowservice\V1\StartWorkflowExecutionRequest;
use Temporal\Common\PayloadLimitOptions;
use Temporal\Internal\Client\PayloadSizeChecker;

final class PayloadSizeCheckerTestCase extends TestCase
{
    /** @var array<array-key, array{string, array}> */
    private array $records = [];

    public function testWarnsOnOversizedInput(): void
    {
        $request = (new StartWorkflowExecutionRequest())
            ->setInput(self::payloads(2000));

        $this->check($request, 'StartWorkflowExecution');

        self::assertCount(1, $this->records);
        [$message, $context] = $this->records[0];
        self::assertStringContainsString('[TMPRL1103]', $message);
        self::assertStringContainsString('payloads', $message);
        self::assertSame('StartWorkflowExecution', $context['method']);
        self::assertSame(1024, $context['limit']);
        self::assertGreaterThan(2000, $context['size']);
    }

    public function testKeepsSilentBelowTheLimit(): void
    {
        $request = (new StartWorkflowExecutionRequest())
            ->setInput(self::payloads(100));

        $this->check($request, 'StartWorkflowExecution');

        self::assertSame([], $this->records);
    }

    public function testWarnsOnOversizedMemoSeparately(): void
    {
        $request = (new StartWorkflowExecutionRequest())
            ->setMemo(new Memo(['fields' => ['key' => self::payload(2000)]]));

        $this->check($request, 'StartWorkflowExecution');

        self::assertCount(1, $this->records);
        self::assertStringContainsString('memo', $this->records[0][0]);
    }

    public function testMemoUsesItsOwnLimit(): void
    {
        $request = (new StartWorkflowExecutionRequest())
            ->setMemo(new Memo(['fields' => ['key' => self::payload(2000)]]));

        // The payload limit is large enough, but the memo limit is not
        $this->check($request, 'StartWorkflowExecution', new PayloadLimitOptions(1024 * 1024, 1024));

        self::assertCount(1, $this->records);
        self::assertStringContainsString('memo', $this->records[0][0]);
    }

    public function testSearchAttributesAreMeasuredAsKeyAndDataLength(): void
    {
        $request = (new StartWorkflowExecutionRequest())
            ->setSearchAttributes(new SearchAttributes([
                'indexed_fields' => ['attr' => self::payload(2000)],
            ]));

        $this->check($request, 'StartWorkflowExecution');

        self::assertCount(1, $this->records);
        // 4 bytes of the key plus the payload data, without the protobuf overhead
        self::assertSame(2004, $this->records[0][1]['size']);
    }

    public function testWarnsForEveryOversizedFieldOfTheRequest(): void
    {
        $request = (new SignalWithStartWorkflowExecutionRequest())
            ->setInput(self::payloads(2000))
            ->setSignalInput(self::payloads(3000));

        $this->check($request, 'SignalWithStartWorkflowExecution');

        self::assertCount(2, $this->records);
    }

    public function testWalksIntoNestedMessages(): void
    {
        $request = (new RespondActivityTaskFailedRequest())
            ->setFailure(
                (new Failure())->setApplicationFailureInfo(
                    (new ApplicationFailureInfo())->setDetails(self::payloads(2000)),
                ),
            );

        $this->check($request, 'RespondActivityTaskFailed');

        self::assertCount(1, $this->records);
    }

    public function testSurvivesRecursiveMessages(): void
    {
        $failure = (new Failure())->setApplicationFailureInfo(
            (new ApplicationFailureInfo())->setDetails(self::payloads(2000)),
        );
        // Failure.cause is a Failure itself
        $failure->setCause(clone $failure);

        $request = (new RespondActivityTaskFailedRequest())->setFailure($failure);

        $this->check($request, 'RespondActivityTaskFailed');

        self::assertCount(2, $this->records);
    }

    public function testDisabledLimitsAreNotChecked(): void
    {
        $request = (new StartWorkflowExecutionRequest())
            ->setInput(self::payloads(2000))
            ->setMemo(new Memo(['fields' => ['key' => self::payload(2000)]]));

        $this->check($request, 'StartWorkflowExecution', new PayloadLimitOptions(null, null));

        self::assertSame([], $this->records);
    }

    public function testNonProtobufRequestIsIgnored(): void
    {
        $this->check(new \stdClass(), 'SomeCall');

        self::assertSame([], $this->records);
    }

    protected function setUp(): void
    {
        $this->records = [];
        parent::setUp();
    }

    private function check(object $request, string $method, ?PayloadLimitOptions $options = null): void
    {
        $logger = new class($this->records) extends AbstractLogger {
            public function __construct(private array &$records) {}

            public function log($level, \Stringable|string $message, array $context = []): void
            {
                $this->records[] = [(string) $message, $context];
            }
        };

        $checker = new PayloadSizeChecker($options ?? new PayloadLimitOptions(1024, 1024), $logger);
        $checker->check($method, $request);
    }

    private static function payload(int $size): Payload
    {
        return (new Payload())->setData(\str_repeat('x', $size));
    }

    private static function payloads(int $size): Payloads
    {
        return new Payloads(['payloads' => [self::payload($size)]]);
    }
}
