<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\Internal\Transport;

use PHPUnit\Framework\TestCase;
use Temporal\Common\PayloadLimitOptions;
use Temporal\DataConverter\DataConverter;
use Temporal\DataConverter\EncodedValues;
use Temporal\Exception\PayloadSizeExceededException;
use Temporal\Interceptor\Header;
use Temporal\Internal\Transport\PayloadSizeLimiter;
use Temporal\Internal\Transport\Request\ExecuteActivity;
use Temporal\Internal\Transport\Request\ExecuteLocalActivity;
use Temporal\Internal\Transport\Request\UpsertMemo;
use Temporal\Worker\WorkerInterface;
use Temporal\Worker\WorkerOptions;

final class PayloadSizeLimiterTestCase extends TestCase
{
    public function testOversizedCommandIsNotSent(): void
    {
        $limiter = new PayloadSizeLimiter(
            PayloadLimitOptions::new()->withPayloadSizeError(1024),
            DataConverter::createDefault(),
        );

        try {
            $limiter->check([$this->activity(2000)]);
            self::fail('The oversized command was let through.');
        } catch (PayloadSizeExceededException $e) {
            self::assertStringContainsString('[TMPRL1103]', $e->getMessage());
            self::assertStringContainsString('error limit', $e->getMessage());
            self::assertSame(1024, $e->limit);
            self::assertGreaterThan(2000, $e->size);
        }
    }

    public function testTheWholeBatchIsMeasured(): void
    {
        $limiter = new PayloadSizeLimiter(
            PayloadLimitOptions::new()->withPayloadSizeError(1024),
            DataConverter::createDefault(),
        );

        $this->expectException(PayloadSizeExceededException::class);

        $limiter->check([$this->activity(10), $this->activity(10), $this->activity(2000)]);
    }

    public function testCommandsBelowTheLimitPassThrough(): void
    {
        $limiter = new PayloadSizeLimiter(
            PayloadLimitOptions::new()->withPayloadSizeError(1024),
            DataConverter::createDefault(),
        );

        $commands = [$this->activity(10), $this->activity(20)];

        self::assertSame($commands, $limiter->check($commands));
    }

    public function testLocalActivityArgumentsAreNotLimited(): void
    {
        $limiter = new PayloadSizeLimiter(
            PayloadLimitOptions::new()->withPayloadSizeError(1024),
            DataConverter::createDefault(),
        );

        $request = new ExecuteLocalActivity(
            'SimpleActivity.echo',
            EncodedValues::fromValues([\str_repeat('x', 2000)]),
            [],
            Header::empty(),
        );

        self::assertCount(1, $limiter->check([$request]), 'Local Activity input never reaches the server.');
    }

    public function testMemoUsesTheMemoErrorLimit(): void
    {
        $limiter = new PayloadSizeLimiter(
            PayloadLimitOptions::new()->withPayloadSizeError(1024 * 1024)->withMemoSizeError(1024),
            DataConverter::createDefault(),
        );

        $this->expectException(PayloadSizeExceededException::class);

        $limiter->check([new UpsertMemo(['key' => \str_repeat('x', 2000)])]);
    }

    public function testWorkerWithoutErrorLimitsHasNoLimiter(): void
    {
        $worker = $this->worker(WorkerOptions::new());

        self::assertNull(PayloadSizeLimiter::forWorker($worker, DataConverter::createDefault()));
    }

    public function testWorkerWithErrorLimitsHasOne(): void
    {
        $worker = $this->worker(
            WorkerOptions::new()->withPayloadLimits(PayloadLimitOptions::new()->withPayloadSizeError(1024)),
        );

        self::assertNotNull(PayloadSizeLimiter::forWorker($worker, DataConverter::createDefault()));
    }

    public function testDisablingTheErrorLimitLeavesItToRoadRunner(): void
    {
        $worker = $this->worker(
            WorkerOptions::new()
                ->withPayloadLimits(PayloadLimitOptions::new()->withPayloadSizeError(1024))
                ->withDisablePayloadErrorLimit(true),
        );

        self::assertNull(PayloadSizeLimiter::forWorker($worker, DataConverter::createDefault()));
    }

    private function worker(WorkerOptions $options): WorkerInterface
    {
        $worker = $this->createMock(WorkerInterface::class);
        $worker->method('getOptions')->willReturn($options);

        return $worker;
    }

    private function activity(int $size): ExecuteActivity
    {
        return new ExecuteActivity(
            'SimpleActivity.echo',
            EncodedValues::fromValues([\str_repeat('x', $size)]),
            [],
            Header::empty(),
        );
    }
}
