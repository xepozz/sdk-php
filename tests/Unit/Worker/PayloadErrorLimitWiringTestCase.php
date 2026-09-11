<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\Worker;

use PHPUnit\Framework\TestCase;
use Temporal\Common\PayloadLimitOptions;
use Temporal\DataConverter\DataConverter;
use Temporal\DataConverter\EncodedValues;
use Temporal\Exception\PayloadSizeExceededException;
use Temporal\Interceptor\Header;
use Temporal\Internal\Queue\ArrayQueue;
use Temporal\Internal\Transport\PayloadSizeLimiter;
use Temporal\Internal\Transport\Request\ExecuteActivity;
use Temporal\Worker\Transport\RPCConnectionInterface;
use Temporal\Worker\WorkerOptions;
use Temporal\WorkerFactory;

/**
 * The limits are enforced on the way out of the Worker, so the wiring in the factory is what
 * decides whether a batch is measured at all.
 */
final class PayloadErrorLimitWiringTestCase extends TestCase
{
    public function testTheBatchOfAWorkerIsMeasured(): void
    {
        $this->expectException(PayloadSizeExceededException::class);

        $this->limit($this->factory(), ['taskQueue' => 'default']);
    }

    public function testAWorkerCanLeaveTheEnforcementToRoadRunner(): void
    {
        $factory = $this->factory(WorkerOptions::new()->withDisablePayloadErrorLimit(true));

        self::assertCount(1, $this->limit($factory, ['taskQueue' => 'default']));
    }

    public function testABatchOfNoWorkerIsNotMeasured(): void
    {
        // The registration handshake carries no task queue and never reaches the server
        self::assertCount(1, $this->limit($this->factory(), []));
    }

    public function testAnUnknownTaskQueueIsNotMeasured(): void
    {
        self::assertCount(1, $this->limit($this->factory(), ['taskQueue' => 'other']));
    }

    public function testWithoutLimitsTheBatchIsUntouched(): void
    {
        $factory = $this->factory(withLimiter: false);

        self::assertCount(1, $this->limit($factory, ['taskQueue' => 'default']));
    }

    public function testAReplayedBatchIsNotMeasured(): void
    {
        self::assertCount(1, $this->limit($this->factory(), ['taskQueue' => 'default'], replaying: true));
    }

    public function testTheResponsesOfABatchGoThroughTheLimit(): void
    {
        $factory = new class(DataConverter::createDefault(), $this->createMock(RPCConnectionInterface::class)) extends WorkerFactory {
            public bool $limited = false;

            protected function createPayloadSizeLimiter(): ?PayloadSizeLimiter
            {
                return null;
            }

            protected function encodeResponses(iterable $commands, array $headers, bool $replaying): string
            {
                $this->limited = true;

                return parent::encodeResponses($commands, $headers, $replaying);
            }
        };

        $method = new \ReflectionMethod(WorkerFactory::class, 'dispatch');
        // An empty batch is enough: what is under test is that every batch takes this way out
        $method->invoke($factory, '[]', ['taskQueue' => 'default']);

        self::assertTrue($factory->limited, 'Every batch a Worker sends goes through the limit.');
    }

    /**
     * @param array<string, mixed> $headers
     * @return iterable<mixed>
     */
    private function limit(WorkerFactory $factory, array $headers, bool $replaying = false): iterable
    {
        $queue = new ArrayQueue();
        $queue->push(new ExecuteActivity(
            'SimpleActivity.echo',
            EncodedValues::fromValues([\str_repeat('x', 2000)]),
            [],
            Header::empty(),
        ));

        $method = new \ReflectionMethod(WorkerFactory::class, 'limitPayloads');

        return $method->invoke($factory, $queue, $headers, $replaying);
    }

    private function factory(?WorkerOptions $options = null, bool $withLimiter = true): WorkerFactory
    {
        $factory = new WorkerFactory(
            DataConverter::createDefault(),
            $this->createMock(RPCConnectionInterface::class),
        );
        // The Workflow warnings are off, so only the error limit can fail anything here
        $factory->newWorker(
            'default',
            ($options ?? WorkerOptions::new())->withPayloadLimits(PayloadLimitOptions::disabled()),
        );

        // The namespace is asked for its limits when the Worker starts, which is not what is
        // under test here
        (new \ReflectionProperty(WorkerFactory::class, 'payloadSizeLimiter'))->setValue(
            $factory,
            $withLimiter
                ? new PayloadSizeLimiter(1024, DataConverter::createDefault())
                : null,
        );

        return $factory;
    }
}
