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
use Temporal\Worker\Environment\Environment;
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

    public function testTheResponsesOfABatchGoThroughTheLimit(): void
    {
        // `dispatch()` is private and needs an encoded batch to be called for real, so the one
        // line that connects the limit to the Worker loop is pinned from the source
        $source = (string) \file_get_contents((string) (new \ReflectionClass(WorkerFactory::class))->getFileName());

        self::assertStringContainsString(
            'encode($this->limitPayloads($this->responses, $headers))',
            $source,
        );
    }

    /**
     * @param array<string, mixed> $headers
     * @return iterable<mixed>
     */
    private function limit(WorkerFactory $factory, array $headers): iterable
    {
        $queue = new ArrayQueue();
        $queue->push(new ExecuteActivity(
            'SimpleActivity.echo',
            EncodedValues::fromValues([\str_repeat('x', 2000)]),
            [],
            Header::empty(),
        ));

        $method = new \ReflectionMethod(WorkerFactory::class, 'limitPayloads');

        return $method->invoke($factory, $queue, $headers);
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
                ? new PayloadSizeLimiter(1024, 1024, DataConverter::createDefault(), new Environment())
                : null,
        );

        return $factory;
    }
}
