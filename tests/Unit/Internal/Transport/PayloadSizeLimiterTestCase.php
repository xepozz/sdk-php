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
use Temporal\Api\PBNamespace\V1\NamespaceInfo;
use Temporal\Api\Workflowservice\V1\DescribeNamespaceRequest;
use Temporal\Api\Workflowservice\V1\DescribeNamespaceResponse;
use Temporal\Client\GRPC\Context;
use Temporal\Client\GRPC\ContextInterface;
use Temporal\Client\GRPC\ServiceClientInterface;
use Temporal\Client\WorkflowClientInterface;
use Temporal\DataConverter\DataConverter;
use Temporal\DataConverter\EncodedValues;
use Temporal\Exception\PayloadSizeExceededException;
use Temporal\Interceptor\Header;
use Temporal\Internal\Queue\ArrayQueue;
use Temporal\Internal\Transport\PayloadSizeLimiter;
use Temporal\Internal\Transport\Request\ExecuteActivity;
use Temporal\Internal\Transport\Request\ExecuteLocalActivity;
use Temporal\Internal\Transport\Request\UpsertMemo;
use Temporal\Worker\Environment\Environment;
use Temporal\Worker\Transport\Command\Client\SuccessClientResponse;
use Temporal\Worker\Transport\Command\Server\TickInfo;

final class PayloadSizeLimiterTestCase extends TestCase
{
    public function testOversizedCommandIsNotSent(): void
    {
        try {
            $this->limiter(payloadSize: 1024)->enforce([$this->activity(2000)]);
            self::fail('The oversized command was let through.');
        } catch (PayloadSizeExceededException $e) {
            self::assertStringContainsString('[TMPRL1103]', $e->getMessage());
            self::assertStringContainsString('error limit', $e->getMessage());
            // The message is the only thing RoadRunner passes on, so it carries the numbers
            self::assertStringContainsString('limit: 1024', $e->getMessage());
            self::assertSame(1024, $e->limit);
            self::assertGreaterThan(2000, $e->size);
        }
    }

    public function testEveryCommandOfTheBatchIsMeasured(): void
    {
        $this->expectException(PayloadSizeExceededException::class);

        $this->limiter(payloadSize: 1024)
            ->enforce([$this->activity(10), $this->activity(10), $this->activity(2000)]);
    }

    public function testCommandsBelowTheLimitPassThrough(): void
    {
        $commands = [$this->activity(10), $this->activity(20)];

        self::assertSame($commands, $this->limiter(payloadSize: 1024)->enforce($commands));
    }

    public function testTheQueueOfAWorkerIsKept(): void
    {
        // The queue drains as it is read, so the measured commands must come back out
        $queue = new ArrayQueue();
        $queue->push($first = $this->activity(10));
        $queue->push($second = $this->activity(20));

        self::assertSame([$first, $second], $this->limiter(payloadSize: 1024)->enforce($queue));
    }

    public function testTheQueueIsNotLeftHalfDrainedByAFailure(): void
    {
        $queue = new ArrayQueue();
        $queue->push($this->activity(2000));
        $queue->push($this->activity(10));

        try {
            $this->limiter(payloadSize: 1024)->enforce($queue);
        } catch (PayloadSizeExceededException) {
            // The commands of a failed task are dropped with it, none may leak into the next one
        }

        self::assertCount(0, $queue);
    }

    public function testLocalActivityArgumentsAreNotLimited(): void
    {
        $request = new ExecuteLocalActivity(
            'SimpleActivity.echo',
            EncodedValues::fromValues([\str_repeat('x', 2000)]),
            [],
            Header::empty(),
        );

        self::assertCount(
            1,
            $this->limiter(payloadSize: 1024)->enforce([$request]),
            'Local Activity input never reaches the server.',
        );
    }

    public function testHandlerResultsAreLeftToTheServer(): void
    {
        // An oversized Query result must not fail the Workflow Task, the server rejects it
        $response = new SuccessClientResponse(1, EncodedValues::fromValues([\str_repeat('x', 2000)]));

        self::assertCount(1, $this->limiter(payloadSize: 1024)->enforce([$response]));
    }

    public function testReplayedCommandsAreNotMeasured(): void
    {
        // A replayed command is matched against the history, it is not sent anywhere
        $limiter = $this->limiter(payloadSize: 1024, replaying: true);

        self::assertCount(1, $limiter->enforce([$this->activity(2000)]));
    }

    public function testMemoUsesTheMemoLimit(): void
    {
        $this->expectException(PayloadSizeExceededException::class);

        // The payload limit is large enough, but the memo limit is not
        $this->limiter(payloadSize: 1024 * 1024, memoSize: 1024)
            ->enforce([new UpsertMemo(['key' => \str_repeat('x', 2000)])]);
    }

    public function testLimitsComeFromTheNamespace(): void
    {
        $limiter = PayloadSizeLimiter::fromClient(
            $this->client(2048, 64),
            DataConverter::createDefault(),
            new Environment(),
        );

        self::assertNotNull($limiter);

        try {
            $limiter->enforce([$this->activity(4000)]);
            self::fail('The namespace limit was not applied.');
        } catch (PayloadSizeExceededException $e) {
            self::assertSame(2048, $e->limit);
        }
    }

    public function testTheNamespaceIsAskedWithABoundedTimeout(): void
    {
        $seen = null;
        $client = $this->client(2048, 0, static function (ContextInterface $ctx) use (&$seen): void {
            $seen = $ctx->getDeadline();
        });

        PayloadSizeLimiter::fromClient($client, DataConverter::createDefault(), new Environment());

        self::assertNotNull($seen, 'A Worker must not hang on a server that does not answer.');
    }

    public function testNamespaceWithoutLimitsIsNotEnforced(): void
    {
        self::assertNull(
            PayloadSizeLimiter::fromClient($this->client(0, 0), DataConverter::createDefault(), new Environment()),
        );
    }

    public function testUnreachableNamespaceLeavesTheWorkerAsItWas(): void
    {
        $client = $this->createMock(WorkflowClientInterface::class);
        $client->method('getServiceClient')->willThrowException(new \RuntimeException('Unavailable'));

        self::assertNull(
            PayloadSizeLimiter::fromClient($client, DataConverter::createDefault(), new Environment()),
        );
    }

    private function limiter(int $payloadSize = 0, int $memoSize = 0, bool $replaying = false): PayloadSizeLimiter
    {
        $env = new Environment();
        $replaying and $env->update(new TickInfo(new \DateTimeImmutable(), isReplaying: true));

        return new PayloadSizeLimiter($payloadSize, $memoSize, DataConverter::createDefault(), $env);
    }

    private function client(int $payloadSize, int $memoSize, ?\Closure $onCall = null): WorkflowClientInterface
    {
        $info = (new NamespaceInfo())->setLimits(
            (new NamespaceInfo\Limits())
                ->setBlobSizeLimitError($payloadSize)
                ->setMemoSizeLimitError($memoSize),
        );

        $serviceClient = $this->createMock(ServiceClientInterface::class);
        $serviceClient->method('getContext')
            ->willReturn(Context::default()->withMetadata(['Temporal-Namespace' => ['test-namespace']]));
        $serviceClient->method('DescribeNamespace')
            ->willReturnCallback(
                static function (DescribeNamespaceRequest $request, ?ContextInterface $ctx) use (
                    $info,
                    $onCall,
                ): DescribeNamespaceResponse {
                    self::assertSame('test-namespace', $request->getNamespace());
                    $onCall === null || $ctx === null or $onCall($ctx);

                    return (new DescribeNamespaceResponse())->setNamespaceInfo($info);
                },
            );

        $client = $this->createMock(WorkflowClientInterface::class);
        $client->method('getServiceClient')->willReturn($serviceClient);

        return $client;
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
