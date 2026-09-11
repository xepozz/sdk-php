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
use Temporal\Client\WorkflowClient;
use Temporal\DataConverter\DataConverter;
use Temporal\DataConverter\EncodedValues;
use Temporal\Exception\PayloadSizeExceededException;
use Temporal\Interceptor\Header;
use Temporal\Internal\Transport\PayloadSizeLimiter;
use Temporal\Internal\Transport\Request\ExecuteActivity;
use Temporal\Internal\Transport\Request\ExecuteLocalActivity;
use Temporal\Internal\Transport\Request\UpsertMemo;

final class PayloadSizeLimiterTestCase extends TestCase
{
    public function testOversizedCommandIsNotSent(): void
    {
        $limiter = $this->limiter(payloadSize: 1024);

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
        $this->expectException(PayloadSizeExceededException::class);

        $this->limiter(payloadSize: 1024)
            ->check([$this->activity(10), $this->activity(10), $this->activity(2000)]);
    }

    public function testCommandsBelowTheLimitPassThrough(): void
    {
        $commands = [$this->activity(10), $this->activity(20)];

        self::assertSame($commands, $this->limiter(payloadSize: 1024)->check($commands));
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
            $this->limiter(payloadSize: 1024)->check([$request]),
            'Local Activity input never reaches the server.',
        );
    }

    public function testMemoUsesTheMemoLimit(): void
    {
        $this->expectException(PayloadSizeExceededException::class);

        // The payload limit is large enough, but the memo limit is not
        $this->limiter(payloadSize: 1024 * 1024, memoSize: 1024)
            ->check([new UpsertMemo(['key' => \str_repeat('x', 2000)])]);
    }

    public function testLimitsComeFromTheNamespace(): void
    {
        $limiter = PayloadSizeLimiter::fromClient(
            $this->client(2048, 64),
            DataConverter::createDefault(),
        );

        self::assertNotNull($limiter);

        try {
            $limiter->check([$this->activity(4000)]);
            self::fail('The namespace limit was not applied.');
        } catch (PayloadSizeExceededException $e) {
            self::assertSame(2048, $e->limit);
        }
    }

    public function testNamespaceWithoutLimitsIsNotEnforced(): void
    {
        self::assertNull(
            PayloadSizeLimiter::fromClient($this->client(0, 0), DataConverter::createDefault()),
        );
    }

    public function testWithoutAClientThereIsNothingToAsk(): void
    {
        self::assertNull(PayloadSizeLimiter::fromClient(null, DataConverter::createDefault()));
    }

    public function testUnreachableNamespaceLeavesTheWorkerAsItWas(): void
    {
        $client = $this->createMock(WorkflowClient::class);
        $client->method('getServiceClient')->willThrowException(new \RuntimeException('Unavailable'));

        self::assertNull(PayloadSizeLimiter::fromClient($client, DataConverter::createDefault()));
    }

    private function limiter(int $payloadSize = 0, int $memoSize = 0): PayloadSizeLimiter
    {
        $limiter = PayloadSizeLimiter::fromClient(
            $this->client($payloadSize, $memoSize),
            DataConverter::createDefault(),
        );

        \assert($limiter !== null);

        return $limiter;
    }

    private function client(int $payloadSize, int $memoSize): WorkflowClient
    {
        $info = new NamespaceInfo();
        if (\method_exists($info, 'setLimits')) {
            $info->setLimits(
                (new NamespaceInfo\Limits())
                    ->setBlobSizeLimitError($payloadSize)
                    ->setMemoSizeLimitError($memoSize),
            );
        }

        $serviceClient = $this->createMock(ServiceClientInterface::class);
        $serviceClient->method('getContext')
            ->willReturn(Context::default()->withMetadata(['Temporal-Namespace' => ['default']]));
        $serviceClient->method('DescribeNamespace')
            ->with(self::callback(
                static fn(DescribeNamespaceRequest $request): bool => $request->getNamespace() === 'default',
            ))
            ->willReturn((new DescribeNamespaceResponse())->setNamespaceInfo($info));

        $client = $this->createMock(WorkflowClient::class);
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
