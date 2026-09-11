<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\Client\GRPC;

use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Temporal\Api\Common\V1\Payload;
use Temporal\Api\Common\V1\Payloads;
use Temporal\Api\Workflowservice\V1\StartWorkflowExecutionRequest;
use Temporal\Api\Workflowservice\V1\WorkflowServiceClient;
use Temporal\Client\GRPC\Connection\ConnectionState;
use Temporal\Client\GRPC\ContextInterface;
use Temporal\Client\GRPC\ServiceClient;
use Temporal\Client\PayloadLimitOptions;
use Temporal\Interceptor\GrpcClientInterceptor;
use Temporal\Internal\Interceptor\Pipeline;

final class PayloadLimitsTestCase extends TestCase
{
    /** @var array<array-key, array{string, array}> */
    private array $records = [];

    public function testWarningIsLoggedOnRpcCall(): void
    {
        $client = $this->createClient()->withPayloadLimits(
            new PayloadLimitOptions(1024, 1024),
            $this->createLogger(),
        );

        $client->testCall($this->request(2000));

        self::assertCount(1, $this->records);
        self::assertStringContainsString('[TMPRL1103]', $this->records[0][0]);
        self::assertSame('testCall', $this->records[0][1]['method']);
    }

    public function testNoWarningWithoutLimits(): void
    {
        $client = $this->createClient();

        $client->testCall($this->request(2000));

        self::assertSame([], $this->records);
    }

    public function testLimitsCanBeDisabled(): void
    {
        $client = $this->createClient()->withPayloadLimits(null, $this->createLogger());

        $client->testCall($this->request(2000));

        self::assertSame([], $this->records);
    }

    public function testClientIsImmutable(): void
    {
        $client = $this->createClient();

        $result = $client->withPayloadLimits(new PayloadLimitOptions(1024, 1024), $this->createLogger());

        self::assertNotSame($client, $result);

        $client->testCall($this->request(2000));

        self::assertSame([], $this->records, 'The original client is not affected.');
    }

    protected function setUp(): void
    {
        $this->records = [];
        parent::setUp();
    }

    private function request(int $size): StartWorkflowExecutionRequest
    {
        return (new StartWorkflowExecutionRequest())->setInput(
            new Payloads(['payloads' => [(new Payload())->setData(\str_repeat('x', $size))]]),
        );
    }

    private function createLogger(): AbstractLogger
    {
        return new class($this->records) extends AbstractLogger {
            public function __construct(private array &$records) {}

            public function log($level, \Stringable|string $message, array $context = []): void
            {
                $this->records[] = [(string) $message, $context];
            }
        };
    }

    private function createClient(): ServiceClient
    {
        $stub = static fn() => new class extends WorkflowServiceClient {
            public function __construct() {}

            public function getConnectivityState($try_to_connect = false): int
            {
                return ConnectionState::Ready->value;
            }

            public function close(): void {}
        };

        return (new class($stub) extends ServiceClient {
            public function testCall(object $request): mixed
            {
                return $this->invoke('testCall', $request, null);
            }
        })->withInterceptorPipeline(
            Pipeline::prepare([new class implements GrpcClientInterceptor {
                public function interceptCall(
                    string $method,
                    object $arg,
                    ContextInterface $ctx,
                    callable $next,
                ): object {
                    // Do not perform a real RPC call
                    return (object) ['method' => $method];
                }
            }]),
        );
    }
}
