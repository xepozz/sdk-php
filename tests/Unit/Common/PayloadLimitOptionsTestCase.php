<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\Common;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Temporal\Client\ClientOptions;
use Temporal\Common\PayloadLimitOptions;
use Temporal\Worker\WorkerOptions;

final class PayloadLimitOptionsTestCase extends TestCase
{
    public function testDefaults(): void
    {
        $options = PayloadLimitOptions::new();

        self::assertSame(512 * 1024, $options->payloadSizeWarning);
        self::assertSame(2 * 1024, $options->memoSizeWarning);
        self::assertTrue($options->isEnabled());
    }

    public function testClientOptionsHaveLimitsByDefault(): void
    {
        $options = new ClientOptions();

        self::assertInstanceOf(PayloadLimitOptions::class, $options->payloadLimits);
        self::assertSame(512 * 1024, $options->payloadLimits->payloadSizeWarning);
    }

    public function testWithersAreImmutable(): void
    {
        $options = PayloadLimitOptions::new();

        $result = $options->withPayloadSizeWarning(1024)->withMemoSizeWarning(64);

        self::assertNotSame($options, $result);
        self::assertSame(512 * 1024, $options->payloadSizeWarning);
        self::assertSame(1024, $result->payloadSizeWarning);
        self::assertSame(64, $result->memoSizeWarning);
    }

    public function testNullDisablesSingleWarning(): void
    {
        $options = PayloadLimitOptions::new()->withPayloadSizeWarning(null);

        self::assertNull($options->payloadSizeWarning);
        self::assertTrue($options->isEnabled(), 'Memo warning is still enabled.');
    }

    public function testNullDisablesAllWarnings(): void
    {
        $options = PayloadLimitOptions::new()
            ->withPayloadSizeWarning(null)
            ->withMemoSizeWarning(null);

        self::assertFalse($options->isEnabled());
    }

    public function testDisabledHasNoLimits(): void
    {
        $options = PayloadLimitOptions::disabled();

        self::assertNull($options->payloadSizeWarning);
        self::assertNull($options->memoSizeWarning);
        self::assertFalse($options->isEnabled());
    }

    public function testClientOptionsWitherDoesNotMutateTheSource(): void
    {
        $options = new ClientOptions();
        $limits = PayloadLimitOptions::new()->withPayloadSizeWarning(1024);

        $result = $options->withPayloadLimits($limits);

        self::assertNotSame($options, $result);
        self::assertSame($limits, $result->payloadLimits);
        self::assertNotSame($limits, $options->payloadLimits);
    }

    public function testWorkerOptionsCarryTheLimits(): void
    {
        $options = WorkerOptions::new();
        $limits = PayloadLimitOptions::disabled();

        $result = $options->withPayloadLimits($limits);

        self::assertNotSame($options, $result);
        self::assertSame($limits, $result->getPayloadLimits());
        self::assertSame(
            PayloadLimitOptions::DEFAULT_PAYLOAD_SIZE_WARNING,
            $options->getPayloadLimits()->payloadSizeWarning,
        );
    }

    public function testClientOptionsDisableLimits(): void
    {
        $options = (new ClientOptions())->withPayloadLimits(null);

        self::assertNull($options->payloadLimits);
    }

    /**
     * @return iterable<array-key, array{int}>
     */
    public static function nonPositiveValues(): iterable
    {
        yield [0];
        yield [-1];
    }

    #[DataProvider('nonPositiveValues')]
    public function testPayloadWarningRejectsNonPositive(int $value): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('`payloadSizeWarning` must be a positive number of bytes');

        PayloadLimitOptions::new()->withPayloadSizeWarning($value);
    }

    #[DataProvider('nonPositiveValues')]
    public function testMemoWarningRejectsNonPositive(int $value): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('`memoSizeWarning` must be a positive number of bytes');

        PayloadLimitOptions::new()->withMemoSizeWarning($value);
    }
}
