<?php

declare(strict_types=1);

namespace Temporal\Tests\Functional;

use PHPUnit\Framework\Attributes\DataProvider;
use Temporal\Testing\Replay\WorkflowReplayer;
use Temporal\Tests\TestCase;

/**
 * Histories in tests/Fixtures/history/generator were produced by the generator-based SDK
 * (upstream master before the Fiber runtime). Every one of them must replay on the Fiber
 * runtime without a non-determinism error: the command sequence emitted by a workflow must
 * not depend on how the workflow code is suspended.
 *
 * The recording scenario for each file is documented in
 * tests/Fixtures/history/generator/README.md.
 */
final class GeneratorHistoryReplayTestCase extends TestCase
{
    public static function histories(): iterable
    {
        $files = \glob(\dirname(__DIR__) . '/Fixtures/history/generator/*.json');
        \sort($files);

        foreach ($files as $file) {
            $type = \basename($file, '.json');
            yield $type => [$type, $file];
        }
    }

    #[DataProvider('histories')]
    public function testHistoryRecordedByTheGeneratorRuntimeReplays(string $type, string $file): void
    {
        (new WorkflowReplayer())->replayFromJSON($type, $file);

        $this->assertTrue(true);
    }
}
