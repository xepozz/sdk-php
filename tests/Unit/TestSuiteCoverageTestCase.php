<?php

declare(strict_types=1);

namespace Temporal\Tests\Unit;

use PHPUnit\Framework\TestCase;

final class TestSuiteCoverageTestCase extends TestCase
{
    private const TEST_DIRECTORIES = ['Acceptance', 'Arch', 'Functional', 'Unit'];

    public function testEveryTestFileIsMatchedByATestSuite(): void
    {
        $root = \dirname(__DIR__, 2);
        $configuration = new \SimpleXMLElement((string) \file_get_contents($root . '/phpunit.xml.dist'));

        $matched = [];

        foreach ($configuration->testsuites->testsuite as $suite) {
            foreach ($suite->directory as $directory) {
                $suffix = (string) ($directory['suffix'] ?? 'Test.php');

                foreach ($this->testFiles($root . '/' . (string) $directory) as $file) {
                    \str_ends_with($file, $suffix) and $matched[$file] = true;
                }
            }

            foreach ($suite->file as $file) {
                $matched[$root . '/' . (string) $file] = true;
            }

            foreach ($suite->exclude as $exclude) {
                $path = $root . '/' . (string) $exclude;

                foreach (\array_keys($matched) as $file) {
                    if ($file === $path || \str_starts_with($file, $path . '/')) {
                        unset($matched[$file]);
                    }
                }
            }
        }

        // tests/Fixtures holds workflow and generated protobuf classes, not test cases.
        $candidates = \array_merge(
            ...\array_map(
                fn(string $directory): array => $this->testFiles($root . '/tests/' . $directory),
                self::TEST_DIRECTORIES,
            ),
        );

        $orphans = \array_values(\array_filter(
            $candidates,
            static fn(string $file): bool => !isset($matched[$file]),
        ));

        self::assertSame(
            [],
            $orphans,
            \sprintf("These test files are not part of any test suite:\n%s", \implode("\n", $orphans)),
        );
    }

    /**
     * @return list<string>
     */
    private function testFiles(string $directory): array
    {
        if (!\is_dir($directory)) {
            return [];
        }

        $files = [];
        $iterator = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($directory));

        /** @var \SplFileInfo $file */
        foreach ($iterator as $file) {
            $name = $file->getFilename();

            if (\str_ends_with($name, 'Test.php') || \str_ends_with($name, 'TestCase.php')) {
                \preg_match('/^abstract\s+class/m', (string) \file_get_contents($file->getPathname()))
                    or $files[] = \str_replace('\\', '/', $file->getPathname());
            }
        }

        \sort($files);

        return $files;
    }
}
