<?php

declare(strict_types=1);

namespace Temporal\Tests\Unit\Internal\Events;

use PHPUnit\Framework\TestCase;
use Temporal\Internal\Events\EventEmitterTrait;

final class EventEmitterTraitTestCase extends TestCase
{
    public function testCallbacksRunInRegistrationOrderAndAreRunOnce(): void
    {
        $emitter = new class {
            use EventEmitterTrait;
        };
        $log = [];

        $emitter->once('tick', static function () use (&$log): void {
            $log[] = 'a';
        });
        $emitter->once('tick', static function () use (&$log): void {
            $log[] = 'b';
        });

        $emitter->emit('tick');
        $emitter->emit('tick');

        self::assertSame(['a', 'b'], $log);
    }

    public function testCallbackRegisteredDuringEmitRunsInTheSameEmit(): void
    {
        $emitter = new class {
            use EventEmitterTrait;
        };
        $log = [];

        $emitter->once('tick', static function () use (&$log, $emitter): void {
            $log[] = 'a';
            $emitter->once('tick', static function () use (&$log): void {
                $log[] = 'registered during a';
            });
        });

        $emitter->emit('tick');

        self::assertSame(['a', 'registered during a'], $log);
    }

    public function testNestedEmitDrainsTheSharedQueueAndLaterRegistrationsStillRun(): void
    {
        $emitter = new class {
            use EventEmitterTrait;
        };
        $log = [];

        $emitter->once('tick', static function () use (&$log, $emitter): void {
            $log[] = 'a';
            // A nested emit of the same event runs the callbacks queued so far.
            $emitter->emit('tick');
            $log[] = 'a after nested';
            // Registered after the nested emit dropped the queue: must run in the outer emit.
            $emitter->once('tick', static function () use (&$log): void {
                $log[] = 'late';
            });
        });
        $emitter->once('tick', static function () use (&$log): void {
            $log[] = 'b';
        });

        $emitter->emit('tick');

        self::assertSame(['a', 'b', 'a after nested', 'late'], $log);

        $emitter->emit('tick');
        self::assertSame(['a', 'b', 'a after nested', 'late'], $log);
    }
}
