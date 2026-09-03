<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Events;

/**
 * @mixin EventEmitterInterface
 * @mixin EventListenerInterface
 *
 * @template T of string
 *
 * @template-implements EventEmitterInterface<T>
 * @template-implements EventListenerInterface<T>
 */
trait EventEmitterTrait
{
    /**
     * @var array<T, \SplQueue<callable>>
     */
    protected array $once = [];

    public function once(string $event, callable $then): static
    {
        ($this->once[$event] ??= new \SplQueue())->enqueue($then);

        return $this;
    }

    public function emit(string $event, array $arguments = []): void
    {
        $queue = $this->once[$event] ?? null;

        if ($queue === null) {
            return;
        }

        // Callbacks registered while emitting, including by a nested emit of the same event,
        // share this queue and run in registration order.
        while (!$queue->isEmpty()) {
            $callback = $queue->dequeue();
            $callback(...$arguments);
        }

        if (($this->once[$event] ?? null) === $queue) {
            unset($this->once[$event]);
        }
    }
}
