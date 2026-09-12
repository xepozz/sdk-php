<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\Worker\Stub;

use Temporal\Workflow;

/**
 * @internal
 */
#[Workflow\WorkflowInterface]
final class BigResultWorkflow
{
    #[Workflow\WorkflowMethod(name: 'BigResultWorkflow')]
    public function handler(): string
    {
        return \str_repeat('x', 2000);
    }
}
