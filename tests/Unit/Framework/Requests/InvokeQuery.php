<?php

declare(strict_types=1);

namespace Temporal\Tests\Unit\Framework\Requests;

use Temporal\DataConverter\EncodedValues;
use Temporal\Worker\Transport\Command\Server\ServerRequest;
use Temporal\Worker\Transport\Command\Server\TickInfo;

/**
 * @internal
 */
final class InvokeQuery extends ServerRequest
{
    public function __construct(string $runId, string $name, ...$args)
    {
        parent::__construct(
            'InvokeQuery',
            new TickInfo(new \DateTimeImmutable()),
            [
                'runId' => $runId,
                'name' => $name,
            ],
            EncodedValues::fromValues($args),
        );
    }
}
