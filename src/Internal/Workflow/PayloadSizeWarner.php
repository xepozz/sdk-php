<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Workflow;

use Psr\Log\LoggerInterface;
use Temporal\Api\Common\V1\Memo;
use Temporal\Common\PayloadLimitOptions;
use Temporal\DataConverter\DataConverterInterface;
use Temporal\DataConverter\ValuesInterface;
use Temporal\Internal\Transport\Request\ExecuteLocalActivity;
use Temporal\Internal\Transport\Request\UpsertMemo;
use Temporal\Internal\Transport\Request\UpsertSearchAttributes;
use Temporal\Worker\Environment\EnvironmentInterface;
use Temporal\Worker\Transport\Command\RequestInterface;

/**
 * Warns when a command produced by a Workflow carries payloads larger than the configured limit.
 *
 * The payloads are measured the same way the server measures them, so the check costs one extra
 * conversion per command that carries payloads. Replayed commands are skipped entirely: they are
 * not sent to the server, so there is nothing to warn about.
 *
 * @internal
 */
final class PayloadSizeWarner
{
    /**
     * Message code used by all the SDKs for the payload size warning.
     */
    private const MESSAGE_CODE = 'TMPRL1103';

    public function __construct(
        private readonly PayloadLimitOptions $limits,
        private readonly DataConverterInterface $converter,
        private readonly EnvironmentInterface $env,
        private readonly LoggerInterface $logger,
    ) {}

    /**
     * Measure a command that is about to be sent to the server.
     */
    public function check(RequestInterface $request): void
    {
        $name = $request->getName();

        // Local Activity arguments are not sent to the server
        if ($name === ExecuteLocalActivity::NAME) {
            return;
        }

        // A replayed command is not sent anywhere, so it is never measured nor reported,
        // the same way the other SDKs check the payloads only when the request is sent.
        if ($this->env->isReplaying()) {
            return;
        }

        try {
            $options = $request->getOptions();

            // Memo and Search Attribute upserts are maps measured key by key against the payload
            // limit, the way the server measures them
            match ($name) {
                UpsertMemo::NAME => $this->warn(
                    $name,
                    'payloads',
                    $this->mapSize($options['memo'] ?? null),
                    $this->limits->payloadSizeWarning,
                ),
                UpsertSearchAttributes::NAME => $this->warn(
                    $name,
                    'payloads',
                    $this->mapSize($options['searchAttributes'] ?? null),
                    $this->limits->payloadSizeWarning,
                ),
                default => null,
            };

            // A Child Workflow carries a Memo of its own, measured against the memo limit
            $this->memo($name, $options['options']['Memo'] ?? null);

            $this->payloads($name, $request->getPayloads());
        } catch (\Throwable) {
            // Measuring is an observability feature: it must not affect the Workflow in any way.
            // A value that cannot be converted fails later, in the codec, as it did before.
        }
    }

    /**
     * Measure the result of a Query or an Update handler that is about to be sent to the server.
     *
     * @param non-empty-string $command
     */
    public function checkValues(string $command, ValuesInterface $values): void
    {
        if ($this->env->isReplaying()) {
            return;
        }

        try {
            $this->payloads($command, $values);
        } catch (\Throwable) {
            // See the comment in `check()`
        }
    }

    /**
     * @return array<array-key, mixed>
     */
    private static function fieldsOf(mixed $fields): array
    {
        return match (true) {
            $fields instanceof \stdClass => (array) $fields,
            \is_array($fields) => $fields,
            default => [],
        };
    }

    private function payloads(string $command, ValuesInterface $values): void
    {
        $limit = $this->limits->payloadSizeWarning;
        if ($limit === null || $values->count() === 0) {
            return;
        }

        $values->setDataConverter($this->converter);
        // `byteSize()` is not available when the protobuf extension is used
        $this->warn($command, 'payloads', \strlen($values->toPayloads()->serializeToString()), $limit);
    }

    /**
     * @param mixed $fields Raw values of a Memo, not converted yet.
     */
    private function memo(string $command, mixed $fields): void
    {
        $limit = $this->limits->memoSizeWarning;
        $fields = self::fieldsOf($fields);
        if ($limit === null || $fields === []) {
            return;
        }

        $payloads = [];
        foreach ($fields as $key => $value) {
            $payloads[(string) $key] = $this->converter->toPayload($value);
        }

        $memo = (new Memo())->setFields($payloads);

        $this->warn($command, 'memo', \strlen($memo->serializeToString()), $limit);
    }

    /**
     * Size of a map of payloads: the server sums the key lengths with the sizes of the payload
     * data, so the encoding overhead of the map itself is not counted.
     *
     * @param mixed $fields Raw values of the map, not converted yet.
     */
    private function mapSize(mixed $fields): int
    {
        $size = 0;
        foreach (self::fieldsOf($fields) as $key => $value) {
            $size += \strlen((string) $key) + \strlen($this->converter->toPayload($value)->getData());
        }

        return $size;
    }

    /**
     * @param non-empty-string $kind
     */
    private function warn(string $command, string $kind, int $size, ?int $limit): void
    {
        if ($limit === null || $size <= $limit) {
            return;
        }

        $this->logger->warning(
            \sprintf(
                '[%s] Attempted to upload %s with size that exceeded the warning limit.',
                self::MESSAGE_CODE,
                $kind,
            ),
            ['command' => $command, 'size' => $size, 'limit' => $limit],
        );
    }
}
