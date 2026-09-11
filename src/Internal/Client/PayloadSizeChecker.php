<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Internal\Client;

use Google\Protobuf\Internal\DescriptorPool;
use Google\Protobuf\Internal\FieldDescriptor;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\MapField;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Psr\Log\LoggerInterface;
use Temporal\Common\PayloadLimitOptions;

/**
 * Warns when an outgoing gRPC request carries payloads larger than the configured limits.
 *
 * Sizes are measured the same way the server measures them: a `Payloads` or `Memo` message is
 * measured as a whole, while `map<string, Payload>` fields are measured as the sum of key lengths
 * and payload data lengths.
 *
 * @internal
 */
final class PayloadSizeChecker
{
    /**
     * Message code used by all the SDKs for the payload size warning.
     */
    private const MESSAGE_CODE = 'TMPRL1103';

    private const TYPE_PAYLOADS = 'temporal.api.common.v1.Payloads';
    private const TYPE_MEMO = 'temporal.api.common.v1.Memo';
    private const TYPE_SEARCH_ATTRIBUTES = 'temporal.api.common.v1.SearchAttributes';

    /**
     * Protects from cycles in the message graph, e.g. {@see \Temporal\Api\Failure\V1\Failure}.
     */
    private const MAX_DEPTH = 10;

    public function __construct(
        private readonly PayloadLimitOptions $limits,
        private readonly LoggerInterface $logger,
    ) {}

    /**
     * @param non-empty-string $method RPC method name.
     */
    public function check(string $method, object $request): void
    {
        if (!$request instanceof Message || !$this->limits->isEnabled()) {
            return;
        }

        $this->inspect($method, $request, 0);
    }

    private static function typeNameOf(Message $message): ?string
    {
        return DescriptorPool::getGeneratedPool()
            ->getDescriptorByClassName($message::class)
            ?->getFullName();
    }

    private function inspect(string $method, Message $message, int $depth): void
    {
        if ($depth > self::MAX_DEPTH) {
            return;
        }

        $descriptor = DescriptorPool::getGeneratedPool()->getDescriptorByClassName($message::class);
        if ($descriptor === null) {
            return;
        }

        /** @var FieldDescriptor $field */
        foreach ($descriptor->getField() as $field) {
            if ($field->getType() !== GPBType::MESSAGE) {
                continue;
            }

            $value = $message->{$field->getGetter()}();
            if ($value === null) {
                continue;
            }

            if ($field->isMap()) {
                \assert($value instanceof MapField);
                foreach ($value as $item) {
                    $item instanceof Message and $this->inspectValue($method, $item, $depth);
                }
                continue;
            }

            if ($value instanceof RepeatedField) {
                foreach ($value as $item) {
                    $item instanceof Message and $this->inspectValue($method, $item, $depth);
                }
                continue;
            }

            \assert($value instanceof Message);
            $this->inspectValue($method, $value, $depth);
        }
    }

    private function inspectValue(string $method, Message $value, int $depth): void
    {
        switch (self::typeNameOf($value)) {
            case self::TYPE_PAYLOADS:
                $this->warnOnPayloadSize($method, $value->byteSize());
                return;

            case self::TYPE_MEMO:
                $this->warnOnMemoSize($method, $value->byteSize());
                return;

            case self::TYPE_SEARCH_ATTRIBUTES:
                // Server measures search attributes as the sum of key and payload data lengths
                $size = 0;
                /** @psalm-suppress UndefinedMethod */
                foreach ($value->getIndexedFields() as $key => $payload) {
                    $size += \strlen((string) $key) + \strlen($payload->getData());
                }

                $this->warnOnPayloadSize($method, $size);
                return;

            default:
                $this->inspect($method, $value, $depth + 1);
        }
    }

    private function warnOnPayloadSize(string $method, int $size): void
    {
        $limit = $this->limits->payloadSizeWarning;
        if ($limit === null || $size <= $limit) {
            return;
        }

        $this->logger->warning(
            \sprintf(
                '[%s] Attempted to send payloads with size that exceeded the warning limit.',
                self::MESSAGE_CODE,
            ),
            ['method' => $method, 'size' => $size, 'limit' => $limit],
        );
    }

    private function warnOnMemoSize(string $method, int $size): void
    {
        $limit = $this->limits->memoSizeWarning;
        if ($limit === null || $size <= $limit) {
            return;
        }

        $this->logger->warning(
            \sprintf(
                '[%s] Attempted to send a memo with size that exceeded the warning limit.',
                self::MESSAGE_CODE,
            ),
            ['method' => $method, 'size' => $size, 'limit' => $limit],
        );
    }
}
