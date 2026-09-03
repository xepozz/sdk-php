<?php

/**
 * This file is part of Temporal package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Temporal\Tests\Unit\DataConverter;

use Temporal\Api\Common\V1\Payload;
use Temporal\DataConverter\DataConverter;
use Temporal\DataConverter\DataConverterInterface;
use Temporal\DataConverter\EncodingKeys;
use Temporal\DataConverter\RawValue;
use Temporal\Tests\Unit\AbstractUnit;

/**
 * @group unit
 * @group data-converter
 */
class RawValueConverterTest extends AbstractUnit
{
    public function testRawPayloadEncoding(): void
    {
        $innerPayload = new Payload([
            'metadata' => [EncodingKeys::METADATA_ENCODING_KEY => EncodingKeys::METADATA_ENCODING_RAW_VALUE],
            'data' => '1',
        ]);
        $message = new RawValue($innerPayload);

        $payload = DataConverter::createDefault()->toPayload($message);

        // A raw value is passed through untouched, metadata included.
        self::assertSame($innerPayload, $payload);
        self::assertSame(EncodingKeys::METADATA_ENCODING_RAW_VALUE, $payload->getMetadata()[EncodingKeys::METADATA_ENCODING_KEY]);
    }

    public function testRawPayloadDecoding(): void
    {
        $innerPayload = new Payload(['data' => '1']);
        $message = new RawValue($innerPayload);

        $encoded = DataConverter::createDefault()->toPayload($message);
        $decoded = DataConverter::createDefault()->fromPayload($encoded, RawValue::class);

        self::assertInstanceOf(RawValue::class, $decoded);
        self::assertSame($decoded->getPayload(), $encoded);
    }

    protected function create(): DataConverterInterface
    {
        return DataConverter::createDefault();
    }
}
