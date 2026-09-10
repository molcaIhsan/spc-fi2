"""
Raw Kafka message parser -- standalone, no dependency on ajinomoto-etl-flink.

Message format (confirmed against a real sample and the source parser in
ajinomoto-etl-flink/src/fi2/sources/parser.py):

    @#HWCODE#ACTION#QTY#QUALITY#TIMESTAMP#SKU_CODE#REASON#!

Real sample parsed during development: '@#YAMATO-12#A#3667#0#1789012526982#4##!'
-> hwcode=YAMATO-12, action_type=A, value=3667, quality=0,
   timestamp=1789012526982 (13-digit epoch ms), sku_code=4, reason=''
(that sample was action_type='A' -- Finish Good count, not 'W' -- Total
Weight, which is what this job filters for; format is confirmed, not the
specific value ranges for a 'W' message on these 3 machines).

NOTE: unlike ajinomoto-etl-flink's parser, this does NOT resolve sku_code to
a friendly SKU name (that used a large hwcode+sku_code -> name mapping table,
SkuMappings, specific to that repo's product catalog). This keeps sku_code
as-is (a short numeric string). If a friendly name is needed later, that
mapping has to be sourced from ajinomoto-etl-flink or wherever it's
authoritative and ported deliberately, not guessed.
"""

import logging
from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedMessage:
    hwcode: str
    action_type: str
    value: float
    quality: float
    timestamp: float   # epoch ms
    sku_code: str
    reason: Optional[str]


def parse_message(raw: str) -> Optional[ParsedMessage]:
    try:
        fields = raw.strip("!").split("#")[1:]  # skip the leading empty field from "@#"

        timestamp = float(fields[4])
        if timestamp <= 0:
            logging.error("Invalid timestamp in message: %r", raw)
            return None

        return ParsedMessage(
            hwcode=fields[0],
            action_type=fields[1],
            value=float(fields[2]),
            quality=float(fields[3]),
            timestamp=timestamp,
            sku_code=fields[5],
            reason=fields[6] if len(fields) > 6 and fields[6] else None,
        )
    except (ValueError, IndexError) as e:
        logging.error("Failed to parse message: %s, raw=%r", e, raw)
        return None
