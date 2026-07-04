"""Cross-repo SRU parser parity guard (CrispZotLib PLAN 7.4 / audit #2a).

fixtures/parity/parser-records.json is synced byte-for-byte from CrispZotLib
(the canonical copy lives in its test/fixtures/parity/; scripts/sync-endpoints.sh
copies it here and to citer). Each case is raw MARCXML/Dublin-Core plus the
agreed parsed-field output. citer asserts the SAME golden in
tests/parser_parity_test.py, and CrispZotLib checks the DC cases from TypeScript
in test/parserParity.test.ts.

A failure here means this repo's SRU parsers drifted from the agreed field
output. This guard exists because parser SOURCE was never compared cross-repo:
the sync-check only diffs endpoints.json + the formatter goldens, which is how
the 'Verfasser' (author) and 'Herausgeber' (editor) relator bugs slipped into
citer / all three repos respectively.
"""
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from sru_library import parse_marcxml, parse_dublin_core, SRUClient

CASES = json.loads(
    (Path(__file__).parent / "fixtures" / "parity" / "parser-records.json").read_text(
        encoding="utf-8"
    )
)
NS = SRUClient(base_url="x").namespaces


def _parse(case):
    raw = {"data": ET.fromstring(case["xml"]), "id": case["name"], "schema": case["schema"]}
    if case["schema"] == "marcxml":
        return parse_marcxml(raw, NS)
    return parse_dublin_core(raw, NS)


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_parser_matches_golden(case):
    rec = _parse(case)
    for field, expected in case["expected"].items():
        assert getattr(rec, field) == expected, f"{case['name']}.{field}"
