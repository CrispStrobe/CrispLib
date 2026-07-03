"""Cross-language golden-file parity harness (CrispZotLib PLAN 7.1).

fixtures/parity/ (records.json + expected.bib + expected.ris) is synced
byte-for-byte from CrispZotLib (the canonical copy lives in its
test/fixtures/parity/; scripts/sync-endpoints.sh copies it here). CrispZotLib
asserts the same goldens from TypeScript in test/formatterParity.test.ts.

A failure here means the Python formatters diverged from the agreed output.
Fix the formatter — or, for an intentional output change, regenerate the
goldens in CrispZotLib (UPDATE_GOLDENS=1 npx vitest run
test/formatterParity.test.ts), sync, and change both formatters in lockstep.
"""
import json
from pathlib import Path

import pytest

from sru_library import BiblioRecord, bibtex_from_record
from library_search import format_record_ris

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "parity"


def _load_records():
    raw = json.loads((FIXTURE_DIR / "records.json").read_text(encoding="utf-8"))
    return [BiblioRecord(**r) for r in raw]


RECORDS = _load_records()
BIBTEX_ENTRIES = (
    (FIXTURE_DIR / "expected.bib").read_text(encoding="utf-8")[:-1].split("\n\n")
)
RIS_ENTRIES = (
    (FIXTURE_DIR / "expected.ris").read_text(encoding="utf-8")[:-1].split("\n\n")
)


def test_goldens_cover_every_fixture_record():
    assert len(BIBTEX_ENTRIES) == len(RECORDS)
    assert len(RIS_ENTRIES) == len(RECORDS)


@pytest.mark.parametrize(
    "record,expected", zip(RECORDS, BIBTEX_ENTRIES), ids=[r.id for r in RECORDS]
)
def test_bibtex_matches_golden(record, expected):
    assert bibtex_from_record(record) == expected


@pytest.mark.parametrize(
    "record,expected", zip(RECORDS, RIS_ENTRIES), ids=[r.id for r in RECORDS]
)
def test_ris_matches_golden(record, expected):
    assert format_record_ris(record) == expected
