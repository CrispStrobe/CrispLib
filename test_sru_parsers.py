"""Contract tests for our SRU parsers (lib/sru_client.py).

These lock in the behaviour we depend on — document typing and field capture —
using small inline MARCXML/Dublin-Core fixtures so they run fully offline and
deterministically (no network, no record/replay harness). They protect the
SRU/IxTheo additions that the upstream suite does not cover.
"""
import xml.etree.ElementTree as ET

from sru_library import parse_marcxml, parse_dublin_core, SRUClient, clean_person_name

NS = SRUClient(base_url='x').namespaces
MARC = 'http://www.loc.gov/MARC21/slim'


def _marc(leader, fields):
    """Build a MARCXML <record> element. `fields` is a list of
    (tag, ind1, ind2, [(code, value), ...]) tuples; controlfield-less."""
    ET.register_namespace('', MARC)
    rec = ET.Element(f'{{{MARC}}}record')
    ld = ET.SubElement(rec, f'{{{MARC}}}leader')
    ld.text = leader
    for tag, ind1, ind2, subs in fields:
        df = ET.SubElement(rec, f'{{{MARC}}}datafield', tag=tag, ind1=ind1, ind2=ind2)
        for code, value in subs:
            sf = ET.SubElement(df, f'{{{MARC}}}subfield', code=code)
            sf.text = value
    return rec


def _parse_marc(rec):
    return parse_marcxml({'data': rec, 'id': 't', 'schema': 'marcxml'}, NS)


# ── Document typing ──────────────────────────────────────────────────────────

def test_journal_article_serial_host():
    """Analytic record with a serial 773 host -> Journal Article, with German
    volume/issue/pages and the host ISSN captured."""
    rec = _marc('00000naa a2200000c 4500', [
        ('245', '1', '0', [('a', 'Quia ignoro, adoro')]),
        ('773', '0', '8', [('t', 'Heiliger Dienst'), ('g', '78(2024), 3, Seite 205-213'),
                           ('7', 'nnas'), ('x', '0017-9620')]),
    ])
    r = _parse_marc(rec)
    assert r.document_type == 'Journal Article'
    assert r.journal_title == 'Heiliger Dienst'
    assert r.volume == '78'
    assert r.issue == '3'
    assert r.pages == '205-213'
    assert r.issn == '0017-9620'


def test_book_chapter_monograph_host():
    """Analytic record with a monograph 773 host -> Book Chapter."""
    rec = _marc('00000naa a2200000c 4500', [
        ('245', '1', '0', [('a', 'Ein Aufsatz')]),
        ('773', '0', '8', [('t', 'Sammelband zur Theologie'), ('g', 'Seite 55-70'), ('7', 'nnam')]),
    ])
    r = _parse_marc(rec)
    assert r.document_type == 'Book Chapter'
    assert r.series == 'Sammelband zur Theologie'


def test_monograph_in_series_is_book():
    """Leader bib-level 'm' (monograph) with a 490 series -> Book, not Book Chapter."""
    rec = _marc('00000nam a2200000c 4500', [
        ('245', '1', '0', [('a', 'Systematische Theologie')]),
        ('490', '0', ' ', [('a', 'Handbuch der Theologie')]),
        ('020', ' ', ' ', [('a', '9783161500000')]),
    ])
    r = _parse_marc(rec)
    assert r.document_type == 'Book'


# ── Field capture ────────────────────────────────────────────────────────────

def test_captures_abstract_ddc_and_corporate_author():
    rec = _marc('00000nam a2200000c 4500', [
        ('245', '1', '0', [('a', 'Klimabericht')]),
        ('520', ' ', ' ', [('a', 'Eine Zusammenfassung des Berichts.')]),
        ('082', '0', '4', [('a', '551.6')]),
        ('110', '2', ' ', [('a', 'Deutsche Nationalbibliothek')]),
    ])
    r = _parse_marc(rec)
    assert r.abstract == 'Eine Zusammenfassung des Berichts.'
    assert 'DDC:551.6' in r.subjects
    assert 'Deutsche Nationalbibliothek' in r.authors


# ── Dublin Core ──────────────────────────────────────────────────────────────

def test_dc_cleans_bnf_names_and_infers_type():
    dc = ET.fromstring(
        '<srw_dc:dc xmlns:srw_dc="info:srw/schema/1/dc-v1.1" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/">'
        '<dc:title>Zeit der Übergänge</dc:title>'
        '<dc:creator>Habermas, Jürgen (1929-2026). Auteur du texte</dc:creator>'
        '<dc:identifier>ISBN 3518122622</dc:identifier>'
        '</srw_dc:dc>'
    )
    r = parse_dublin_core({'data': dc, 'id': 't', 'schema': 'dublincore'}, NS)
    assert 'Habermas, Jürgen' in r.authors
    assert 'Auteur du texte' not in ' '.join(r.authors)
    assert r.document_type == 'Book'


def test_clean_person_name_unit():
    assert clean_person_name('Habermas, Jürgen (1929-2026). Auteur du texte') == 'Habermas, Jürgen'
    assert clean_person_name('Guido van Rossum') == 'Guido van Rossum'


# ── Generic parser: identifiers embedded in dc:identifier text (PLAN 3.4) ─────

DC = 'http://purl.org/dc/elements/1.1/'


def _dc(fields):
    """Build a Dublin Core record container. `fields` is (localname, text)."""
    ET.register_namespace('dc', DC)
    rec = ET.Element('record')
    for name, text in fields:
        el = ET.SubElement(rec, f'{{{DC}}}{name}')
        el.text = text
    return rec


def test_generic_parse_pulls_isbn_issn_url_from_dc_identifier():
    """ElementTree has no contains(text()) XPath, so the generic parser must
    iterate dc:identifier elements to recover ISBN/ISSN/URL embedded in text."""
    rec = _dc([
        ('title', 'A Generic Record'),
        ('identifier', 'ISBN 978-3-16-148410-0'),
        ('identifier', 'ISSN 0378-5955'),
        ('identifier', 'http://example.org/record/1'),
    ])
    r = SRUClient(base_url='x')._generic_parse({'data': rec, 'id': 't', 'raw_xml': '<record/>'}, NS)
    assert r is not None
    assert r.title == 'A Generic Record'
    assert r.isbn and r.isbn.replace('-', '') == '9783161484100'
    assert r.issn == '0378-5955'
    assert 'http://example.org/record/1' in r.urls
