#!/usr/bin/env python3
# sru_library.py
"""
SRU Library - A flexible SRU (Search/Retrieve via URL) client for bibliographic data

This module provides a modular approach to query various library SRU endpoints
without requiring hardcoded classes for each specific library.
"""

import requests
# Prefer defusedxml to harden against XXE / billion-laughs / quadratic-blowup
# attacks when parsing untrusted SRU/OAI responses. Fall back to stdlib
# ElementTree only if defusedxml is not installed.
# defusedxml hardens the parsing functions; element types still come from stdlib.
import xml.etree.ElementTree as _stdlib_ET  # nosec B405 
try:
    import defusedxml.ElementTree as ET  # type: ignore[import-not-found]
    # defusedxml only wraps parsers; re-export Element from stdlib for type hints.
    ET.Element = _stdlib_ET.Element  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    ET = _stdlib_ET  # nosec B405 
import urllib.parse
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple, Callable
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("sru_library")

# BiblioRecord, the name/type helpers, and the SRU parsers live in the shared
# sru_shared module (PLAN 8.5) so CrispLib and citer share ONE implementation
# and can no longer drift — the parser-parity golden guards the output.
from sru_shared import (  # noqa: E402
    BiblioRecord,
    clean_person_name,
    map_dc_type,
    infer_document_type,
    parse_dublin_core,
    parse_marcxml,
    parse_rdfxml,
)


class SRUClient:
    """
    A flexible SRU (Search/Retrieve via URL) client that can work with any SRU endpoint.
    """
    
    # Registry of record format parsers
    parsers: Dict[str, Callable] = {}
    
    @classmethod
    def register_parser(cls, schema_name):
        """Decorator to register a parser function for a specific schema."""
        def decorator(parser_func):
            cls.parsers[schema_name] = parser_func
            return parser_func
        return decorator
    
    def __init__(self,
                base_url: str,
                default_schema: Optional[str] = None,
                version: str = "1.1",
                namespaces: Optional[Dict[str, str]] = None,
                timeout: int = 30,
                record_parser: Optional[Callable] = None,
                query_params: Optional[Dict[str, str]] = None):
        """
        Initialize SRU client.
        """
        self.base_url = base_url
        self.version = version
        self.timeout = timeout
        self.default_schema = default_schema
        self.custom_parser = record_parser
        self.query_params = query_params or {}
        
        # Comprehensive set of namespaces for different record formats
        self.namespaces = {
            # SRU namespaces
            'srw': 'http://www.loc.gov/zing/srw/',
            'sd': 'http://www.loc.gov/zing/srw/diagnostic/',  # Added diagnostic namespace
            
            # Dublin Core
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcterms': 'http://purl.org/dc/terms/',
            
            # MARC
            'marc': 'http://www.loc.gov/MARC21/slim',
            'mxc': 'info:lc/xmlns/marcxchange-v2',
            
            # XML Schema
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
            
            # RDF and related vocabularies
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            'owl': 'http://www.w3.org/2002/07/owl#',
            'skos': 'http://www.w3.org/2004/02/skos/core#',
            'foaf': 'http://xmlns.com/foaf/0.1/',
            'bibo': 'http://purl.org/ontology/bibo/',
            'schema': 'http://schema.org/',
            
            # Library specific
            'gndo': 'https://d-nb.info/standards/elementset/gnd#',
            'marcRole': 'http://id.loc.gov/vocabulary/relators/',
            'rdau': 'http://rdaregistry.info/Elements/u/',
            'isbd': 'http://iflastandards.info/ns/isbd/elements/',
            'umbel': 'http://umbel.org/umbel#',
            'gbv': 'http://purl.org/ontology/gbv/',
            
            # Thesauri and classifications
            'editeur': 'https://ns.editeur.org/thema/',
            'thesoz': 'http://lod.gesis.org/thesoz/',
            'agrovoc': 'https://aims.fao.org/aos/agrovoc/',
            'lcsh': 'https://id.loc.gov/authorities/subjects/',
            'mesh': 'http://id.nlm.nih.gov/mesh/vocab#',
            
            # Library institutions
            'dnbt': 'https://d-nb.info/standards/elementset/dnb#',
            'nsogg': 'https://purl.org/bncf/tid/',
            'ram': 'https://data.bnf.fr/ark:/12148/',
            'naf': 'https://id.loc.gov/authorities/names/',
            'embne': 'https://datos.bne.es/resource/',
            
            # Misc
            'geo': 'http://www.opengis.net/ont/geosparql#',
            'sf': 'http://www.opengis.net/ont/sf#',
            'bflc': 'http://id.loc.gov/ontologies/bflc/',
            'agrelon': 'https://d-nb.info/standards/elementset/agrelon#',
            'dcmitype': 'http://purl.org/dc/dcmitype/',
            'dbp': 'http://dbpedia.org/property/',
            'dnb_intern': 'http://dnb.de/',
            'madsrdf': 'http://www.loc.gov/mads/rdf/v1#',
            'v': 'http://www.w3.org/2006/vcard/ns#',
            'cidoc': 'http://www.cidoc-crm.org/cidoc-crm/',
            'dcatde': 'http://dcat-ap.de/def/dcatde/',
            'ebu': 'http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#',
            'wdrs': 'http://www.w3.org/2007/05/powder-s#',
            'lib': 'http://purl.org/library/',
            'mo': 'http://purl.org/ontology/mo/'
        }
        
        # Update with provided namespaces
        if namespaces:
            self.namespaces.update(namespaces)
    
    def build_query_url(self, query: str,
                        schema: Optional[str] = None,
                        max_records: int = 10,
                        start_record: int = 1) -> str:
        """
        Build SRU query URL.
        
        Args:
            query: CQL query
            schema: Record schema
            max_records: Maximum number of records to return
            start_record: Start record position
            
        Returns:
            Complete SRU query URL
        """
        schema = schema or self.default_schema
        
        # Base parameters
        params = {
            'version': self.version,
            'operation': 'searchRetrieve',
            'query': query,
            'maximumRecords': str(max_records),
            'startRecord': str(start_record)
        }
        
        # Add schema if specified
        if schema:
            params['recordSchema'] = schema
        
        # Add additional query parameters
        params.update(self.query_params)
        
        # Construct URL
        param_string = '&'.join([f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items()])
        
        if '?' in self.base_url:
            return f"{self.base_url}&{param_string}"
        else:
            return f"{self.base_url}?{param_string}"
    
    def execute_query(self, query: str,
                    schema: Optional[str] = None,
                    max_records: int = 10,
                    start_record: int = 1) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Execute SRU query and return raw results.
        
        Args:
            query: CQL query
            schema: Record schema
            max_records: Maximum number of records to return
            start_record: Start record position
            
        Returns:
            Tuple of (total_records, list of raw record data)
        """
        url = self.build_query_url(query, schema, max_records, start_record)
        logger.debug(f"Querying: {url}")
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.content)  # nosec B314 
            
            # Check for diagnostics (errors)
            namespaces = {
                'srw': 'http://www.loc.gov/zing/srw/',
                'sd': 'http://www.loc.gov/zing/srw/diagnostic/'
            }
            
            # First check BNF-specific diagnostics
            bnf_diagnostics = root.findall('.//sd:diagnostic', namespaces)
            if bnf_diagnostics:
                for diag in bnf_diagnostics:
                    message_elem = diag.find('./sd:message', namespaces)
                    details_elem = diag.find('./sd:details', namespaces)


                    # Log details if available
                    if message_elem is not None and message_elem.text:
                        logger.warning(f"SRU Diagnostic: {message_elem.text}")
                    if details_elem is not None and details_elem.text:
                        logger.warning(f"Details: {details_elem.text}")
                        # For BNF schema issues
                        if "Schéma inconnu" in details_elem.text:
                            logger.warning("The server does not support the requested schema. Try 'dublincore' instead.")
                            if url and 'recordSchema=marcxchange' in url:
                                corrected_url = url.replace('recordSchema=marcxchange', 'recordSchema=dublincore')
                                logger.info(f"Retrying with corrected URL: {corrected_url}")
                                response = requests.get(corrected_url, timeout=self.timeout)
                                response.raise_for_status()
                                root = ET.fromstring(response.content)  # nosec B314 
            
            # Check standard SRU diagnostics
            diagnostics = root.findall('.//srw:diagnostics/sd:diagnostic', namespaces)
            if diagnostics:
                for diag in diagnostics:
                    message_elem = diag.find('./sd:message', namespaces)
                    if message_elem is not None and message_elem.text:
                        logger.warning(f"SRU Diagnostic: {message_elem.text}")
                    
                    details_elem = diag.find('./sd:details', namespaces)
                    if details_elem is not None and details_elem.text:
                        logger.warning(f"Details: {details_elem.text}")
                        # For schema issues
                        if "schema" in details_elem.text.lower() and "unknown" in details_elem.text.lower():
                            logger.warning("The server does not support the requested schema. Try with a different schema.")
            
            # Get number of records
            num_records_elem = root.find('.//srw:numberOfRecords', namespaces)
            if num_records_elem is None:
                logger.warning("Could not find number of records in response")
                return 0, []
            
            try:
                num_records = int(num_records_elem.text or "0")
                logger.debug(f"Found {num_records} records")
            except (ValueError, TypeError):
                logger.warning(f"Invalid number of records: {num_records_elem.text}")
                return 0, []
            
            if num_records == 0:
                return 0, []
            
            # Extract records
            records: List[Dict[str, Any]] = []
            record_elems = root.findall('.//srw:record', namespaces)
            
            for record_elem in record_elems:
                # Get record schema
                schema_elem = record_elem.find('.//srw:recordSchema', namespaces)
                record_schema = schema_elem.text.strip() if schema_elem is not None and schema_elem.text else None
                
                # Get record data
                record_data_elem = record_elem.find('.//srw:recordData', namespaces)
                if record_data_elem is not None:
                    # Store the raw XML for the record
                    record_xml = ET.tostring(record_data_elem).decode('utf-8')
                    
                    # Get record identifier
                    record_id_elem = record_elem.find('.//srw:recordIdentifier', namespaces)
                    record_id = record_id_elem.text if record_id_elem is not None else None
                    
                    # Get record position if available
                    position_elem = record_elem.find('.//srw:recordPosition', namespaces)
                    position = position_elem.text if position_elem is not None else None
                    
                    records.append({
                        'id': record_id or position or f"record-{len(records)+1}",
                        'schema': record_schema,
                        'data': record_data_elem,
                        'raw_xml': record_xml
                    })
            
            return num_records, records
            
        except requests.RequestException as e:
            logger.error(f"Error executing query: {e}")
            return 0, []
        except ET.ParseError as e:
            logger.error(f"Error parsing XML response: {e}")
            return 0, []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return 0, []
    
    def search(self, query: str,
            schema: Optional[str] = None,
            max_records: int = 10,
            start_record: int = 1) -> Tuple[int, List[BiblioRecord]]:
        """
        Search the SRU endpoint and parse records.
        
        Args:
            query: CQL query
            schema: Record schema
            max_records: Maximum number of records to return
            start_record: Start record position
            
        Returns:
            Tuple of (total_records, list of BiblioRecord objects)
        """
        total, raw_records = self.execute_query(query, schema, max_records, start_record)
        
        if not raw_records:
            return total, []
        
        records: List[BiblioRecord] = []
        existing_ids = set()  # Track existing record IDs to avoid duplicates
        
        for raw_record in raw_records:
            try:
                # Ensure record ID is unique
                record_id = raw_record.get('id', f"record-{len(records)+1}")
                if record_id in existing_ids:
                    i = 1
                    new_id = f"{record_id}_{i}"
                    while new_id in existing_ids:
                        i += 1
                        new_id = f"{record_id}_{i}"
                    record_id = new_id
                existing_ids.add(record_id)
                raw_record['id'] = record_id
                
                # Determine the parser to use
                parser = self.custom_parser
                if not parser and raw_record['schema'] in self.parsers:
                    parser = self.parsers[raw_record['schema']]
                
                record = None
                if parser:
                    try:
                        record = parser(raw_record, self.namespaces)
                    except Exception as e:
                        logger.warning(f"Error in custom parser for record {record_id}: {e}")
                        # Fall back to generic parser
                        record = self._generic_parse(raw_record, self.namespaces)
                else:
                    # Use a generic parser as fallback
                    record = self._generic_parse(raw_record, self.namespaces)
                    
                if record:
                    # Ensure record has the correct ID and schema
                    record.id = record_id
                    record.schema = raw_record.get('schema')
                    records.append(record)
                else:
                    # Make a minimal record if all parsing failed
                    min_record = BiblioRecord(
                        id=record_id,
                        title=f"Unparseable Record {record_id}",
                        raw_data=raw_record['raw_xml'],
                        schema=raw_record.get('schema')
                    )
                    records.append(min_record)
                    logger.debug(f"Created minimal record for {record_id} due to parsing failure")
            
            except Exception as e:
                logger.error(f"Error handling record {raw_record.get('id', 'unknown')}: {e}")
                # Make a minimal record despite the error
                record_id = raw_record.get('id', f"record-{len(records)+1}")
                if record_id in existing_ids:
                    i = 1
                    new_id = f"{record_id}_{i}"
                    while new_id in existing_ids:
                        i += 1
                        new_id = f"{record_id}_{i}"
                    record_id = new_id
                existing_ids.add(record_id)
                
                try:
                    # Try to extract title from raw XML as a last resort
                    title_match = re.search(r'<dc:title[^>]*>(.*?)</dc:title>', raw_record['raw_xml'], re.DOTALL)
                    title = title_match.group(1).strip() if title_match else f"Error Record {record_id}"
                except Exception:
                    title = f"Error Record {record_id}"
                    
                min_record = BiblioRecord(
                    id=record_id,
                    title=title,
                    raw_data=raw_record['raw_xml'],
                    schema=raw_record.get('schema')
                )
                records.append(min_record)
        
        return total, records
    
    def _generic_parse(self, raw_record: Dict[str, Any], 
                    namespaces: Dict[str, str]) -> Optional[BiblioRecord]:
        """
        Generic record parser for when no specific parser is available.
        Attempts to extract basic Dublin Core or MARC data.
        
        Args:
            raw_record: Raw record data
            namespaces: XML namespaces
            
        Returns:
            BiblioRecord or None if parsing fails
        """
        record_data = raw_record['data']
        record_id = raw_record.get('id', 'unknown')
        
        # Try to find title using various possible paths
        title_paths = [
            './/dc:title', 
            './/dcterms:title',
            './/title',
            './/marc:datafield[@tag="245"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="245"]/mxc:subfield[@code="a"]',
        ]

        title = None
        for path in title_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text and elem.text.strip():
                    title = elem.text.strip()
                    break
            except Exception:  # nosec B110,B112
                continue

        if not title:
            # namespace-agnostic fallback (no local-name() XPath — PLAN 3.4)
            for el in record_data.iter():
                if el.tag.rsplit('}', 1)[-1] == 'title' and el.text and el.text.strip():
                    title = el.text.strip()
                    break

        if not title:
            title = f"Untitled Record ({record_id})"
        
        # Try to find authors
        authors: List[str] = []
        editors: List[str] = []
        translators: List[str] = []
        contributors: List[Dict[str, str]] = []
        
        # Extract creators/authors
        author_paths = [
            './/dc:creator',
            './/dcterms:creator',
            './/creator',
            './/marc:datafield[@tag="100"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="100"]/mxc:subfield[@code="a"]',
            './/marc:datafield[@tag="700"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="700"]/mxc:subfield[@code="a"]',
        ]
        
        seen_names = set()  # Track seen names to avoid duplicates
        
        for path in author_paths:
            try:
                elems = record_data.findall(path, namespaces)
                for elem in elems:
                    if elem.text and elem.text.strip():
                        name = elem.text.strip()
                        
                        # Check if it's an editor
                        if re.search(r'\b(?:ed(?:itor)?|hrsg|hg)\b', name.lower(), re.IGNORECASE) or "(ed" in name.lower() or "(hg" in name.lower() or "(hg.)" in name.lower():

                            # Clean editor name by removing role designation

                            clean_name = re.sub(r'\s*[\(\[][^)]*(?:ed|hrsg|edit|hg)[^)]*[\)\]]', '', name)
                            clean_name = re.sub(r'\s*(?:ed|hrsg|edit|hg)\.?(?:\s+|$)', '', clean_name)
                            clean_name = clean_name.strip()
                            
                            if clean_name and clean_name not in seen_names:
                                editors.append(clean_name)
                                seen_names.add(clean_name)
                            continue
                        
                        # Check if it's a translator
                        if re.search(r'\b(?:trans|transl|translator|übersetz|übers)\b', name.lower(), re.IGNORECASE):
                            # Clean translator name
                            clean_name = re.sub(r'\s*[\(\[][^)]*(?:trans|übersetz)[^)]*[\)\]]', '', name)
                            clean_name = re.sub(r'\s*(?:trans|transl|translator|übersetz|übers)\.?(?:\s+|$)', '', clean_name)
                            clean_name = clean_name.strip()
                            
                            if clean_name and clean_name not in seen_names:
                                translators.append(clean_name)
                                seen_names.add(clean_name)
                            continue
                            
                        # Regular author
                        if name not in seen_names:
                            authors.append(name)
                            seen_names.add(name)
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find year
        year = None
        date_paths = [
            './/dc:date',
            './/dcterms:date',
            './/dcterms:issued',
            './/date',
            './/marc:datafield[@tag="260"]/marc:subfield[@code="c"]',
            './/mxc:datafield[@tag="260"]/mxc:subfield[@code="c"]',
            './/marc:datafield[@tag="264"]/marc:subfield[@code="c"]',
            './/mxc:datafield[@tag="264"]/mxc:subfield[@code="c"]',
        ]
        
        for path in date_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    date_text = elem.text.strip()
                    # Extract year
                    match = re.search(r'\b(1\d{3}|20\d{2})\b', date_text)
                    if match:
                        year = match.group(1)
                        break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find publisher
        publisher = None
        publisher_paths = [
            './/dc:publisher',
            './/dcterms:publisher',
            './/publisher',
            './/marc:datafield[@tag="260"]/marc:subfield[@code="b"]',
            './/mxc:datafield[@tag="260"]/mxc:subfield[@code="b"]',
            './/marc:datafield[@tag="264"]/marc:subfield[@code="b"]',
            './/mxc:datafield[@tag="264"]/mxc:subfield[@code="b"]',
        ]
        
        for path in publisher_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    publisher = elem.text.strip()
                    # Clean up publisher string (remove trailing punctuation)
                    publisher = re.sub(r'[,:]$', '', publisher).strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find place of publication
        place_of_publication = None
        place_paths = [
            './/marc:datafield[@tag="260"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="260"]/mxc:subfield[@code="a"]',
            './/marc:datafield[@tag="264"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="264"]/mxc:subfield[@code="a"]'
        ]
        
        for path in place_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    place_of_publication = elem.text.strip()
                    # Clean up place (remove trailing punctuation)
                    place_of_publication = re.sub(r'[,:]$', '', place_of_publication).strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find ISBN
        isbn = None
        isbn_paths = [
            './/bibo:isbn13',
            './/bibo:isbn10',
            './/bibo:isbn',
            './/bibo:gtin14',
            './/marc:datafield[@tag="020"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="020"]/mxc:subfield[@code="a"]',
        ]
        
        for path in isbn_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    isbn_text = elem.text.strip()
                    # Extract ISBN
                    match = re.search(r'(?:ISBN[:\s]*)?(\d[\d\-X]+)', isbn_text)
                    if match:
                        isbn = match.group(1)
                        break
                    else:
                        isbn = isbn_text
                        break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find ISSN
        issn = None
        issn_paths = [
            './/bibo:issn',
            './/marc:datafield[@tag="022"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="022"]/mxc:subfield[@code="a"]',
        ]

        for path in issn_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    issn_text = elem.text.strip()
                    # Extract ISSN
                    match = re.search(r'(?:ISSN[:\s]*)?(\d{4}-\d{3}[\dX])', issn_text)
                    if match:
                        issn = match.group(1)
                        break
                    else:
                        issn = issn_text
                        break
            except Exception:  # nosec B110,B112
                continue

        # Namespace-agnostic fallback for identifiers embedded in <identifier>
        # element text, e.g. Dublin Core "<dc:identifier>ISBN 978-3-16-148410-0
        # </dc:identifier>". ElementTree has no contains()/text()/local-name()
        # XPath (raises "invalid predicate"), which silently disabled the old
        # './/dc:identifier[contains(text(),"ISBN")]' paths — so DC records lost
        # their ISBN/ISSN/URL. Iterate instead (PLAN 3.4).
        def _localname(tag):
            return tag.rsplit('}', 1)[-1] if isinstance(tag, str) and '}' in tag else tag
        identifier_texts = [
            el.text.strip()
            for el in record_data.iter()
            if _localname(el.tag) == 'identifier' and el.text and el.text.strip()
        ]
        if not isbn:
            for t in identifier_texts:
                if 'isbn' in t.lower():
                    match = re.search(r'(?:ISBN[:\s]*)?(\d[\d\-X]+)', t)
                    isbn = match.group(1) if match else t
                    break
        if not issn:
            for t in identifier_texts:
                if 'issn' in t.lower():
                    match = re.search(r'(?:ISSN[:\s]*)?(\d{4}-\d{3}[\dX])', t)
                    if match:
                        issn = match.group(1)
                        break

        # Try to find journal title (for articles)
        journal_title = None
        journal_paths = [
            './/marc:datafield[@tag="773"]/marc:subfield[@code="t"]',
            './/mxc:datafield[@tag="773"]/mxc:subfield[@code="t"]',
            './/marc:datafield[@tag="773"]/marc:subfield[@code="p"]',
            './/mxc:datafield[@tag="773"]/mxc:subfield[@code="p"]'
        ]
        
        for path in journal_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    journal_title = elem.text.strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find volume and issue
        volume = None
        issue = None
        
        # Volume
        volume_paths = [
            './/marc:datafield[@tag="773"]/marc:subfield[@code="v"]',
            './/mxc:datafield[@tag="773"]/mxc:subfield[@code="v"]'
        ]
        
        for path in volume_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    volume = elem.text.strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Issue
        issue_paths = [
            './/marc:datafield[@tag="773"]/marc:subfield[@code="l"]',
            './/mxc:datafield[@tag="773"]/mxc:subfield[@code="l"]'
        ]
        
        for path in issue_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    issue = elem.text.strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find page range
        pages = None
        pages_paths = [
            './/marc:datafield[@tag="773"]/marc:subfield[@code="g"]',
            './/mxc:datafield[@tag="773"]/mxc:subfield[@code="g"]'
        ]
        
        for path in pages_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    # Try to extract page range from various formats
                    page_text = elem.text.strip()
                    page_match = re.search(r'p\.?\s*(\d+(?:-\d+)?)', page_text, re.IGNORECASE)
                    if page_match:
                        pages = page_match.group(1)
                    else:
                        # Just use raw text if no pattern matched
                        pages = page_text
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find extent (number of pages for books)
        extent = None
        extent_paths = [
            './/marc:datafield[@tag="300"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="300"]/mxc:subfield[@code="a"]'
        ]
        
        for path in extent_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    extent = elem.text.strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find DOI
        doi = None

        # Implementation for finding DOI that doesn't use getparent()
        for path in ['.//marc:datafield[@tag="024"][@ind1="7"]', './/mxc:datafield[@tag="024"][@ind1="7"]']:
            try:
                fields = record_data.findall(path, namespaces)
                for fld in fields:
                    type_subfield = fld.find('./marc:subfield[@code="2"]', namespaces) or fld.find('./mxc:subfield[@code="2"]', namespaces)
                    value_subfield = fld.find('./marc:subfield[@code="a"]', namespaces) or fld.find('./mxc:subfield[@code="a"]', namespaces)
                    
                    if (type_subfield is not None and type_subfield.text 
                            and type_subfield.text.strip().lower() == "doi" 
                            and value_subfield is not None and value_subfield.text):
                        doi = value_subfield.text.strip()
                        break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find document type
        document_type = None
        leader = None
        
        try:
            leader_elem = record_data.find('.//marc:leader', namespaces)
            # NB: `if not leader_elem` is True for a childless element in ElementTree
            # (truthiness = has children), which wrongly discarded the leader.
            if leader_elem is None:
                leader_elem = record_data.find('.//mxc:leader', namespaces)
            if leader_elem is not None and leader_elem.text:
                leader = leader_elem.text
        except Exception:  # nosec
            pass
        
        if leader:
            # Position 6 and 7 in MARC leader indicate record type and bibliographic level
            if len(leader) >= 8:
                record_type = leader[6]
                biblio_level = leader[7]
                
                if record_type == 'a' and biblio_level == 's':
                    document_type = 'Journal'
                elif record_type == 'a' and biblio_level == 'm':
                    document_type = 'Book'
                elif record_type == 'a' and biblio_level == 'a':
                    document_type = 'Journal Article'
                elif record_type == 'a' and biblio_level == 'c':
                    document_type = 'Book Chapter'
                elif record_type == 'e':
                    document_type = 'Map'
                elif record_type == 'g':
                    document_type = 'Video'
                elif record_type == 'j':
                    document_type = 'Music'
                elif record_type == 'k':
                    document_type = 'Image'
                elif record_type == 'm':
                    document_type = 'Computer File'
        
        # Infer document type from other clues if not found in leader
        if not document_type:
            if journal_title and (pages or volume or issue):
                document_type = 'Journal Article'
            elif issn:
                document_type = 'Journal'
            elif isbn:
                document_type = 'Book'
        
        # Try to find URLs
        urls = []
        url_paths = [
            './/foaf:primaryTopic',
            './/umbel:isLike',
            './/marc:datafield[@tag="856"]/marc:subfield[@code="u"]',
            './/mxc:datafield[@tag="856"]/mxc:subfield[@code="u"]'
        ]

        for path in url_paths:
            try:
                elems = record_data.findall(path, namespaces)
                for elem in elems:
                    # Check for resource attribute first (RDF style)
                    resource = elem.get('{'+namespaces.get('rdf', '')+'}resource')
                    if resource and resource.startswith('http'):
                        urls.append(resource)
                    # Check for text content
                    elif elem.text and elem.text.strip().startswith('http'):
                        urls.append(elem.text.strip())
            except Exception:  # nosec B110,B112
                continue

        # <identifier>http…</identifier> URLs (replaces the non-functional
        # contains(text(),"http") XPath — PLAN 3.4).
        for t in identifier_texts:
            if t.startswith('http') and t not in urls:
                urls.append(t)
        
        # Try to find subjects
        subjects = []
        subject_paths = [
            './/dc:subject',
            './/dcterms:subject',
            './/marc:datafield[@tag="650"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="650"]/mxc:subfield[@code="a"]',
            './/marc:datafield[@tag="651"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="651"]/mxc:subfield[@code="a"]',
            './/marc:datafield[@tag="653"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="653"]/mxc:subfield[@code="a"]'
        ]
        
        for path in subject_paths:
            try:
                elems = record_data.findall(path, namespaces)
                for elem in elems:
                    if elem.text and elem.text.strip():
                        subjects.append(elem.text.strip())
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find abstract/description
        abstract = None
        abstract_paths = [
            './/dc:description',
            './/dcterms:abstract',
            './/marc:datafield[@tag="520"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="520"]/mxc:subfield[@code="a"]'
        ]
        
        for path in abstract_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    abstract = elem.text.strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find language
        language = None
        language_paths = [
            './/dc:language',
            './/dcterms:language',
            './/marc:datafield[@tag="041"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="041"]/mxc:subfield[@code="a"]'
        ]
        
        for path in language_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    language = elem.text.strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find series
        series = None
        series_paths = [
            './/marc:datafield[@tag="490"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="490"]/mxc:subfield[@code="a"]',
            './/marc:datafield[@tag="830"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="830"]/mxc:subfield[@code="a"]'
        ]
        
        for path in series_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    series = elem.text.strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Try to find edition
        edition = None
        edition_paths = [
            './/marc:datafield[@tag="250"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="250"]/mxc:subfield[@code="a"]'
        ]
        
        for path in edition_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    edition = elem.text.strip()
                    break
            except Exception:  # nosec B110,B112 
                continue
        
        # Create BiblioRecord with all extracted fields
        return BiblioRecord(
            id=record_id,
            title=title,
            authors=authors,
            editors=editors,
            translators=translators,
            contributors=contributors,
            year=year,
            publisher_name=publisher,
            place_of_publication=place_of_publication,
            isbn=isbn,
            issn=issn,
            urls=urls,
            abstract=abstract,
            language=language,
            format=document_type,  # Use detected document_type as format
            subjects=subjects,
            series=series,
            extent=extent,
            edition=edition,
            journal_title=journal_title,
            volume=volume,
            issue=issue,
            pages=pages,
            doi=doi,
            document_type=document_type,
            raw_data=raw_record['raw_xml'],
            schema=raw_record.get('schema')
        )
    
    def _extract_text(self, elem: ET.Element, xpath_list: List[str], 
                     namespaces: Dict[str, str]) -> Optional[str]:
        """Extract text using a list of XPath expressions, trying each until one succeeds."""
        for xpath in xpath_list:
            result = elem.find(xpath, namespaces)
            if result is not None and result.text and result.text.strip():
                return result.text.strip()
        return None
    
    def _find_elements(self, elem: ET.Element, xpath_list: List[str], 
                       namespaces: Dict[str, str]) -> List[ET.Element]:
        """Find elements using a list of XPath expressions, trying each until one succeeds."""
        for xpath in xpath_list:
            results = elem.findall(xpath, namespaces)
            if results:
                return results
        return []


# Register the shared SRU parsers (imported near the top) on this SRUClient.
for _schema in ('info:srw/schema/1/dc-v1.1', 'dc', 'dublincore'):
    SRUClient.register_parser(_schema)(parse_dublin_core)
for _schema in ('marcxml', 'info:srw/schema/1/marcxml-v1.1', 'MARC21-xml'):
    SRUClient.register_parser(_schema)(parse_marcxml)
SRUClient.register_parser('RDFxml')(parse_rdfxml)

# ── Formatter helpers shared with CrispZotLib (parity-critical) ──────────────
# These mirror src/modules/librarySearch/formatters.ts EXACTLY. The shared
# golden files (fixtures/parity/, synced from CrispZotLib) assert byte-identical
# BibTeX/RIS output across both languages — change them in lockstep only.

def escape_bibtex(value: str) -> str:
    """Escape BibTeX-special characters in a prose value. Applied to text
    fields (title, names, journal, publisher, address, series, note). NOT
    applied to url/doi/isbn, where a backslash would corrupt the identifier."""
    return re.sub(r'([#$%&_{}])', r'\\\1', value)


def clean_creator_name(raw: str) -> str:
    """Strip role indicators ("Schmidt, Anna [Verfasser]") and stray brackets
    from a creator name for BibTeX/RIS output."""
    name = re.sub(r'\s*\[[^\]]*\]', '', raw)
    name = re.sub(r',\s*$', '', name.strip())
    name = re.sub(r'\]\s*$', '', name)
    name = re.sub(r'^\s*\[', '', name)
    return name


# Substrings that mark a name as an organization rather than a person —
# corporate authors ("United Nations") must not be split into "Nations, United".
# Same pattern as CORPORATE_MARKERS in CrispZotLib's formatters.ts.
CORPORATE_NAME_MARKERS = re.compile(
    r'(univ(?:ersit)?|institut|department|abteilung|minist|organi[sz]ation|associat'
    r'|society|gesellschaft|foundation|stiftung|verlag|bibliothek|library|committee'
    r'|commission|kommission|council|corporation|gmbh|publish|verein|hochschule'
    r'|akademie|academy|bundes|united nations|european union)'
    r'|(\b(?:inc|ltd|ag|co|plc|llc|who|unesco|oecd|office|bureau|agency|company|press)\b)',
    re.IGNORECASE,
)


def _bibtex_creator_list(names: List[str]) -> Optional[str]:
    """Clean a creator list for BibTeX; None when no usable name remains."""
    cleaned = [clean_creator_name(n) for n in names]
    cleaned = [n for n in cleaned if n]
    if not cleaned:
        return None
    return " and ".join(escape_bibtex(n) for n in cleaned)


def format_ris_creator(raw: str) -> str:
    """Format a creator for a RIS AU/ED line ("Last, First"). Role markers are
    stripped; corporate and mononym names are kept verbatim rather than
    flipped. Returns "" when nothing usable remains."""
    name = clean_creator_name(raw)
    if not name:
        return ""
    if ',' in name:
        return name
    if CORPORATE_NAME_MARKERS.search(name):
        return name
    parts = name.split()
    if len(parts) == 1:
        return name
    return f"{parts[-1]}, {' '.join(parts[:-1])}"


def bibtex_from_record(record: BiblioRecord) -> str:
    """
    Convert a BiblioRecord to BibTeX format.

    Parity-critical: must produce byte-identical output to CrispZotLib's
    formatRecordBibtex (asserted by test_formatter_parity.py on the shared
    goldens in fixtures/parity/).

    Args:
        record: BiblioRecord object

    Returns:
        BibTeX formatted string
    """
    # Get citation key from record
    citation_key = record.get_citation_key()

    # Determine entry type; a record with a journal title but no explicit
    # document_type is still an article.
    if record.document_type:
        doc_type_lower = record.document_type.lower()
        if "article" in doc_type_lower:
            entry_type = "article"
        elif "chapter" in doc_type_lower:
            entry_type = "incollection"
        elif "thesis" in doc_type_lower:
            entry_type = "phdthesis"
        elif "proceedings" in doc_type_lower:
            entry_type = "inproceedings"
        elif "report" in doc_type_lower:
            entry_type = "techreport"
        else:
            entry_type = "book"
    elif record.journal_title:
        entry_type = "article"
    else:
        entry_type = "book"

    # Start building BibTeX
    bibtex = [f"@{entry_type}{{{citation_key},"]

    # Strip the ISBD statement of responsibility (" / John Smith") from the
    # title. Whitespace required on BOTH sides so in-word slashes survive
    # ("TCP/IP", "Either/Or").
    title = re.sub(r'\s+/\s+[^/]+$', '', record.title)
    title = escape_bibtex(title)
    bibtex.append(f"  title = {{{title}}},")

    # Authors / editors / translators (role markers stripped, empties dropped)
    if record.authors:
        authors_list = _bibtex_creator_list(record.authors)
        if authors_list:
            bibtex.append(f"  author = {{{authors_list}}},")

    if record.editors:
        editors_list = _bibtex_creator_list(record.editors)
        if editors_list:
            bibtex.append(f"  editor = {{{editors_list}}},")

    if record.translators:
        translators_list = _bibtex_creator_list(record.translators)
        if translators_list:
            bibtex.append(f"  translator = {{{translators_list}}},")

    # Year
    if record.year:
        bibtex.append(f"  year = {{{record.year}}},")

    # Journal for articles
    if entry_type == "article" and record.journal_title:
        bibtex.append(f"  journal = {{{escape_bibtex(record.journal_title)}}},")

        # Volume
        if record.volume:
            bibtex.append(f"  volume = {{{record.volume}}},")

        # Issue/Number
        if record.issue:
            bibtex.append(f"  number = {{{record.issue}}},")

    # Publisher
    if record.publisher_name:
        bibtex.append(f"  publisher = {{{escape_bibtex(record.publisher_name)}}},")

    # Address (place of publication)
    if record.place_of_publication:
        bibtex.append(f"  address = {{{escape_bibtex(record.place_of_publication)}}},")

    # Series
    if record.series:
        bibtex.append(f"  series = {{{escape_bibtex(record.series)}}},")

    # ISBN
    if record.isbn:
        bibtex.append(f"  isbn = {{{record.isbn}}},")

    # ISSN for journals
    if entry_type == "article" and record.issn:
        bibtex.append(f"  issn = {{{record.issn}}},")

    # DOI
    if record.doi:
        bibtex.append(f"  doi = {{{record.doi}}},")

    # Pages
    if record.pages:
        bibtex.append(f"  pages = {{{record.pages}}},")

    # Edition
    if record.edition:
        bibtex.append(f"  edition = {{{record.edition}}},")

    # URL (use the first one if multiple are available)
    if record.urls:
        bibtex.append(f"  url = {{{record.urls[0]}}},")

    # Language
    if record.language:
        bibtex.append(f"  language = {{{record.language}}},")

    # Put record ID in note field for reference
    bibtex.append(f"  note = {{ID: {escape_bibtex(record.id)}}}")

    # Close the entry
    bibtex.append("}")

    return "\n".join(bibtex)

# Function to convert a list of BiblioRecords to BibTeX format
def bibtex_from_records(records: List[BiblioRecord]) -> str:
    """
    Convert a list of BiblioRecords to BibTeX format with proper handling
    for duplicate keys.
    
    Args:
        records: List of BiblioRecord objects
        
    Returns:
        BibTeX formatted string with all records
    """
    results = []
    used_keys = set()
    
    for i, record in enumerate(records):
        # Get base citation key and ensure uniqueness
        base_key = record.get_citation_key()
        # Clean up citation key to avoid problematic characters
        base_key = re.sub(r'[^a-zA-Z0-9]', '', base_key)
        
        # If key is empty (e.g., no author), use "unknown"
        if not base_key:
            base_key = "unknown"
            
        citation_key = base_key
        
        # If key already exists, add a suffix
        if citation_key in used_keys:
            j = 1
            while f"{citation_key}{j}" in used_keys:
                j += 1
            citation_key = f"{citation_key}{j}"
        
        used_keys.add(citation_key)
        
        # Create a copy of the record with the new key
        record_copy = BiblioRecord(
            id=citation_key,
            title=record.title,
            authors=record.authors.copy() if record.authors else [],
            editors=record.editors.copy() if record.editors else [],
            translators=record.translators.copy() if record.translators else [],
            contributors=record.contributors.copy() if record.contributors else [],
            year=record.year,
            publisher_name=record.publisher_name,
            place_of_publication=record.place_of_publication,
            isbn=record.isbn,
            issn=record.issn,
            urls=record.urls.copy() if record.urls else [],
            abstract=record.abstract,
            language=record.language,
            format=record.format,
            subjects=record.subjects.copy() if record.subjects else [],
            series=record.series,
            extent=record.extent,
            edition=record.edition,
            journal_title=record.journal_title,
            volume=record.volume,
            issue=record.issue,
            pages=record.pages,
            doi=record.doi,
            document_type=record.document_type,
            raw_data=record.raw_data,
            schema=record.schema
        )
        
        # Add BibTeX for this record
        results.append(bibtex_from_record(record_copy))
        
        # Add a separator between records
        if i < len(records) - 1:
            results.append("")
    
    return "\n".join(results)

# List of commonly used SRU endpoints
# SRU endpoints come from the shared manifest (edit endpoints.json, not here).
from endpoints_manifest import SRU_ENDPOINTS  # noqa: E402
