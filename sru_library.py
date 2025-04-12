#!/usr/bin/env python3
# sru_library.py
"""
SRU Library - A flexible SRU (Search/Retrieve via URL) client for bibliographic data

This module provides a modular approach to query various library SRU endpoints
without requiring hardcoded classes for each specific library.
"""

import requests
import xml.etree.ElementTree as ET
import urllib.parse
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union, Tuple, Callable
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("sru_library")

@dataclass
class BiblioRecord:
    """Data class for bibliographic records."""
    id: str
    title: str
    authors: List[str] = field(default_factory=list)
    year: Optional[str] = None
    publisher_name: Optional[str] = None
    place_of_publication: Optional[str] = None
    isbn: Optional[str] = None
    issn: Optional[str] = None
    urls: List[str] = field(default_factory=list)
    abstract: Optional[str] = None
    language: Optional[str] = None
    format: Optional[str] = None
    subjects: List[str] = field(default_factory=list)
    series: Optional[str] = None
    extent: Optional[str] = None  # Number of pages, duration, etc.
    edition: Optional[str] = None
    raw_data: Any = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "authors": self.authors,
            "year": self.year,
            "publisher_name": self.publisher_name,
            "place_of_publication": self.place_of_publication,
            "isbn": self.isbn,
            "issn": self.issn,
            "urls": self.urls,
            "abstract": self.abstract,
            "language": self.language,
            "format": self.format,
            "subjects": self.subjects,
            "series": self.series,
            "extent": self.extent,
            "edition": self.edition
        }
    
    def __str__(self) -> str:
        """String representation of the record."""
        authors_str = ", ".join(self.authors) if self.authors else "Unknown"
        pub_info = []
        if self.place_of_publication:
            pub_info.append(self.place_of_publication)
        if self.publisher_name:
            pub_info.append(self.publisher_name)
        
        pub_str = ": ".join(pub_info) if pub_info else "Unknown"
        
        return f"{self.title} by {authors_str} ({self.year or 'n.d.'}, {pub_str})"


class SRUClient:
    """
    A flexible SRU (Search/Retrieve via URL) client that can work with any SRU endpoint.
    """
    
    # Registry of record format parsers
    parsers = {}
    
    @classmethod
    def register_parser(cls, schema_name):
        """Decorator to register a parser function for a specific schema."""
        def decorator(parser_func):
            cls.parsers[schema_name] = parser_func
            return parser_func
        return decorator
    
    def __init__(self, 
                base_url: str,
                default_schema: str = None,
                version: str = "1.1",
                namespaces: Dict[str, str] = None,
                timeout: int = 30, 
                record_parser: Optional[Callable] = None,
                query_params: Dict[str, str] = None):
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
                        schema: str = None,
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
                    schema: str = None,
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
            root = ET.fromstring(response.content)
            
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
                    uri_elem = diag.find('./sd:uri', namespaces)
                    
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
                                root = ET.fromstring(response.content)
            
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
                num_records = int(num_records_elem.text)
                logger.debug(f"Found {num_records} records")
            except (ValueError, TypeError):
                logger.warning(f"Invalid number of records: {num_records_elem.text}")
                return 0, []
            
            if num_records == 0:
                return 0, []
            
            # Extract records
            records = []
            record_elems = root.findall('.//srw:record', namespaces)
            
            for record_elem in record_elems:
                # Get record schema
                schema_elem = record_elem.find('.//srw:recordSchema', namespaces)
                record_schema = schema_elem.text if schema_elem is not None else None
                
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
            schema: str = None,
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
        
        records = []
        for raw_record in raw_records:
            try:
                # Determine the parser to use
                parser = self.custom_parser
                if not parser and raw_record['schema'] in self.parsers:
                    parser = self.parsers[raw_record['schema']]
                
                record = None
                if parser:
                    try:
                        record = parser(raw_record, self.namespaces)
                    except Exception as e:
                        logger.warning(f"Error in custom parser for record {raw_record.get('id', 'unknown')}: {e}")
                        # Fall back to generic parser
                        record = self._generic_parse(raw_record, self.namespaces)
                else:
                    # Use a generic parser as fallback
                    record = self._generic_parse(raw_record, self.namespaces)
                    
                if record:
                    records.append(record)
                else:
                    # Make a minimal record if all parsing failed
                    record_id = raw_record.get('id', f"record-{len(records)+1}")
                    min_record = BiblioRecord(
                        id=record_id,
                        title=f"Unparseable Record {record_id}",
                        raw_data=raw_record['raw_xml']
                    )
                    records.append(min_record)
                    logger.debug(f"Created minimal record for {record_id} due to parsing failure")
            
            except Exception as e:
                logger.error(f"Error handling record {raw_record.get('id', 'unknown')}: {e}")
                # Make a minimal record despite the error
                record_id = raw_record.get('id', f"record-{len(records)+1}")
                try:
                    # Try to extract title from raw XML as a last resort
                    title_match = re.search(r'<dc:title[^>]*>(.*?)</dc:title>', raw_record['raw_xml'], re.DOTALL)
                    title = title_match.group(1).strip() if title_match else f"Error Record {record_id}"
                except Exception:
                    title = f"Error Record {record_id}"
                    
                min_record = BiblioRecord(
                    id=record_id,
                    title=title,
                    raw_data=raw_record['raw_xml']
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
            './/*[local-name()="title"]'
        ]
        
        title = None
        for path in title_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text and elem.text.strip():
                    title = elem.text.strip()
                    break
            except Exception:
                continue
        
        if not title:
            title = f"Untitled Record ({record_id})"
        
        # Try to find authors
        authors = []
        author_paths = [
            './/dc:creator',
            './/dcterms:creator',
            './/creator',
            './/marc:datafield[@tag="100"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="100"]/mxc:subfield[@code="a"]',
            './/marc:datafield[@tag="700"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="700"]/mxc:subfield[@code="a"]',
            './/*[local-name()="creator"]',
            './/*[local-name()="author"]'
        ]
        
        for path in author_paths:
            try:
                elems = record_data.findall(path, namespaces)
                for elem in elems:
                    if elem.text and elem.text.strip():
                        authors.append(elem.text.strip())
            except Exception:
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
            './/*[local-name()="date"]',
            './/*[local-name()="issued"]'
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
            except Exception:
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
            './/*[local-name()="publisher"]'
        ]
        
        for path in publisher_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    publisher = elem.text.strip()
                    # Clean up publisher string (remove trailing punctuation)
                    publisher = re.sub(r'[,:]$', '', publisher).strip()
                    break
            except Exception:
                continue
        
        # Try to find ISBN
        isbn = None
        isbn_paths = [
            './/bibo:isbn13',
            './/bibo:isbn10',
            './/bibo:isbn',
            './/bibo:gtin14',
            './/dc:identifier[contains(text(), "ISBN")]',
            './/marc:datafield[@tag="020"]/marc:subfield[@code="a"]',
            './/mxc:datafield[@tag="020"]/mxc:subfield[@code="a"]',
            './/*[local-name()="identifier" and contains(text(), "ISBN")]'
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
            except Exception:
                continue
        
        # Try to find URLs
        urls = []
        url_paths = [
            './/foaf:primaryTopic',
            './/umbel:isLike',
            './/dc:identifier[contains(text(), "http")]',
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
            except Exception:
                continue
        
        return BiblioRecord(
            id=record_id,
            title=title,
            authors=authors,
            year=year,
            publisher=publisher,
            isbn=isbn,
            urls=urls,
            raw_data=raw_record['raw_xml']
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


# Register parser for Dublin Core format
@SRUClient.register_parser('info:srw/schema/1/dc-v1.1')
@SRUClient.register_parser('dc')
@SRUClient.register_parser('dublincore')
def parse_dublin_core(raw_record, namespaces):
    """Parse Dublin Core format records."""
    data = raw_record['data']
    record_id = raw_record.get('id', 'unknown')
    
    # Add Dublin Core namespace if not present
    ns = namespaces.copy()
    if 'dc' not in ns:
        ns['dc'] = 'http://purl.org/dc/elements/1.1/'
    
    # Find title
    title_elem = data.find('.//dc:title', ns)
    title = title_elem.text.strip() if title_elem is not None and title_elem.text else "Untitled"
    
    # Find authors
    authors = []
    creator_elems = data.findall('.//dc:creator', ns)
    for elem in creator_elems:
        if elem.text and elem.text.strip():
            # Remove roles like "Author" if present
            author = elem.text.strip()
            if '.' in author:
                parts = author.split('.')
                if len(parts) > 1:
                    author = parts[0].strip()
            authors.append(author)
    
    # Find dates
    date_elem = data.find('.//dc:date', ns)
    year = None
    if date_elem is not None and date_elem.text:
        date_text = date_elem.text.strip()
        # Extract year
        match = re.search(r'\b(1\d{3}|20\d{2})\b', date_text)
        if match:
            year = match.group(1)
    
    # Find publisher
    publisher_elem = data.find('.//dc:publisher', ns)
    publisher = publisher_elem.text.strip() if publisher_elem is not None and publisher_elem.text else None
    
    # Find identifiers (ISBN, etc.)
    isbn = None
    identifier_elems = data.findall('.//dc:identifier', ns)
    for elem in identifier_elems:
        if elem.text and 'isbn' in elem.text.lower():
            # Extract ISBN
            match = re.search(r'(?:ISBN[:\s]*)?(\d[\d\-X]+)', elem.text)
            if match:
                isbn = match.group(1)
                break
    
    # Find subjects
    subjects = []
    subject_elems = data.findall('.//dc:subject', ns)
    for elem in subject_elems:
        if elem.text and elem.text.strip():
            subjects.append(elem.text.strip())
    
    # Find description (abstract)
    description_elem = data.find('.//dc:description', ns)
    abstract = description_elem.text.strip() if description_elem is not None and description_elem.text else None
    
    # Find language
    language_elem = data.find('.//dc:language', ns)
    language = language_elem.text.strip() if language_elem is not None and language_elem.text else None
    
    # Find format
    format_elem = data.find('.//dc:format', ns)
    format_str = format_elem.text.strip() if format_elem is not None and format_elem.text else None
    
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        year=year,
        publisher=publisher,
        isbn=isbn,
        subjects=subjects,
        abstract=abstract,
        language=language,
        format=format_str,
        raw_data=raw_record['raw_xml']
    )


# Register parser for MARCXML format
@SRUClient.register_parser('marcxml')
@SRUClient.register_parser('info:srw/schema/1/marcxml-v1.1')
@SRUClient.register_parser('MARC21-xml')
def parse_marcxml(raw_record, namespaces):
    """Parse MARCXML format records."""
    data = raw_record['data']
    record_id = raw_record.get('id', 'unknown')
    
    # Add MARC namespace if not present
    ns = namespaces.copy()
    if 'marc' not in ns:
        ns['marc'] = 'http://www.loc.gov/MARC21/slim'
    if 'mxc' not in ns:
        ns['mxc'] = 'info:lc/xmlns/marcxchange-v2'
    
    # Find record element (which might be nested differently depending on the source)
    record = data.find('.//marc:record', ns)
    if record is None:
        record = data.find('.//mxc:record', ns)
    if record is None:
        record = data.find('.//*[local-name()="record"]')
    if record is None:
        record = data  # Use the data element as fallback
    
    # Find title (MARC field 245 subfield a)
    title = "Untitled"
    title_fields = record.findall('.//marc:datafield[@tag="245"]/marc:subfield[@code="a"]', ns)
    if not title_fields:
        title_fields = record.findall('.//mxc:datafield[@tag="245"]/mxc:subfield[@code="a"]', ns)
    if title_fields and title_fields[0].text:
        title = title_fields[0].text.strip()
        # Some titles end with / or : or other punctuation
        title = re.sub(r'[/:]$', '', title).strip()
    
    # Find authors (MARC fields 100, 700)
    authors = []
    
    # Creator (100)
    creator_fields = record.findall('.//marc:datafield[@tag="100"]/marc:subfield[@code="a"]', ns)
    if not creator_fields:
        creator_fields = record.findall('.//mxc:datafield[@tag="100"]/mxc:subfield[@code="a"]', ns)
    for field in creator_fields:
        if field.text and field.text.strip():
            authors.append(field.text.strip())
    
    # Contributors (700)
    contributor_fields = record.findall('.//marc:datafield[@tag="700"]/marc:subfield[@code="a"]', ns)
    if not contributor_fields:
        contributor_fields = record.findall('.//mxc:datafield[@tag="700"]/mxc:subfield[@code="a"]', ns)
    for field in contributor_fields:
        if field.text and field.text.strip():
            authors.append(field.text.strip())
    
    # Find year (MARC field 260/264 subfield c)
    year = None
    for tag in ['260', '264']:
        date_fields = record.findall(f'.//marc:datafield[@tag="{tag}"]/marc:subfield[@code="c"]', ns)
        if not date_fields:
            date_fields = record.findall(f'.//mxc:datafield[@tag="{tag}"]/mxc:subfield[@code="c"]', ns)
        if date_fields and date_fields[0].text:
            date_text = date_fields[0].text.strip()
            # Extract year
            match = re.search(r'\b(1\d{3}|20\d{2})\b', date_text)
            if match:
                year = match.group(1)
                break
    
    # Find publisher (MARC field 260/264 subfield b)
    publisher = None
    for tag in ['260', '264']:
        publisher_fields = record.findall(f'.//marc:datafield[@tag="{tag}"]/marc:subfield[@code="b"]', ns)
        if not publisher_fields:
            publisher_fields = record.findall(f'.//mxc:datafield[@tag="{tag}"]/mxc:subfield[@code="b"]', ns)
        if publisher_fields and publisher_fields[0].text:
            publisher = publisher_fields[0].text.strip()
            # Some publishers end with , or : or other punctuation
            publisher = re.sub(r'[,:]$', '', publisher).strip()
            break
    
    # Find ISBN (MARC field 020 subfield a)
    isbn = None
    isbn_fields = record.findall('.//marc:datafield[@tag="020"]/marc:subfield[@code="a"]', ns)
    if not isbn_fields:
        isbn_fields = record.findall('.//mxc:datafield[@tag="020"]/mxc:subfield[@code="a"]', ns)
    if isbn_fields and isbn_fields[0].text:
        isbn_text = isbn_fields[0].text.strip()
        # Extract just the ISBN part
        match = re.search(r'(\d[\d\-X]+)', isbn_text)
        if match:
            isbn = match.group(1)
    
    # Find subjects (MARC fields 650, 651)
    subjects = []
    for tag in ['650', '651']:
        subject_fields = record.findall(f'.//marc:datafield[@tag="{tag}"]/marc:subfield[@code="a"]', ns)
        if not subject_fields:
            subject_fields = record.findall(f'.//mxc:datafield[@tag="{tag}"]/mxc:subfield[@code="a"]', ns)
        for field in subject_fields:
            if field.text and field.text.strip():
                subjects.append(field.text.strip())
    
    # Find language (MARC field 041 subfield a or 008 positions 35-37)
    language = None
    language_fields = record.findall('.//marc:datafield[@tag="041"]/marc:subfield[@code="a"]', ns)
    if not language_fields:
        language_fields = record.findall('.//mxc:datafield[@tag="041"]/mxc:subfield[@code="a"]', ns)
    if language_fields and language_fields[0].text:
        language = language_fields[0].text.strip()
    
    # Find URLs (MARC field 856 subfield u)
    urls = []
    url_fields = record.findall('.//marc:datafield[@tag="856"]/marc:subfield[@code="u"]', ns)
    if not url_fields:
        url_fields = record.findall('.//mxc:datafield[@tag="856"]/mxc:subfield[@code="u"]', ns)
    for field in url_fields:
        if field.text and field.text.strip():
            urls.append(field.text.strip())
    
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        year=year,
        publisher=publisher,
        isbn=isbn,
        subjects=subjects,
        language=language,
        urls=urls,
        raw_data=raw_record['raw_xml']
    )


# Register parser for RDF/XML format
@SRUClient.register_parser('RDFxml')
def parse_rdfxml(raw_record, namespaces):
    """Parse RDF/XML format records (like those from DNB)."""
    data = raw_record['data']
    record_id = raw_record.get('id', 'unknown')
    
    # Complete set of namespaces for RDF records
    ns = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'dcterms': 'http://purl.org/dc/terms/',
        'bibo': 'http://purl.org/ontology/bibo/',
        'gndo': 'https://d-nb.info/standards/elementset/gnd#',
        'marcRole': 'http://id.loc.gov/vocabulary/relators/',
        'rdau': 'http://rdaregistry.info/Elements/u/',
        'schema': 'http://schema.org/',
        'foaf': 'http://xmlns.com/foaf/0.1/',
        'owl': 'http://www.w3.org/2002/07/owl#',
        'skos': 'http://www.w3.org/2004/02/skos/core#',
        'xsd': 'http://www.w3.org/2001/XMLSchema#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'umbel': 'http://umbel.org/umbel#',
        'isbd': 'http://iflastandards.info/ns/isbd/elements/'
    }
    
    # Update with any additional namespaces from the client
    if namespaces:
        for k, v in namespaces.items():
            if k not in ns:
                ns[k] = v
    
    # Find description element
    desc = data.find('.//rdf:Description', ns)
    if desc is None:
        logger.warning(f"No RDF:Description found in record {record_id}")
        return None
    
    # Find title
    title_elem = desc.find('./dc:title', ns)
    if title_elem is None:
        title_elem = desc.find('./dcterms:title', ns)
    title = title_elem.text.strip() if title_elem is not None and title_elem.text else "Untitled"
    
    # Find alternative titles
    alt_title_elem = desc.find('./dcterms:alternative', ns)
    if alt_title_elem is not None and alt_title_elem.text:
        alt_title = alt_title_elem.text.strip()
        if alt_title:
            if ":" not in title:
                title = f"{title}: {alt_title}"
    
    # Find authors with improved handling to avoid duplicates
    authors = []
    seen_authors = set()  # Track seen authors to avoid duplicates
    
    # Function to clean author name (remove duplicate entries, trailing commas, etc.)
    def clean_author_name(name):
        # Remove trailing commas and whitespace
        name = re.sub(r',\s*$', '', name.strip())
        return name
    
    # Find authors from P60327 (author statement)
    author_statement = desc.find('./rdau:P60327', ns)
    if author_statement is not None and author_statement.text:
        # Parse author statement which may contain multiple authors
        statement_text = author_statement.text.strip()
        # Split on common separators
        potential_authors = re.split(r',\s*|\s*;\s*|\s+und\s+|\s+and\s+', statement_text)
        for author in potential_authors:
            clean_name = clean_author_name(author)
            if clean_name and clean_name not in seen_authors:
                authors.append(clean_name)
                seen_authors.add(clean_name)
    
    # Extract authors from creator elements
    for creator_path in ['./dcterms:creator', './dc:creator']:
        creator_elems = desc.findall(creator_path, ns)
        for creator_elem in creator_elems:
            # Check if it's a reference to another element
            creator_resource = creator_elem.get('{'+ns['rdf']+'}resource')
            if creator_resource:
                # Try to find the referenced resource in the document
                creator_desc = data.find(f'.//rdf:Description[@rdf:about="{creator_resource}"]', ns)
                if creator_desc is not None:
                    name_elem = creator_desc.find('./gndo:preferredName', ns)
                    if name_elem is not None and name_elem.text:
                        author_name = clean_author_name(name_elem.text)
                        if author_name not in seen_authors:
                            authors.append(author_name)
                            seen_authors.add(author_name)
                continue
                
            # If creator contains nested elements
            nested_nodes = creator_elem.findall('.//*', ns)
            for node in nested_nodes:
                if 'preferredName' in node.tag and node.text:
                    author_name = clean_author_name(node.text)
                    if author_name not in seen_authors:
                        authors.append(author_name)
                        seen_authors.add(author_name)
                    break
    
    # Extract authors from marcRole elements
    author_roles = ['aut', 'cre', 'edt']
    for role in author_roles:
        role_elems = desc.findall(f'./marcRole:{role}', ns)
        for role_elem in role_elems:
            # Check if it's a reference
            resource = role_elem.get('{'+ns['rdf']+'}resource')
            if resource:
                # Find the referenced element
                author_desc = data.find(f'.//rdf:Description[@rdf:about="{resource}"]', ns)
                if author_desc is not None:
                    name_elem = author_desc.find('./gndo:preferredName', ns)
                    if name_elem is not None and name_elem.text:
                        author_name = clean_author_name(name_elem.text)
                        if author_name not in seen_authors:
                            authors.append(author_name)
                            seen_authors.add(author_name)
                continue
                
            # Handle nested description elements
            for node_desc in role_elem.findall('./rdf:Description', ns):
                name_elem = node_desc.find('./gndo:preferredName', ns)
                if name_elem is not None and name_elem.text:
                    author_name = clean_author_name(name_elem.text)
                    if author_name not in seen_authors:
                        authors.append(author_name)
                        seen_authors.add(author_name)
                        
            # Handle node ID references (common in DNB records)
            node_id = role_elem.get('{'+ns['rdf']+'}nodeID')
            if node_id:
                node_desc = data.find(f'.//rdf:Description[@rdf:nodeID="{node_id}"]', ns)
                if node_desc is not None:
                    name_elem = node_desc.find('./gndo:preferredName', ns)
                    if name_elem is not None and name_elem.text:
                        author_name = clean_author_name(name_elem.text)
                        if author_name not in seen_authors:
                            authors.append(author_name)
                            seen_authors.add(author_name)
    
    # Find year
    year = None
    issued_elem = desc.find('./dcterms:issued', ns)
    if issued_elem is not None and issued_elem.text:
        # Extract year
        match = re.search(r'\b(1\d{3}|20\d{2})\b', issued_elem.text)
        if match:
            year = match.group(1)
    
    # Find publisher - separately handling name and place
    publisher_name = None
    publisher_elem = desc.find('./dc:publisher', ns)
    if publisher_elem is not None and publisher_elem.text:
        publisher_name = publisher_elem.text.strip()
    
    # Find place of publication
    places = []
    place_elems = desc.findall('./rdau:P60163', ns)
    for place_elem in place_elems:
        if place_elem is not None and place_elem.text and place_elem.text.strip():
            places.append(place_elem.text.strip())
    
    place_of_publication = ", ".join(places) if places else None
    
    # Check for publication statement that might have both
    pub_statement = desc.find('./rdau:P60333', ns)
    if pub_statement is not None and pub_statement.text:
        statement = pub_statement.text.strip()
        # If we don't already have separate place/publisher, try to parse from statement
        if not place_of_publication or not publisher_name:
            # Try to split on ": " which often separates place from publisher
            parts = statement.split(" : ", 1)
            if len(parts) > 1:
                if not place_of_publication:
                    place_of_publication = parts[0].strip()
                if not publisher_name:
                    # Further process publisher name to remove year
                    pub_part = parts[1].strip()
                    # Remove year in brackets at the end
                    pub_part = re.sub(r',?\s*\[\d{4}\]$', '', pub_part)
                    publisher_name = pub_part
    
    # Find edition
    edition = None
    edition_elem = desc.find('./bibo:edition', ns)
    if edition_elem is not None and edition_elem.text:
        edition = edition_elem.text.strip()
    
    # Find extent (number of pages, etc.)
    extent = None
    extent_elem = desc.find('./isbd:P1053', ns)
    if extent_elem is not None and extent_elem.text:
        extent = extent_elem.text.strip()
    
    # Find series
    series = None
    series_elem = desc.find('./dcterms:isPartOf', ns)
    citation_elem = desc.find('./dcterms:bibliographicCitation', ns)
    
    if series_elem is not None:
        # Check if it's a text value
        if series_elem.text:
            series = series_elem.text.strip()
        # Or a resource reference
        else:
            resource = series_elem.get('{'+ns['rdf']+'}resource')
            if resource:
                series_parts = resource.split('/')
                if series_parts:
                    series = series_parts[-1]
    elif citation_elem is not None and citation_elem.text:
        series = citation_elem.text.strip()
    
    # Find ISBN
    isbn = None
    for isbn_field in ['isbn13', 'isbn10', 'isbn', 'gtin14']:
        isbn_elem = desc.find(f'./bibo:{isbn_field}', ns)
        if isbn_elem is not None and isbn_elem.text:
            isbn = isbn_elem.text.strip()
            break
    
    # Find ISSN
    issn = None
    issn_elem = desc.find('./bibo:issn', ns)
    if issn_elem is not None and issn_elem.text:
        issn = issn_elem.text.strip()
    
    # Find identifiers
    identifiers = []
    id_elems = desc.findall('./dc:identifier', ns)
    for elem in id_elems:
        if elem.text and elem.text.strip():
            identifiers.append(elem.text.strip())
    
    # Find subjects
    subjects = []
    seen_subjects = set()
    # Check subject references
    subject_elems = desc.findall('./dcterms:subject', ns)
    for elem in subject_elems:
        # If it's a reference
        resource = elem.get('{'+ns['rdf']+'}resource')
        if resource:
            # Extract subject from URI
            subject = resource.split('/')[-1]
            if subject and subject not in seen_subjects:
                subjects.append(subject)
                seen_subjects.add(subject)
                continue
        
        # If it has text content
        if elem.text and elem.text.strip():
            subject = elem.text.strip()
            if subject not in seen_subjects:
                subjects.append(subject)
                seen_subjects.add(subject)
    
    # Also check dc:subject
    dc_subject_elems = desc.findall('./dc:subject', ns)
    for elem in dc_subject_elems:
        if elem.text and elem.text.strip():
            subject = elem.text.strip()
            if subject not in seen_subjects:
                subjects.append(subject)
                seen_subjects.add(subject)
    
    # Find language
    language = None
    language_elem = desc.find('./dcterms:language', ns)
    if language_elem is not None:
        # If it's a reference
        resource = language_elem.get('{'+ns['rdf']+'}resource')
        if resource:
            # Extract language code from the URI
            parts = resource.split('/')
            if parts:
                language = parts[-1]
        # If it has text content
        elif language_elem.text and language_elem.text.strip():
            language = language_elem.text.strip()
    
    # Find abstract/description
    abstract = None
    for desc_tag in ['description', 'abstract', 'P60493']:
        for ns_prefix in ['dc', 'dcterms', 'rdau']:
            desc_elem = desc.find(f'./{ns_prefix}:{desc_tag}', ns)
            if desc_elem is not None and desc_elem.text:
                abstract = desc_elem.text.strip()
                break
        if abstract:
            break
    
    # Find URLs
    urls = []
    seen_urls = set()
    
    # Check primaryTopic links
    for primaryTopic_elem in desc.findall('./foaf:primaryTopic', ns):
        resource = primaryTopic_elem.get('{'+ns['rdf']+'}resource')
        if resource and resource.startswith('http') and resource not in seen_urls:
            urls.append(resource)
            seen_urls.add(resource)
    
    # Check umbel:isLike links
    for like_elem in desc.findall('./umbel:isLike', ns):
        resource = like_elem.get('{'+ns['rdf']+'}resource')
        if resource and resource.startswith('http') and resource not in seen_urls:
            urls.append(resource)
            seen_urls.add(resource)
    
    # Find format
    format_type = None
    format_elem = desc.find('./dcterms:format', ns)
    if format_elem is not None:
        resource = format_elem.get('{'+ns['rdf']+'}resource')
        if resource:
            format_type = resource.split('/')[-1]
        elif format_elem.text:
            format_type = format_elem.text.strip()
    
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        year=year,
        publisher_name=publisher_name,
        place_of_publication=place_of_publication,
        isbn=isbn,
        issn=issn,
        urls=urls,
        abstract=abstract,
        language=language,
        format=format_type,
        subjects=subjects,
        series=series,
        extent=extent,
        edition=edition,
        raw_data=raw_record['raw_xml']
    )

# List of commonly used SRU endpoints
SRU_ENDPOINTS = {
    # National Libraries
    'dnb': {
        'name': 'Deutsche Nationalbibliothek',
        'url': 'https://services.dnb.de/sru/dnb',
        'default_schema': 'RDFxml',
        'description': 'The German National Library',
        'version': '1.1',
        'examples': {
            'title': 'TIT=Python',
            'author': 'PER=Einstein',
            'isbn': 'ISBN=9783658310844',
            'advanced': {'TIT': 'Python', 'JHR': '2023'}
        }
    },
    'bnf': {
        'name': 'Bibliothèque nationale de France',
        'url': 'http://catalogue.bnf.fr/api/SRU',
        'default_schema': 'dublincore',  # Important: changed from marcxchange
        'description': 'The French National Library',
        'version': '1.2',
        'examples': {
            'title': 'bib.title any "Python"',  # Changed from 'all' to 'any'
            'author': 'bib.author any "Einstein"',  # Changed from 'all' to 'any'
            'isbn': 'bib.isbn any "9782012919198"',
            'advanced': 'bib.title any "Python" and bib.date any "2023"'
        }
    },
    'zdb': {
        'name': 'ZDB - German Union Catalogue of Serials',
        'url': 'https://services.dnb.de/sru/zdb',
        'default_schema': 'MARC21-xml',
        'description': 'German Union Catalogue of Serials',
        'version': '1.1',
        'examples': {
            'title': 'TIT=Journal',
            'issn': 'ISS=0740-171x',
            'advanced': {'TIT': 'Journal', 'JHR': '2023'}
        }
    },
    'loc': {
        'name': 'Library of Congress',
        'url': 'https://lccn.loc.gov/sru',
        'default_schema': 'marcxml',
        'description': 'Library of Congress catalog',
        'version': '1.1',
        'examples': {
            'title': 'title="Python"',
            'author': 'author="Einstein"',
            'isbn': 'isbn=9781234567890',
            'advanced': 'title="Python" and author="Rossum"'
        }
    },
    
    # Other libraries and collections
    'trove': {
        'name': 'Trove (National Library of Australia)',
        'url': 'http://www.nla.gov.au/apps/srw/search/peopleaustralia',
        'default_schema': 'dc',
        'description': 'Australia\'s cultural collections',
        'version': '1.1',
        'examples': {
            'name': 'bath.name="Smith"',
            'advanced': 'pa.surname="Smith" and pa.firstname="John"'
        }
    },
    'kb': {
        'name': 'KB - National Library of the Netherlands',
        'url': 'http://jsru.kb.nl/sru',
        'default_schema': 'dc',
        'description': 'Dutch National Library',
        'version': '1.1',
        'examples': {
            'title': 'dc.title=Python',
            'advanced': 'dc.title=Python and dc.date=2023'
        }
    },
    'bibsys': {
        'name': 'BIBSYS - Norwegian Library Service',
        'url': 'http://sru.bibsys.no/search/biblio',
        'default_schema': 'dc',
        'description': 'Norwegian academic libraries',
        'version': '1.1',
        'examples': {
            'title': 'title="Python"',
            'author': 'author="Einstein"',
            'advanced': 'title="Python" and date="2023"'
        }
    }
}