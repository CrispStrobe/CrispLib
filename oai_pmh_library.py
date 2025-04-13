#!/usr/bin/env python3
# oai_pmh_library.py
"""
OAI-PMH Library - A flexible OAI-PMH (Open Archives Initiative Protocol for Metadata Harvesting) client
for bibliographic data

This module provides a modular approach to query various library OAI-PMH endpoints
without requiring hardcoded classes for each specific library.
"""

import requests
import xml.etree.ElementTree as ET
import logging
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union, Tuple, Callable, Generator
import re

# Try to import Sickle for enhanced OAI-PMH support
try:
    from sickle import Sickle
    from sickle.iterator import OAIResponseIterator
    from sickle.oaiexceptions import NoRecordsMatch, BadArgument
    SICKLE_AVAILABLE = True
except ImportError:
    SICKLE_AVAILABLE = False
    
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("oai_pmh_library")

from sru_library import BiblioRecord

class OAIClient:
    """
    A flexible OAI-PMH client that can work with any OAI-PMH endpoint.
    """
    
    # Registry of metadata format parsers
    parsers = {}
    
    @classmethod
    def register_parser(cls, metadata_prefix):
        """Decorator to register a parser function for a specific metadata format."""
        def decorator(parser_func):
            cls.parsers[metadata_prefix] = parser_func
            return parser_func
        return decorator
    
    def __init__(self, 
                base_url: str,
                default_metadata_prefix: str = "oai_dc",
                namespaces: Dict[str, str] = None,
                timeout: int = 30, 
                record_parser: Optional[Callable] = None,
                use_sickle: bool = True):
        """
        Initialize OAI-PMH client.
        
        Args:
            base_url: The base URL of the OAI-PMH repository
            default_metadata_prefix: Default metadata format to request
            namespaces: Additional XML namespaces for parsing
            timeout: Request timeout in seconds
            record_parser: Custom parser function for records
            use_sickle: Whether to use Sickle library when available
        """
        self.base_url = base_url
        self.timeout = timeout
        self.default_metadata_prefix = default_metadata_prefix
        self.custom_parser = record_parser
        self.use_sickle = use_sickle and SICKLE_AVAILABLE
        
        # Comprehensive set of namespaces for different record formats
        self.namespaces = {
            # OAI namespaces
            'oai': 'http://www.openarchives.org/OAI/2.0/',
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
            
            # Dublin Core
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcterms': 'http://purl.org/dc/terms/',
            
            # MARC
            'marc': 'http://www.loc.gov/MARC21/slim',
            'marc21': 'http://www.loc.gov/MARC21/slim',
            
            # MODS
            'mods': 'http://www.loc.gov/mods/v3',
            
            # XML Schema
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
            
            # RDF and related vocabularies
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            
            # Other common namespaces
            'xlink': 'http://www.w3.org/1999/xlink',
            'mets': 'http://www.loc.gov/METS/',
        }
        
        # Update with provided namespaces
        if namespaces:
            self.namespaces.update(namespaces)
            
        # Initialize Sickle if available and requested
        self.sickle = None
        if self.use_sickle:
            self.sickle = Sickle(self.base_url, timeout=self.timeout)
            logger.debug(f"Using Sickle for OAI-PMH access at {self.base_url}")
    
    def identify(self) -> Dict[str, Any]:
        """
        Get repository information using the Identify verb.
        
        Returns:
            Dictionary of repository information
        """
        try:
            if self.use_sickle:
                identify = self.sickle.Identify()
                return {
                    'repositoryName': identify.repositoryName,
                    'baseURL': identify.baseURL,
                    'protocolVersion': identify.protocolVersion,
                    'adminEmail': identify.adminEmail,
                    'earliestDatestamp': identify.earliestDatestamp,
                    'deletedRecord': identify.deletedRecord,
                    'granularity': identify.granularity
                }
            else:
                url = f"{self.base_url}?verb=Identify"
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                root = ET.fromstring(response.content)
                
                # Check for error
                error = root.find('.//oai:error', self.namespaces)
                if error is not None:
                    code = error.get('code', 'unknown')
                    message = error.text or 'Unknown error'
                    logger.error(f"OAI-PMH error ({code}): {message}")
                    return {'error': {'code': code, 'message': message}}
                
                # Extract repository information
                info = {}
                elements = [
                    'repositoryName', 'baseURL', 'protocolVersion', 
                    'adminEmail', 'earliestDatestamp', 'deletedRecord',
                    'granularity'
                ]
                
                for element in elements:
                    elem = root.find(f'.//oai:{element}', self.namespaces)
                    if elem is not None and elem.text:
                        info[element] = elem.text
                
                return info
                
        except Exception as e:
            logger.error(f"Error in Identify: {e}")
            return {'error': str(e)}
    
    def list_sets(self) -> List[Dict[str, str]]:
        """
        List available sets in the repository.
        
        Returns:
            List of dictionaries with set information
        """
        try:
            if self.use_sickle:
                sets = []
                for s in self.sickle.ListSets():
                    sets.append({
                        'setSpec': s.setSpec,
                        'setName': s.setName,
                        'setDescription': getattr(s, 'setDescription', '')
                    })
                return sets
            else:
                url = f"{self.base_url}?verb=ListSets"
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                root = ET.fromstring(response.content)
                
                # Check for error
                error = root.find('.//oai:error', self.namespaces)
                if error is not None:
                    code = error.get('code', 'unknown')
                    message = error.text or 'Unknown error'
                    logger.error(f"OAI-PMH error ({code}): {message}")
                    if code == 'noSetHierarchy':
                        return []
                    return [{'error': {'code': code, 'message': message}}]
                
                # Extract sets
                sets = []
                set_elements = root.findall('.//oai:set', self.namespaces)
                
                for set_elem in set_elements:
                    set_spec = set_elem.find('./oai:setSpec', self.namespaces)
                    set_name = set_elem.find('./oai:setName', self.namespaces)
                    set_desc = set_elem.find('./oai:setDescription', self.namespaces)
                    
                    if set_spec is not None:
                        set_info = {
                            'setSpec': set_spec.text,
                            'setName': set_name.text if set_name is not None else '',
                            'setDescription': ET.tostring(set_desc).decode('utf-8') if set_desc is not None else ''
                        }
                        sets.append(set_info)
                
                return sets
                
        except Exception as e:
            logger.error(f"Error in ListSets: {e}")
            return [{'error': str(e)}]
    
    def list_metadata_formats(self, identifier: str = None) -> List[Dict[str, str]]:
        """
        List available metadata formats.
        
        Args:
            identifier: Optional record identifier to check formats for specific record
            
        Returns:
            List of dictionaries with metadata format information
        """
        try:
            params = {'verb': 'ListMetadataFormats'}
            if identifier:
                params['identifier'] = identifier
                
            if self.use_sickle:
                formats = []
                for fmt in self.sickle.ListMetadataFormats(identifier=identifier):
                    formats.append({
                        'metadataPrefix': fmt.metadataPrefix,
                        'schema': fmt.schema,
                        'metadataNamespace': fmt.metadataNamespace
                    })
                return formats
            else:
                url = f"{self.base_url}?{self._build_query_string(params)}"
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                root = ET.fromstring(response.content)
                
                # Check for error
                error = root.find('.//oai:error', self.namespaces)
                if error is not None:
                    code = error.get('code', 'unknown')
                    message = error.text or 'Unknown error'
                    logger.error(f"OAI-PMH error ({code}): {message}")
                    return [{'error': {'code': code, 'message': message}}]
                
                # Extract metadata formats
                formats = []
                format_elements = root.findall('.//oai:metadataFormat', self.namespaces)
                
                for format_elem in format_elements:
                    prefix = format_elem.find('./oai:metadataPrefix', self.namespaces)
                    schema = format_elem.find('./oai:schema', self.namespaces)
                    namespace = format_elem.find('./oai:metadataNamespace', self.namespaces)
                    
                    if prefix is not None:
                        format_info = {
                            'metadataPrefix': prefix.text,
                            'schema': schema.text if schema is not None else '',
                            'metadataNamespace': namespace.text if namespace is not None else ''
                        }
                        formats.append(format_info)
                
                return formats
                
        except Exception as e:
            logger.error(f"Error in ListMetadataFormats: {e}")
            return [{'error': str(e)}]
    
    def _build_query_string(self, params: Dict[str, str]) -> str:
        """Build a URL query string from parameters."""
        return "&".join([f"{k}={v}" for k, v in params.items()])
    
    def _follow_resumption_token(self, url_with_token, max_requests=10):
        """Follow resumption tokens to get all results.
        
        Args:
            url_with_token: URL including the resumption token
            max_requests: Maximum number of requests to make (to prevent infinite loops)
            
        Returns:
            List of all elements collected
        """
        all_results = []
        request_count = 0
        
        while url_with_token and request_count < max_requests:
            try:
                response = requests.get(url_with_token, timeout=self.timeout)
                response.raise_for_status()
                
                root = ET.fromstring(response.content)
                
                # Extract results (will be different based on the verb)
                if 'verb=ListSets' in url_with_token:
                    results = root.findall('.//oai:set', self.namespaces)
                elif 'verb=ListIdentifiers' in url_with_token:
                    results = root.findall('.//oai:header', self.namespaces)
                elif 'verb=ListRecords' in url_with_token:
                    results = root.findall('.//oai:record', self.namespaces)
                else:
                    results = []
                
                all_results.extend(results)
                
                # Check for resumption token
                token_elem = root.find('.//oai:resumptionToken', self.namespaces)
                if token_elem is not None and token_elem.text:
                    # Extract the base URL without parameters
                    base_url = url_with_token.split('?')[0]
                    url_with_token = f"{base_url}?verb={url_with_token.split('verb=')[1].split('&')[0]}&resumptionToken={token_elem.text}"
                else:
                    url_with_token = None
                
                request_count += 1
                
            except Exception as e:
                logger.error(f"Error following resumption token: {e}")
                url_with_token = None
        
        return all_results
    
    def list_identifiers(self, 
                        metadata_prefix: str = None,
                        set_spec: str = None,
                        from_date: str = None,
                        until_date: str = None,
                        max_results: int = None) -> List[Dict[str, str]]:
        """
        List record identifiers with optional filtering.
        
        Args:
            metadata_prefix: Metadata format
            set_spec: Optional set for filtering
            from_date: Optional start date (YYYY-MM-DD)
            until_date: Optional end date (YYYY-MM-DD)
            max_results: Maximum number of results to return
            
        Returns:
            List of dictionaries with identifier information
        """
        try:
            metadata_prefix = metadata_prefix or self.default_metadata_prefix
            
            # Build parameters
            params = {
                'verb': 'ListIdentifiers',
                'metadataPrefix': metadata_prefix
            }
            
            if set_spec:
                params['set'] = set_spec
            if from_date:
                params['from'] = from_date
            if until_date:
                params['until'] = until_date
                
            if self.use_sickle:
                identifiers = []
                sickle_params = {k: v for k, v in params.items() if k != 'verb'}
                
                # Handle date parameters correctly for Sickle
                if 'from' in sickle_params:
                    sickle_params['from'] = sickle_params.pop('from')
                
                for header in self.sickle.ListIdentifiers(**sickle_params):
                    identifiers.append({
                        'identifier': header.identifier,
                        'datestamp': header.datestamp,
                        'setSpec': header.setSpec
                    })
                    
                    if max_results and len(identifiers) >= max_results:
                        break
                        
                return identifiers
            else:
                url = f"{self.base_url}?{self._build_query_string(params)}"
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                root = ET.fromstring(response.content)
                
                # Check for error
                error = root.find('.//oai:error', self.namespaces)
                if error is not None:
                    code = error.get('code', 'unknown')
                    message = error.text or 'Unknown error'
                    logger.error(f"OAI-PMH error ({code}): {message}")
                    if code == 'noRecordsMatch':
                        return []
                    return [{'error': {'code': code, 'message': message}}]
                
                # Extract identifiers
                identifiers = []
                header_elements = root.findall('.//oai:header', self.namespaces)
                
                for i, header_elem in enumerate(header_elements):
                    if max_results and i >= max_results:
                        break
                        
                    identifier = header_elem.find('./oai:identifier', self.namespaces)
                    datestamp = header_elem.find('./oai:datestamp', self.namespaces)
                    
                    if identifier is not None:
                        # Get setSpec elements (can be multiple)
                        sets = []
                        for set_elem in header_elem.findall('./oai:setSpec', self.namespaces):
                            if set_elem.text:
                                sets.append(set_elem.text)
                        
                        header_info = {
                            'identifier': identifier.text,
                            'datestamp': datestamp.text if datestamp is not None else '',
                            'setSpec': sets
                        }
                        identifiers.append(header_info)
                
                # Check for resumption token for handling resumption
                # This is a simplified implementation; a full one would follow the token
                resumption_token = root.find('.//oai:resumptionToken', self.namespaces)
                if resumption_token is not None and resumption_token.text:
                    logger.info(f"More results available with resumptionToken: {resumption_token.text}")
                    
                return identifiers
                
        except Exception as e:
            logger.error(f"Error in ListIdentifiers: {e}")
            return [{'error': str(e)}]
    
    def get_record(self, 
                  identifier: str,
                  metadata_prefix: str = None) -> Optional[Dict[str, Any]]:
        """
        Get a specific record by identifier.
        
        Args:
            identifier: Record identifier
            metadata_prefix: Metadata format
            
        Returns:
            Record data dictionary or None if not found
        """
        try:
            metadata_prefix = metadata_prefix or self.default_metadata_prefix
            
            params = {
                'verb': 'GetRecord',
                'identifier': identifier,
                'metadataPrefix': metadata_prefix
            }
            
            if self.use_sickle:
                record = self.sickle.GetRecord(identifier=identifier, metadataPrefix=metadata_prefix)
                return self._process_sickle_record(record)
            else:
                url = f"{self.base_url}?{self._build_query_string(params)}"
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                root = ET.fromstring(response.content)
                
                # Check for error
                error = root.find('.//oai:error', self.namespaces)
                if error is not None:
                    code = error.get('code', 'unknown')
                    message = error.text or 'Unknown error'
                    logger.error(f"OAI-PMH error ({code}): {message}")
                    return None
                
                # Extract record
                record_elem = root.find('.//oai:record', self.namespaces)
                if record_elem is None:
                    logger.warning(f"No record found for identifier {identifier}")
                    return None
                    
                return self._process_record_element(record_elem, metadata_prefix)
                
        except Exception as e:
            logger.error(f"Error in GetRecord: {e}")
            return None
    
    def _follow_resumption_token_with_records(self, url_with_token, max_results=None, max_requests=10):
        """Follow resumption tokens to get all records as BiblioRecord objects.
        
        Args:
            url_with_token: URL including the resumption token
            max_results: Maximum number of results to return
            max_requests: Maximum number of requests to make
            
        Returns:
            List of BiblioRecord objects
        """
        all_records = []
        request_count = 0
        
        while url_with_token and request_count < max_requests:
            if max_results and len(all_records) >= max_results:
                break
                
            try:
                response = requests.get(url_with_token, timeout=self.timeout)
                response.raise_for_status()
                
                root = ET.fromstring(response.content)
                
                # Extract records
                record_elements = root.findall('.//oai:record', self.namespaces)
                metadata_prefix = self.default_metadata_prefix
                
                for i, record_elem in enumerate(record_elements):
                    if max_results and len(all_records) >= max_results:
                        break
                        
                    record = self._process_record_element(record_elem, metadata_prefix)
                    if record:
                        biblio_record = self._to_biblio_record(record, metadata_prefix)
                        if biblio_record:
                            all_records.append(biblio_record)
                
                # Check for next resumption token
                token_elem = root.find('.//oai:resumptionToken', self.namespaces)
                if token_elem is not None and token_elem.text:
                    # Extract the base URL without parameters
                    base_url = url_with_token.split('?')[0]
                    url_with_token = f"{base_url}?verb=ListRecords&resumptionToken={token_elem.text}"
                else:
                    url_with_token = None
                
                request_count += 1
                
            except Exception as e:
                logger.error(f"Error following resumption token: {e}")
                url_with_token = None
        
        return all_records


    def list_records(self, 
                    metadata_prefix: str = None,
                    set_spec: str = None,
                    from_date: str = None,
                    until_date: str = None,
                    max_results: int = None) -> Tuple[int, List[BiblioRecord]]:
        """
        List records with optional filtering.
        
        Args:
            metadata_prefix: Metadata format
            set_spec: Optional set for filtering
            from_date: Optional start date (YYYY-MM-DD)
            until_date: Optional end date (YYYY-MM-DD)
            max_results: Maximum number of results to return
            
        Returns:
            Tuple of (count, list of BiblioRecord objects)
        """
        try:
            metadata_prefix = metadata_prefix or self.default_metadata_prefix
            
            # Check if we need to ensure date ranges
            if (from_date and not until_date) or (until_date and not from_date):
                # Some repositories (like DNB) require both from and until
                if 'dnb' in self.base_url.lower():
                    if not until_date:
                        until_date = datetime.now().strftime('%Y-%m-%d')
                    if not from_date:
                        from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                    logger.info(f"Added missing date bound for DNB: from={from_date}, until={until_date}")
            
            # If both dates are provided, check if we need to split the range to avoid 413 errors
            date_chunks = [(from_date, until_date)]
            if from_date and until_date:
                try:
                    start = datetime.strptime(from_date, '%Y-%m-%d')
                    end = datetime.strptime(until_date, '%Y-%m-%d')
                    
                    # If range is more than 60 days for certain endpoints, split it
                    if (end - start).days > 60 and ('dnb' in self.base_url.lower() or
                                                'deutsche-digitale-bibliothek' in self.base_url.lower()):
                        date_chunks = self._split_date_range(from_date, until_date, interval_days=30)
                        logger.info(f"Split date range into {len(date_chunks)} chunks to avoid 413 errors")
                except ValueError:
                    # If we can't parse dates, use as is
                    pass
            
            all_records = []
            total_count = 0
            
            # Process each date chunk
            for chunk_from, chunk_until in date_chunks:
                # Stop if we've reached max_results
                if max_results and len(all_records) >= max_results:
                    break
                
                # Calculate how many more results we need if max_results is specified
                chunk_max = None
                if max_results:
                    chunk_max = max_results - len(all_records)
                
                # Build parameters
                params = {
                    'verb': 'ListRecords',
                    'metadataPrefix': metadata_prefix
                }
                
                if set_spec:
                    params['set'] = set_spec
                if chunk_from:
                    params['from'] = chunk_from
                if chunk_until:
                    params['until'] = chunk_until
                    
                records = []
                chunk_count = 0
                
                if self.use_sickle:
                    sickle_params = {k: v for k, v in params.items() if k != 'verb'}
                    
                    try:
                        for oai_record in self.sickle.ListRecords(**sickle_params):
                            chunk_count += 1
                            record = self._process_sickle_record(oai_record)
                            
                            if record:
                                # Parse into BiblioRecord
                                biblio_record = self._to_biblio_record(record, metadata_prefix)
                                if biblio_record:
                                    records.append(biblio_record)
                            
                            if chunk_max and len(records) >= chunk_max:
                                break
                                
                    except NoRecordsMatch:
                        logger.info(f"No records match for chunk {chunk_from} to {chunk_until}")
                    except BadArgument as e:
                        logger.warning(f"Bad argument for chunk {chunk_from} to {chunk_until}: {e}")
                    except Exception as e:
                        logger.error(f"Error retrieving records for chunk {chunk_from} to {chunk_until}: {e}")
                        # Try with a smaller chunk if possible
                        if (chunk_from and chunk_until and 
                                datetime.strptime(chunk_until, '%Y-%m-%d') > 
                                datetime.strptime(chunk_from, '%Y-%m-%d') + timedelta(days=5)):
                            # Split this chunk further and retry
                            sub_chunks = self._split_date_range(chunk_from, chunk_until, interval_days=5)
                            for sub_from, sub_until in sub_chunks:
                                try:
                                    sub_params = sickle_params.copy()
                                    sub_params['from'] = sub_from
                                    sub_params['until'] = sub_until
                                    
                                    for oai_record in self.sickle.ListRecords(**sub_params):
                                        chunk_count += 1
                                        record = self._process_sickle_record(oai_record)
                                        
                                        if record:
                                            biblio_record = self._to_biblio_record(record, metadata_prefix)
                                            if biblio_record:
                                                records.append(biblio_record)
                                        
                                        if chunk_max and len(records) >= chunk_max:
                                            break
                                except Exception as sub_e:
                                    logger.error(f"Error in sub-chunk {sub_from} to {sub_until}: {sub_e}")
                else:
                    url = f"{self.base_url}?{self._build_query_string(params)}"
                    
                    try:
                        response = requests.get(url, timeout=self.timeout)
                        response.raise_for_status()
                        
                        root = ET.fromstring(response.content)
                        
                        # Check for error
                        error = root.find('.//oai:error', self.namespaces)
                        if error is not None:
                            code = error.get('code', 'unknown')
                            message = error.text or 'Unknown error'
                            logger.warning(f"OAI-PMH error ({code}): {message} for chunk {chunk_from} to {chunk_until}")
                            
                            if code == 'noRecordsMatch':
                                continue  # Try next chunk
                            elif code == 'badArgument':
                                # Try with default dates if dates might be the issue
                                if chunk_from or chunk_until:
                                    # Retry without date parameters
                                    new_params = params.copy()
                                    if 'from' in new_params:
                                        del new_params['from']
                                    if 'until' in new_params:
                                        del new_params['until']
                                    
                                    url = f"{self.base_url}?{self._build_query_string(new_params)}"
                                    try:
                                        response = requests.get(url, timeout=self.timeout)
                                        response.raise_for_status()
                                        root = ET.fromstring(response.content)
                                    except Exception:
                                        continue  # Try next chunk
                                else:
                                    continue  # Try next chunk
                        
                        # Extract records
                        record_elements = root.findall('.//oai:record', self.namespaces)
                        chunk_count = len(record_elements)
                        
                        for i, record_elem in enumerate(record_elements):
                            if chunk_max and i >= chunk_max:
                                break
                                
                            record = self._process_record_element(record_elem, metadata_prefix)
                            if record:
                                biblio_record = self._to_biblio_record(record, metadata_prefix)
                                if biblio_record:
                                    records.append(biblio_record)
                        
                        # Check for resumption token
                        resumption_token = root.find('.//oai:resumptionToken', self.namespaces)
                        if resumption_token is not None and resumption_token.text and not (chunk_max and len(records) >= chunk_max):
                            logger.info(f"Following resumption token for chunk {chunk_from} to {chunk_until}")
                            
                            # Get completeListSize if available
                            if 'completeListSize' in resumption_token.attrib:
                                try:
                                    chunk_count = int(resumption_token.attrib['completeListSize'])
                                except (ValueError, TypeError):
                                    pass
                            
                            # Follow the resumption token
                            token_url = f"{self.base_url}?verb=ListRecords&resumptionToken={resumption_token.text}"
                            more_records = self._follow_resumption_token_with_records(token_url, chunk_max - len(records) if chunk_max else None)
                            
                            for record in more_records:
                                if record:
                                    records.append(record)
                                
                                if chunk_max and len(records) >= chunk_max:
                                    break
                    
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 413:  # Request Entity Too Large
                            logger.warning(f"Request too large for chunk {chunk_from} to {chunk_until}")
                            
                            # Try with a smaller chunk if possible
                            if (chunk_from and chunk_until and 
                                    datetime.strptime(chunk_until, '%Y-%m-%d') > 
                                    datetime.strptime(chunk_from, '%Y-%m-%d') + timedelta(days=5)):
                                # Split this chunk further and retry
                                sub_chunks = self._split_date_range(chunk_from, chunk_until, interval_days=5)
                                for sub_from, sub_until in sub_chunks:
                                    sub_params = params.copy()
                                    sub_params['from'] = sub_from
                                    sub_params['until'] = sub_until
                                    
                                    sub_url = f"{self.base_url}?{self._build_query_string(sub_params)}"
                                    try:
                                        sub_response = requests.get(sub_url, timeout=self.timeout)
                                        sub_response.raise_for_status()
                                        
                                        sub_root = ET.fromstring(sub_response.content)
                                        sub_record_elements = sub_root.findall('.//oai:record', self.namespaces)
                                        
                                        for i, record_elem in enumerate(sub_record_elements):
                                            record = self._process_record_element(record_elem, metadata_prefix)
                                            if record:
                                                biblio_record = self._to_biblio_record(record, metadata_prefix)
                                                if biblio_record:
                                                    records.append(biblio_record)
                                            
                                            if chunk_max and len(records) >= chunk_max:
                                                break
                                    except Exception as sub_e:
                                        logger.error(f"Error in sub-chunk {sub_from} to {sub_until}: {sub_e}")
                        else:
                            logger.error(f"HTTP error for chunk {chunk_from} to {chunk_until}: {e}")
                    except Exception as e:
                        logger.error(f"Error retrieving records for chunk {chunk_from} to {chunk_until}: {e}")
                
                # Add records from this chunk to the total
                all_records.extend(records)
                total_count += chunk_count
                
                logger.info(f"Retrieved {len(records)} records for chunk {chunk_from} to {chunk_until}")
            
            return total_count, all_records
                
        except Exception as e:
            logger.error(f"Error in ListRecords: {e}")
            return 0, []
    
    # search method for OAI-PMH client that handles DNB specific requirements
    # with chunk processing
    def search(self, query: Dict[str, str] = None,
            metadata_prefix: str = None,
            set_spec: str = None,
            from_date: str = None,
            until_date: str = None,
            max_results: int = 20) -> Tuple[int, List[BiblioRecord]]:
        """
        Search records using the provided criteria.
        
        Args:
            query: Dictionary mapping field names to search terms
            metadata_prefix: Metadata format
            set_spec: Optional set for filtering
            from_date: Optional start date (YYYY-MM-DD)
            until_date: Optional end date (YYYY-MM-DD)
            max_results: Maximum number of results to return
            
        Returns:
            Tuple of (count, list of BiblioRecord objects)
        """
        # OAI-PMH doesn't support direct searching, so we use filtering and post-process
        logger.debug(f"Searching with query: {query}, set: {set_spec}, from: {from_date}, until: {until_date}")
        
        # Special handling for Deutsche Nationalbibliothek
        is_dnb = 'dnb' in self.base_url.lower()
        
        # Handle repositories that require both from and until dates
        if is_dnb:
            # For DNB, if we have a search query but no dates, use a reasonable date range
            if query and (not from_date or not until_date):
                if not until_date:
                    until_date = datetime.now().strftime('%Y-%m-%d')
                    logger.info(f"Added missing until date for DNB search: {until_date}")
                if not from_date:
                    # Set from_date to 5 years before until_date for better coverage
                    try:
                        until_dt = datetime.strptime(until_date, '%Y-%m-%d')
                        from_date = (until_dt - timedelta(days=365*5)).strftime('%Y-%m-%d')
                    except ValueError:
                        from_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
                    logger.info(f"Added missing from date for DNB search: {from_date}")
            
            # If from_date is still missing but until_date is set, handle that case too
            elif from_date and not until_date:
                until_date = datetime.now().strftime('%Y-%m-%d')
                logger.info(f"Added missing until date for DNB: {until_date}")
            elif until_date and not from_date:
                # Set from_date to 1 year before until_date
                try:
                    until_dt = datetime.strptime(until_date, '%Y-%m-%d')
                    from_date = (until_dt - timedelta(days=365)).strftime('%Y-%m-%d')
                except ValueError:
                    from_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
                logger.info(f"Added missing from date for DNB: {from_date}")
        
        # Get records within specified date range
        all_records = []
        filtered_records = []  # Will hold records that match the query
        total_count = 0
        
        if is_dnb and query:
            # For DNB, better to search in smaller chunks of time to avoid 413 errors
            # Split the date range into several chunks
            try:
                start_date = datetime.strptime(from_date, '%Y-%m-%d')
                end_date = datetime.strptime(until_date, '%Y-%m-%d')
                
                # For long periods, split by years instead of months
                if (end_date - start_date).days > 365:
                    chunks = []
                    current = start_date
                    while current < end_date:
                        next_chunk = min(datetime(current.year + 1, 1, 1), end_date)
                        chunks.append((
                            current.strftime('%Y-%m-%d'),
                            next_chunk.strftime('%Y-%m-%d')
                        ))
                        current = next_chunk + timedelta(days=1)
                        if current > end_date:
                            break
                else:
                    # Otherwise use the default chunking
                    chunks = self._split_date_range(from_date, until_date, interval_days=30)
                
                logger.info(f"Searching across {len(chunks)} time chunks")
                
                # Search each time period and filter as we go
                for chunk_index, (chunk_start, chunk_end) in enumerate(chunks):
                    logger.info(f"Processing chunk {chunk_index+1}/{len(chunks)}: {chunk_start} to {chunk_end}")
                    
                    chunk_count, chunk_records = self.list_records(
                        metadata_prefix=metadata_prefix,
                        set_spec=set_spec,
                        from_date=chunk_start,
                        until_date=chunk_end,
                        max_results=100  # Get a good number per chunk
                    )
                    
                    total_count += chunk_count
                    
                    # Filter this chunk immediately
                    for record in chunk_records:
                        if self._record_matches_query(record, query):
                            filtered_records.append(record)
                    
                    logger.info(f"Found {len(filtered_records)} matching records so far")
                    
                    # Stop if we've found enough matching records
                    if len(filtered_records) >= max_results:
                        logger.info(f"Reached desired number of results ({max_results}), stopping search")
                        break
                        
            except Exception as e:
                logger.error(f"Error when chunking dates: {e}")
                # Fall back to standard search
                total_count, all_records = self.list_records(
                    metadata_prefix=metadata_prefix,
                    set_spec=set_spec,
                    from_date=from_date,
                    until_date=until_date,
                    max_results=100
                )
                
                # Filter records from the standard search
                for record in all_records:
                    if self._record_matches_query(record, query):
                        filtered_records.append(record)
        else:
            # For other repositories, use the standard approach
            total_count, all_records = self.list_records(
                metadata_prefix=metadata_prefix,
                set_spec=set_spec,
                from_date=from_date,
                until_date=until_date,
                max_results=100
            )
            
            # If there's a query, filter the records
            if query:
                for record in all_records:
                    if self._record_matches_query(record, query):
                        filtered_records.append(record)
            else:
                # No query, so all records match
                filtered_records = all_records
        
        # Return the matching records, up to max_results
        actual_count = len(filtered_records)
        return actual_count, filtered_records[:max_results]

    def _record_matches_query(self, record, query):
        """
        Check if a record matches the given query.
        
        Args:
            record: BiblioRecord to check
            query: Dictionary of field name to search term
            
        Returns:
            True if the record matches all query terms, False otherwise
        """
        # Check each search field
        for field, term in query.items():
            term_lower = term.lower()
            term_words = term_lower.split()
            
            if field.lower() == 'title' and record.title:
                title_lower = record.title.lower()
                
                # Try both exact substring match and word-by-word match
                if term_lower not in title_lower:
                    # Check if all words in the term appear in the title
                    word_match = all(word in title_lower for word in term_words)
                    if not word_match:
                        return False
            
            elif field.lower() == 'author' and record.authors:
                author_match = False
                for author in record.authors:
                    author_lower = author.lower()
                    
                    # Check for exact substring or all words match
                    if term_lower in author_lower:
                        author_match = True
                        break
                    
                    # Try word-by-word match
                    if all(word in author_lower for word in term_words):
                        author_match = True
                        break
                
                if not author_match:
                    # Also check editors and translators
                    if record.editors and any(term_lower in editor.lower() or 
                                            all(word in editor.lower() for word in term_words) 
                                            for editor in record.editors):
                        # Found in editors, consider it a match
                        pass
                    elif record.translators and any(term_lower in translator.lower() or 
                                                all(word in translator.lower() for word in term_words) 
                                                for translator in record.translators):
                        # Found in translators, consider it a match
                        pass
                    else:
                        return False
            
            elif field.lower() == 'subject' and record.subjects:
                subject_match = False
                for subject in record.subjects:
                    subject_lower = subject.lower()
                    
                    if term_lower in subject_lower:
                        subject_match = True
                        break
                    
                    # Try word-by-word match
                    if all(word in subject_lower for word in term_words):
                        subject_match = True
                        break
                
                if not subject_match:
                    return False
            
            elif field.lower() == 'year' and record.year:
                if term != record.year:
                    return False
            
            elif field.lower() == 'isbn' and record.isbn:
                # Clean ISBN for comparison
                r_isbn = re.sub(r'[^0-9X]', '', record.isbn)
                s_isbn = re.sub(r'[^0-9X]', '', term)
                if s_isbn not in r_isbn:
                    return False
            
            elif field.lower() == 'issn' and record.issn:
                # Clean ISSN for comparison
                r_issn = re.sub(r'[^0-9X]', '', record.issn)
                s_issn = re.sub(r'[^0-9X]', '', term)
                if s_issn not in r_issn:
                    return False
            
            # If we're searching any other field that we don't know how to handle,
            # and we haven't matched in known fields, consider it a non-match
            elif field.lower() not in ['title', 'author', 'subject', 'year', 'isbn', 'issn']:
                # Try checking in raw data as a last resort
                if record.raw_data and term_lower in record.raw_data.lower():
                    # Found in raw data, consider it a match
                    pass
                else:
                    return False
        
        # If we got here, the record matches all query terms
        return True
    
    def _process_sickle_record(self, sickle_record) -> Dict[str, Any]:
        """Process a record returned by Sickle."""
        try:
            # Extract basic information
            record = {
                'identifier': sickle_record.header.identifier,
                'datestamp': sickle_record.header.datestamp,
                'sets': getattr(sickle_record.header, 'setSpec', []),
                # Use the default metadata prefix instead of trying to access it from the record
                'metadata_prefix': self.default_metadata_prefix,
                'raw': sickle_record.raw
            }
            
            # Parse XML for metadata
            root = ET.fromstring(sickle_record.raw)
            metadata_elem = root.find('.//oai:metadata', self.namespaces)
            
            if metadata_elem is not None:
                record['metadata'] = ET.tostring(metadata_elem).decode('utf-8')
                
                # Add the first child of metadata which is the actual metadata element
                for child in metadata_elem:
                    record['metadata_root'] = child
                    break
            
            return record
            
        except Exception as e:
            logger.error(f"Error processing Sickle record: {e}")
            return None

    def _split_date_range(self, from_date, until_date, interval_days=30):
        """
        Split a date range into smaller chunks to avoid 413 errors.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            until_date: End date (YYYY-MM-DD)
            interval_days: Maximum days per chunk
            
        Returns:
            List of (from_date, until_date) tuples
        """
        if not from_date:
            from_date = datetime.now() - timedelta(days=365)  # Default to one year ago
            from_date = from_date.strftime('%Y-%m-%d')
        
        if not until_date:
            until_date = datetime.now().strftime('%Y-%m-%d')  # Default to today
        
        # Convert to datetime objects
        try:
            start_date = datetime.strptime(from_date, '%Y-%m-%d')
            end_date = datetime.strptime(until_date, '%Y-%m-%d')
        except ValueError:
            # Try with just year-month
            try:
                start_date = datetime.strptime(from_date, '%Y-%m')
                start_date = start_date.replace(day=1)
                end_date = datetime.strptime(until_date, '%Y-%m')
                # Set to last day of month
                next_month = end_date.replace(day=28) + timedelta(days=4)
                end_date = next_month - timedelta(days=next_month.day)
            except ValueError:
                # Try with just year
                try:
                    start_date = datetime.strptime(from_date, '%Y')
                    start_date = start_date.replace(month=1, day=1)
                    end_date = datetime.strptime(until_date, '%Y')
                    end_date = end_date.replace(month=12, day=31)
                except ValueError:
                    logger.error(f"Could not parse dates: {from_date} - {until_date}")
                    return [(from_date, until_date)]
        
        # If dates are too close, return as is
        if (end_date - start_date).days <= interval_days:
            return [(from_date, until_date)]
        
        # Split into chunks
        chunks = []
        current_date = start_date
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=interval_days), end_date)
            chunks.append((
                current_date.strftime('%Y-%m-%d'),
                chunk_end.strftime('%Y-%m-%d')
            ))
            current_date = chunk_end + timedelta(days=1)
        
        return chunks
    
    def _process_record_element(self, record_elem, metadata_prefix) -> Dict[str, Any]:
        """Process an OAI-PMH record element from XML."""
        try:
            # Extract header information
            header = record_elem.find('./oai:header', self.namespaces)
            if header is None:
                return None
                
            identifier = header.find('./oai:identifier', self.namespaces)
            datestamp = header.find('./oai:datestamp', self.namespaces)
            
            # Get setSpec elements (can be multiple)
            sets = []
            for set_elem in header.findall('./oai:setSpec', self.namespaces):
                if set_elem is not None and set_elem.text:
                    sets.append(set_elem.text)
            
            record = {
                'identifier': identifier.text if identifier is not None else 'unknown',
                'datestamp': datestamp.text if datestamp is not None else '',
                'sets': sets,
                'metadata_prefix': metadata_prefix,
                'raw': ET.tostring(record_elem).decode('utf-8')
            }
            
            # Extract metadata
            metadata_elem = record_elem.find('./oai:metadata', self.namespaces)
            if metadata_elem is not None:
                record['metadata'] = ET.tostring(metadata_elem).decode('utf-8')
                
                # Add the first child of metadata which is the actual metadata element
                for child in metadata_elem:
                    record['metadata_root'] = child
                    break
            
            return record
            
        except Exception as e:
            logger.error(f"Error processing record element: {e}")
            return None
    
    def _to_biblio_record(self, record, metadata_prefix) -> Optional[BiblioRecord]:
        """Convert OAI-PMH record to BiblioRecord."""
        try:
            # Determine which parser to use
            parser = self.custom_parser
            if not parser and metadata_prefix in self.parsers:
                parser = self.parsers[metadata_prefix]
            
            if parser:
                return parser(record, self.namespaces)
            
            # Use a generic parser as fallback
            return self._generic_parse(record, self.namespaces)
            
        except Exception as e:
            logger.error(f"Error converting to BiblioRecord: {e}")
            
            # Make a minimal record despite the error
            return BiblioRecord(
                id=record.get('identifier', 'unknown'),
                title=f"Error Processing Record {record.get('identifier', 'unknown')}",
                authors=[],
                editors=[],
                translators=[],
                raw_data=record.get('raw', '')
            )
    
    def _generic_parse(self, record, namespaces) -> BiblioRecord:
        """Generic fallback parser for OAI-PMH records."""
        # Extract metadata based on metadata prefix
        metadata_prefix = record.get('metadata_prefix', 'oai_dc')
        metadata_root = record.get('metadata_root')
        
        # Default values
        id = record.get('identifier', 'unknown')
        title = "Untitled Record"
        authors = []
        editors = []
        translators = []
        year = None
        publisher = None
        place = None
        subjects = []
        urls = []
        
        if metadata_root is not None:
            # Try to extract data using common patterns
            if metadata_prefix in ['oai_dc', 'dc']:
                # Dublin Core
                title_elem = metadata_root.find('./dc:title', namespaces)
                if title_elem is not None and title_elem.text:
                    title = title_elem.text.strip()
                
                # Authors (creators)
                for creator in metadata_root.findall('./dc:creator', namespaces):
                    if creator.text and creator.text.strip():
                        authors.append(creator.text.strip())
                
                # Date/Year
                date_elem = metadata_root.find('./dc:date', namespaces)
                if date_elem is not None and date_elem.text:
                    # Try to extract a year from date
                    year_match = re.search(r'\b(1\d{3}|20\d{2})\b', date_elem.text)
                    if year_match:
                        year = year_match.group(1)
                
                # Publisher
                publisher_elem = metadata_root.find('./dc:publisher', namespaces)
                if publisher_elem is not None and publisher_elem.text:
                    publisher = publisher_elem.text.strip()
                
                # Subjects
                for subject in metadata_root.findall('./dc:subject', namespaces):
                    if subject.text and subject.text.strip():
                        subjects.append(subject.text.strip())
                
                # Identifiers (extract URLs and ISBNs)
                isbn = None
                issn = None
                for identifier in metadata_root.findall('./dc:identifier', namespaces):
                    if identifier.text and identifier.text.strip():
                        id_text = identifier.text.strip()
                        if id_text.startswith('http'):
                            urls.append(id_text)
                        elif 'isbn' in id_text.lower():
                            isbn_match = re.search(r'(?:isbn[:\s]*)?(\d[\d\-X]+)', id_text, re.IGNORECASE)
                            if isbn_match:
                                isbn = isbn_match.group(1)
                        elif 'issn' in id_text.lower():
                            issn_match = re.search(r'(?:issn[:\s]*)?(\d[\d\-X]+)', id_text, re.IGNORECASE)
                            if issn_match:
                                issn = issn_match.group(1)
            
            elif metadata_prefix == 'mods':
                # MODS format
                title_elem = metadata_root.find('.//mods:title', namespaces)
                if title_elem is not None and title_elem.text:
                    title = title_elem.text.strip()
                
                # Authors from name elements
                for name in metadata_root.findall('.//mods:name', namespaces):
                    role = name.find('.//mods:roleTerm', namespaces)
                    name_part = name.find('.//mods:namePart', namespaces)
                    
                    # Check if it's an author/creator
                    if (role is None or 
                        (role.text and role.text.lower() in ['author', 'creator', 'aut', 'cre'])):
                        if name_part is not None and name_part.text:
                            authors.append(name_part.text.strip())
                
                # Date/Year
                date_issued = metadata_root.find('.//mods:dateIssued', namespaces)
                if date_issued is not None and date_issued.text:
                    year_match = re.search(r'\b(1\d{3}|20\d{2})\b', date_issued.text)
                    if year_match:
                        year = year_match.group(1)
                
                # Publisher
                publisher_elem = metadata_root.find('.//mods:publisher', namespaces)
                if publisher_elem is not None and publisher_elem.text:
                    publisher = publisher_elem.text.strip()
                
                # Place of publication
                place_elem = metadata_root.find('.//mods:placeTerm', namespaces)
                if place_elem is not None and place_elem.text:
                    place = place_elem.text.strip()
                
                # Subjects
                for subject in metadata_root.findall('.//mods:subject', namespaces):
                    topic = subject.find('.//mods:topic', namespaces)
                    if topic is not None and topic.text:
                        subjects.append(topic.text.strip())
                
                # Identifiers
                isbn = None
                issn = None
                for identifier in metadata_root.findall('.//mods:identifier', namespaces):
                    if identifier.get('type') == 'isbn' and identifier.text:
                        isbn = identifier.text.strip()
                    elif identifier.get('type') == 'issn' and identifier.text:
                        issn = identifier.text.strip()
                    elif identifier.get('type') == 'uri' and identifier.text:
                        urls.append(identifier.text.strip())
            
            elif metadata_prefix in ['marc', 'marc21', 'marcxml']:
                # MARC XML format
                datafields = metadata_root.findall('.//marc:datafield', namespaces)
                if not datafields:
                    datafields = metadata_root.findall('.//*[local-name()="datafield"]')
                
                for field in datafields:
                    tag = field.get('tag')
                    
                    # Title (245)
                    if tag == '245':
                        title_parts = []
                        for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                            if subfield.text:
                                title_parts.append(subfield.text.strip())
                        for subfield in field.findall('.//marc:subfield[@code="b"]', namespaces):
                            if subfield.text:
                                title_parts.append(subfield.text.strip())
                        if title_parts:
                            title = ' '.join(title_parts)
                    
                    # Authors (100, 700)
                    elif tag in ['100', '700']:
                        for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                            if subfield.text and subfield.text.strip():
                                authors.append(subfield.text.strip())
                    
                    # Publication info (260, 264)
                    elif tag in ['260', '264']:
                        for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                            if subfield.text and subfield.text.strip():
                                place = subfield.text.strip()
                        for subfield in field.findall('.//marc:subfield[@code="b"]', namespaces):
                            if subfield.text and subfield.text.strip():
                                publisher = subfield.text.strip()
                        for subfield in field.findall('.//marc:subfield[@code="c"]', namespaces):
                            if subfield.text and subfield.text.strip():
                                year_match = re.search(r'\b(1\d{3}|20\d{2})\b', subfield.text)
                                if year_match:
                                    year = year_match.group(1)
                    
                    # ISBN (020)
                    elif tag == '020':
                        for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                            if subfield.text and subfield.text.strip():
                                isbn_match = re.search(r'(\d[\d\-X]+)', subfield.text)
                                if isbn_match:
                                    isbn = isbn_match.group(1)
                    
                    # ISSN (022)
                    elif tag == '022':
                        for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                            if subfield.text and subfield.text.strip():
                                issn = subfield.text.strip()
                    
                    # Subjects (6XX)
                    elif tag.startswith('6'):
                        for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                            if subfield.text and subfield.text.strip():
                                subjects.append(subfield.text.strip())
                    
                    # URLs (856)
                    elif tag == '856':
                        for subfield in field.findall('.//marc:subfield[@code="u"]', namespaces):
                            if subfield.text and subfield.text.strip():
                                urls.append(subfield.text.strip())
        
        # Clean up title (remove trailing punctuation)
        title = re.sub(r'[/\s:.]+$', '', title).strip()
        
        # Create BiblioRecord
        return BiblioRecord(
            id=id,
            title=title,
            authors=authors,
            editors=editors,
            translators=translators,
            year=year,
            publisher_name=publisher,
            place_of_publication=place,
            isbn=isbn,
            issn=issn,
            urls=urls,
            subjects=subjects,
            raw_data=record.get('raw', '')
        )


# Register parser for Dublin Core format
@OAIClient.register_parser('oai_dc')
@OAIClient.register_parser('dc')
def parse_dublin_core(record, namespaces):
    """Parse Dublin Core format from OAI-PMH."""
    metadata_root = record.get('metadata_root')
    record_id = record.get('identifier', 'unknown')
    
    if metadata_root is None:
        return BiblioRecord(
            id=record_id,
            title=f"Unparseable Record {record_id}",
            raw_data=record.get('raw', '')
        )
    
    # Add Dublin Core namespace if not present
    ns = namespaces.copy()
    if 'dc' not in ns:
        ns['dc'] = 'http://purl.org/dc/elements/1.1/'
    
    # Find title
    title_elem = metadata_root.find('./dc:title', ns)
    title = title_elem.text.strip() if title_elem is not None and title_elem.text else "Untitled"
    
    # Clean up title - remove author and translator information
    title = re.sub(r'\s*/\s*[^/]+$', '', title)  # Remove "/ Author" at the end
    
    # Find authors (creators)
    authors = []
    editors = []
    translators = []
    
    for creator in metadata_root.findall('./dc:creator', ns):
        if creator.text and creator.text.strip():
            author_text = creator.text.strip()
            
            # Check if there are any role indicators
            if '[' in author_text and ']' in author_text:
                # Check if it's a translator
                if re.search(r'\[\s*Übersetzer\s*\]', author_text):
                    translator_name = re.sub(r'\s*\[\s*Übersetzer\s*\]', '', author_text)
                    translators.append(translator_name)
                # Check if it's an editor
                elif re.search(r'\[\s*(?:Herausgeber|Hrsg\.?|Editor|Ed\.?)\s*\]', author_text):
                    editor_name = re.sub(r'\s*\[\s*(?:Herausgeber|Hrsg\.?|Editor|Ed\.?)\s*\]', '', author_text)
                    editors.append(editor_name)
                # Check if it's a narrator
                elif re.search(r'\[\s*Erzähler\s*\]', author_text):
                    # For narrators, we'll add them as authors but clean the role
                    author_name = re.sub(r'\s*\[\s*Erzähler\s*\]', '', author_text)
                    authors.append(author_name)
                # Default case: clean the author name
                else:
                    # Clean up author text - remove role indicators
                    author_name = re.sub(r'\s*\[[^\]]*\]', '', author_text)
                    # Fix broken brackets
                    author_name = re.sub(r'\]\s*$', '', author_name)
                    author_name = re.sub(r'^\s*\[', '', author_name)
                    if author_name:
                        authors.append(author_name)
            else:
                authors.append(author_text)
    
    # Find contributors
    for contributor in metadata_root.findall('./dc:contributor', namespaces):
        if contributor.text and contributor.text.strip():
            contributor_text = contributor.text.strip()
            # Check if it's an editor
            if re.search(r'\b(editor|ed\.|hrsg|hg\.)\b', contributor_text, re.IGNORECASE) or "(ed" in contributor_text.lower():
                clean_name = re.sub(r'\s*[\(\[][^)]*(?:ed|hrsg|edit|hg)[^)]*[\)\]]', '', contributor_text)
                clean_name = re.sub(r'\s*(?:ed|hrsg|edit|hg)\.?(?:\s+|$)', '', clean_name)
                clean_name = clean_name.strip()
                
                if clean_name:
                    editors.append(clean_name)
            # Check if it's a translator
            elif re.search(r'\b(translator|trans\.|übers)\b', contributor_text, re.IGNORECASE) or "(trans" in contributor_text.lower():
                clean_name = re.sub(r'\s*[\(\[][^)]*(?:trans|übers)[^)]*[\)\]]', '', contributor_text)
                clean_name = re.sub(r'\s*(?:trans|transl|translator|übers)\.?(?:\s+|$)', '', clean_name)
                clean_name = clean_name.strip()
                
                if clean_name:
                    translators.append(clean_name)
            else:
                # Regular contributor
                authors.append(contributor_text)
    
    # Find dates
    year = None
    
    for date in metadata_root.findall('./dc:date', ns):
        if date.text and date.text.strip():
            date_text = date.text.strip()
            
            # Try to extract a year
            year_match = re.search(r'\b(1\d{3}|20\d{2})\b', date_text)
            if year_match and not year:
                year = year_match.group(1)
    
    # Find publisher
    publisher_elem = metadata_root.find('./dc:publisher', ns)
    publisher = None
    place_of_publication = None
    
    if publisher_elem is not None and publisher_elem.text:
        publisher_text = publisher_elem.text.strip()
        
        # Split place and publisher if separated by " : "
        if " : " in publisher_text:
            parts = publisher_text.split(" : ", 1)
            place_of_publication = parts[0].strip()
            publisher = parts[1].strip()
        else:
            publisher = publisher_text
    
    # Find format
    format_elem = metadata_root.find('./dc:format', ns)
    format_str = format_elem.text.strip() if format_elem is not None and format_elem.text else None
    
    # Find language
    language_elem = metadata_root.find('./dc:language', ns)
    language = language_elem.text.strip() if language_elem is not None and language_elem.text else None
    
    # Find subjects
    subjects = []
    for subject in metadata_root.findall('./dc:subject', ns):
        if subject.text and subject.text.strip():
            subjects.append(subject.text.strip())
    
    # Find description (as abstract)
    abstract = None
    desc_elem = metadata_root.find('./dc:description', ns)
    if desc_elem is not None and desc_elem.text:
        abstract = desc_elem.text.strip()
    
    # Find identifiers including ISBN, ISSN, and URLs
    isbn = None
    issn = None
    urls = []
    
    for id_elem in metadata_root.findall('./dc:identifier', ns):
        if id_elem.text and id_elem.text.strip():
            id_text = id_elem.text.strip()
            
            # Extract URLs
            if id_text.startswith('http'):
                urls.append(id_text)
            
            # Extract ISBN
            elif 'isbn' in id_text.lower():
                isbn_match = re.search(r'(?:ISBN[:\s]*)?(\d[\d\-X]+)', id_text, re.IGNORECASE)
                if isbn_match:
                    isbn = isbn_match.group(1)
            
            # Extract ISSN
            elif 'issn' in id_text.lower():
                issn_match = re.search(r'(?:issn[:\s]*)?(\d[\d\-X]+)', id_text, re.IGNORECASE)
                if issn_match:
                    issn = issn_match.group(1)
    
    # Try to find source information that might be publication data
    source_elem = metadata_root.find('./dc:source', ns)
    if source_elem is not None and source_elem.text and not place_of_publication:
        # Source might contain place of publication
        source_text = source_elem.text.strip()
        place_match = re.search(r'^([^:]+):', source_text)
        if place_match:
            place_of_publication = place_match.group(1).strip()
    
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        year=year,
        publisher_name=publisher,
        place_of_publication=place_of_publication,
        isbn=isbn,
        issn=issn,
        urls=urls,
        abstract=abstract,
        language=language,
        format=format_str,
        subjects=subjects,
        raw_data=record.get('raw', '')
    )

# Register parser for MARCXML format
@OAIClient.register_parser('marcxml')
@OAIClient.register_parser('marc21')
def parse_marcxml(record, namespaces):
    """Parse MARCXML format from OAI-PMH."""
    metadata_root = record.get('metadata_root')
    record_id = record.get('identifier', 'unknown')
    
    if metadata_root is None:
        return BiblioRecord(
            id=record_id,
            title=f"Unparseable Record {record_id}",
            raw_data=record.get('raw', '')
        )
    
    # Add MARC namespace if not present
    ns = namespaces.copy()
    if 'marc' not in ns:
        ns['marc'] = 'http://www.loc.gov/MARC21/slim'
    
    # Get all datafields
    datafields = metadata_root.findall('.//marc:datafield', ns)
    if not datafields:
        # Try without namespace
        datafields = metadata_root.findall('.//*[local-name()="datafield"]')
        if not datafields:
            return BiblioRecord(
                id=record_id,
                title=f"No MARC data found for {record_id}",
                raw_data=record.get('raw', '')
            )
    
    # Default values
    title = "Untitled"
    authors = []
    editors = []
    translators = []
    year = None
    publisher = None
    place = None
    isbn = None
    issn = None
    subjects = []
    urls = []
    abstract = None
    language = None
    series = None
    edition = None
    extent = None
    
    # Helper function to get subfield text
    def get_subfield(field, code):
        for subfield in field.findall(f'.//marc:subfield[@code="{code}"]', ns):
            if subfield.text:
                return subfield.text.strip()
        # Try without namespace
        for subfield in field.findall(f'.//*[local-name()="subfield"][@code="{code}"]'):
            if subfield.text:
                return subfield.text.strip()
        return None
    
    # Process each datafield based on its tag
    for field in datafields:
        tag = field.get('tag')
        
        # Title (245)
        if tag == '245':
            title_parts = []
            a = get_subfield(field, 'a')
            if a:
                title_parts.append(a)
            b = get_subfield(field, 'b')
            if b:
                title_parts.append(b)
            
            if title_parts:
                title = ' '.join(title_parts)
                # Clean up title (remove trailing punctuation)
                title = re.sub(r'[/\s:.]+$', '', title).strip()
        
        # Main Author (100)
        elif tag == '100':
            a = get_subfield(field, 'a')
            if a:
                # Check for role in subfield e
                e = get_subfield(field, 'e')
                if e and ('edit' in e.lower() or 'hrsg' in e.lower() or 'hg' in e.lower()):
                    editors.append(a)
                elif e and ('trans' in e.lower() or 'übers' in e.lower()):
                    translators.append(a)
                else:
                    authors.append(a)
        
        # Additional Authors/Contributors (700)
        elif tag == '700':
            a = get_subfield(field, 'a')
            if a:
                # Check for role in subfield e
                e = get_subfield(field, 'e')
                if e and ('edit' in e.lower() or 'hrsg' in e.lower() or 'hg' in e.lower()):
                    editors.append(a)
                elif e and ('trans' in e.lower() or 'übers' in e.lower()):
                    translators.append(a)
                else:
                    authors.append(a)
        
        # Publication Info (260, 264)
        elif tag in ['260', '264']:
            a = get_subfield(field, 'a')
            if a:
                place = re.sub(r'[:\s]+$', '', a).strip()
            
            b = get_subfield(field, 'b')
            if b:
                publisher = re.sub(r'[,\s]+$', '', b).strip()
            
            c = get_subfield(field, 'c')
            if c:
                # Extract year
                year_match = re.search(r'\b(1\d{3}|20\d{2})\b', c)
                if year_match:
                    year = year_match.group(1)
        
        # ISBN (020)
        elif tag == '020':
            a = get_subfield(field, 'a')
            if a:
                # Extract ISBN
                isbn_match = re.search(r'(\d[\d\-X]+)', a)
                if isbn_match:
                    isbn = isbn_match.group(1)
        
        # ISSN (022)
        elif tag == '022':
            a = get_subfield(field, 'a')
            if a:
                issn = a
        
        # Language (041, 008)
        elif tag == '041':
            a = get_subfield(field, 'a')
            if a:
                language = a
        
        # Edition (250)
        elif tag == '250':
            a = get_subfield(field, 'a')
            if a:
                edition = a
        
        # Physical Description (300)
        elif tag == '300':
            a = get_subfield(field, 'a')
            if a:
                extent = a
        
        # Series (490, 830)
        elif tag in ['490', '830']:
            a = get_subfield(field, 'a')
            if a and not series:
                series = a
        
        # Notes - might contain abstract (500, 520)
        elif tag == '520':
            a = get_subfield(field, 'a')
            if a:
                abstract = a
        
        # Subjects (6XX)
        elif tag.startswith('6'):
            a = get_subfield(field, 'a')
            if a:
                subjects.append(a)
            
            for code in ['b', 'c', 'd', 'v', 'x', 'y', 'z']:
                subvalue = get_subfield(field, code)
                if subvalue:
                    # Add with separator to indicate it's a subdivision
                    subjects.append(f"{a} -- {subvalue}")
        
        # Electronic Location (856)
        elif tag == '856':
            u = get_subfield(field, 'u')
            if u:
                urls.append(u)
    
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        year=year,
        publisher_name=publisher,
        place_of_publication=place,
        isbn=isbn,
        issn=issn,
        urls=urls,
        abstract=abstract,
        language=language,
        subjects=subjects,
        series=series,
        extent=extent,
        edition=edition,
        raw_data=record.get('raw', '')
    )

@OAIClient.register_parser('mods')
def parse_mods(record, namespaces):
    """Parse MODS format from OAI-PMH."""
    metadata_root = record.get('metadata_root')
    record_id = record.get('identifier', 'unknown')
    
    if metadata_root is None:
        return BiblioRecord(
            id=record_id,
            title=f"Unparseable Record {record_id}",
            raw_data=record.get('raw', '')
        )
    
    # Add MODS namespace if not present
    ns = namespaces.copy()
    if 'mods' not in ns:
        ns['mods'] = 'http://www.loc.gov/mods/v3'
    
    # Find title
    title = "Untitled"
    title_info = metadata_root.find('.//mods:titleInfo/mods:title', ns)
    if title_info is not None and title_info.text:
        title = title_info.text.strip()
        
        # Check for subtitle
        subtitle = metadata_root.find('.//mods:titleInfo/mods:subTitle', ns)
        if subtitle is not None and subtitle.text:
            title = f"{title}: {subtitle.text.strip()}"
    
    # Find authors
    authors = []
    editors = []
    translators = []
    
    for name in metadata_root.findall('.//mods:name', ns):
        role = name.find('.//mods:roleTerm', ns)
        is_author = False
        
        # Check if it's an author/creator role
        if role is None:
            is_author = True  # Default to treating as author if no role specified
        elif role.text and role.text.lower() in ['editor', 'edt']:
            # Get name parts and add to editors
            name_parts = []
            for part in name.findall('.//mods:namePart', ns):
                if part.text and part.text.strip():
                    name_parts.append(part.text.strip())
            
            if name_parts:
                editors.append(' '.join(name_parts))
        # Check if it's a translator
        elif role.text and role.text.lower() in ['translator', 'trl']:
            # Get name parts and add to translators
            name_parts = []
            for part in name.findall('.//mods:namePart', ns):
                if part.text and part.text.strip():
                    name_parts.append(part.text.strip())
            
            if name_parts:
                translators.append(' '.join(name_parts))
        
        if is_author:
            # Get name parts
            name_parts = []
            for part in name.findall('.//mods:namePart', ns):
                if part.text and part.text.strip():
                    name_parts.append(part.text.strip())
            
            if name_parts:
                authors.append(' '.join(name_parts))
    
    # Find origin info
    year = None
    publisher = None
    place = None
    edition = None
    
    origin_info = metadata_root.find('.//mods:originInfo', ns)
    if origin_info is not None:
        # Date issued
        date_issued = origin_info.find('./mods:dateIssued', ns)
        if date_issued is not None and date_issued.text:
            # Try to extract year
            year_match = re.search(r'\b(1\d{3}|20\d{2})\b', date_issued.text)
            if year_match:
                year = year_match.group(1)
        
        # Publisher
        publisher_elem = origin_info.find('./mods:publisher', ns)
        if publisher_elem is not None and publisher_elem.text:
            publisher = publisher_elem.text.strip()
        
        # Place of publication
        place_elem = origin_info.find('./mods:place/mods:placeTerm', ns)
        if place_elem is not None and place_elem.text:
            place = place_elem.text.strip()
        
        # Edition
        edition_elem = origin_info.find('./mods:edition', ns)
        if edition_elem is not None and edition_elem.text:
            edition = edition_elem.text.strip()
    
    # Find physical description
    extent = None
    physical_desc = metadata_root.find('.//mods:physicalDescription', ns)
    if physical_desc is not None:
        extent_elem = physical_desc.find('./mods:extent', ns)
        if extent_elem is not None and extent_elem.text:
            extent = extent_elem.text.strip()
    
    # Find language
    language = None
    language_elem = metadata_root.find('.//mods:language/mods:languageTerm', ns)
    if language_elem is not None and language_elem.text:
        language = language_elem.text.strip()
    
    # Find abstract
    abstract = None
    abstract_elem = metadata_root.find('.//mods:abstract', ns)
    if abstract_elem is not None and abstract_elem.text:
        abstract = abstract_elem.text.strip()
    
    # Find subjects
    subjects = []
    for subject in metadata_root.findall('.//mods:subject', ns):
        topic = subject.find('./mods:topic', ns)
        if topic is not None and topic.text:
            subjects.append(topic.text.strip())
        
        # Geographic subjects
        geographic = subject.find('./mods:geographic', ns)
        if geographic is not None and geographic.text:
            subjects.append(f"Geographic: {geographic.text.strip()}")
        
        # Name subjects
        name = subject.find('./mods:name/mods:namePart', ns)
        if name is not None and name.text:
            subjects.append(f"Name: {name.text.strip()}")
    
    # Find series
    series = None
    related_item = metadata_root.find('.//mods:relatedItem[@type="series"]', ns)
    if related_item is not None:
        series_title = related_item.find('.//mods:title', ns)
        if series_title is not None and series_title.text:
            series = series_title.text.strip()
    
    # Find identifiers
    isbn = None
    issn = None
    urls = []
    
    for identifier in metadata_root.findall('.//mods:identifier', ns):
        id_type = identifier.get('type', '')
        if identifier.text and identifier.text.strip():
            id_text = identifier.text.strip()
            
            if id_type.lower() == 'isbn':
                isbn = id_text
            elif id_type.lower() == 'issn':
                issn = id_text
            elif id_type.lower() == 'uri' or id_text.startswith('http'):
                urls.append(id_text)
    
    # Check for location URLs
    for url in metadata_root.findall('.//mods:location/mods:url', ns):
        if url.text and url.text.strip():
            urls.append(url.text.strip())
    
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        year=year,
        publisher_name=publisher,
        place_of_publication=place,
        isbn=isbn,
        issn=issn,
        urls=urls,
        abstract=abstract,
        language=language,
        subjects=subjects,
        series=series,
        extent=extent,
        edition=edition,
        raw_data=record.get('raw', '')
    )

# Fix the generic fallback parser as well
def _generic_parse(self, record, namespaces) -> BiblioRecord:
    """Generic fallback parser for OAI-PMH records."""
    # Extract metadata based on metadata prefix
    metadata_prefix = record.get('metadata_prefix', 'oai_dc')
    metadata_root = record.get('metadata_root')
    
    # Default values
    id = record.get('identifier', 'unknown')
    title = "Untitled Record"
    authors = []
    editors = []
    translators = []
    year = None
    publisher = None
    place = None
    subjects = []
    urls = []
    
    if metadata_root is not None:
        # Try to extract data using common patterns
        if metadata_prefix in ['oai_dc', 'dc']:
            # Dublin Core
            title_elem = metadata_root.find('./dc:title', namespaces)
            if title_elem is not None and title_elem.text:
                title = title_elem.text.strip()
            
            # Authors (creators)
            for creator in metadata_root.findall('./dc:creator', namespaces):
                if creator.text and creator.text.strip():
                    authors.append(creator.text.strip())
            
            # Date/Year
            date_elem = metadata_root.find('./dc:date', namespaces)
            if date_elem is not None and date_elem.text:
                # Try to extract a year from date
                year_match = re.search(r'\b(1\d{3}|20\d{2})\b', date_elem.text)
                if year_match:
                    year = year_match.group(1)
            
            # Publisher
            publisher_elem = metadata_root.find('./dc:publisher', namespaces)
            if publisher_elem is not None and publisher_elem.text:
                publisher = publisher_elem.text.strip()
            
            # Subjects
            for subject in metadata_root.findall('./dc:subject', namespaces):
                if subject.text and subject.text.strip():
                    subjects.append(subject.text.strip())
            
            # Identifiers (extract URLs and ISBNs)
            isbn = None
            issn = None
            for identifier in metadata_root.findall('./dc:identifier', namespaces):
                if identifier.text and identifier.text.strip():
                    id_text = identifier.text.strip()
                    if id_text.startswith('http'):
                        urls.append(id_text)
                    elif 'isbn' in id_text.lower():
                        isbn_match = re.search(r'(?:isbn[:\s]*)?(\d[\d\-X]+)', id_text, re.IGNORECASE)
                        if isbn_match:
                            isbn = isbn_match.group(1)
                    elif 'issn' in id_text.lower():
                        issn_match = re.search(r'(?:issn[:\s]*)?(\d[\d\-X]+)', id_text, re.IGNORECASE)
                        if issn_match:
                            issn = issn_match.group(1)
        
        elif metadata_prefix == 'mods':
            # MODS format
            title_elem = metadata_root.find('.//mods:title', namespaces)
            if title_elem is not None and title_elem.text:
                title = title_elem.text.strip()
            
            # Authors from name elements
            for name in metadata_root.findall('.//mods:name', namespaces):
                role = name.find('.//mods:roleTerm', namespaces)
                name_part = name.find('.//mods:namePart', namespaces)
                
                # Check if it's an author/creator
                if (role is None or 
                    (role.text and role.text.lower() in ['author', 'creator', 'aut', 'cre'])):
                    if name_part is not None and name_part.text:
                        authors.append(name_part.text.strip())
            
            # Date/Year
            date_issued = metadata_root.find('.//mods:dateIssued', namespaces)
            if date_issued is not None and date_issued.text:
                year_match = re.search(r'\b(1\d{3}|20\d{2})\b', date_issued.text)
                if year_match:
                    year = year_match.group(1)
            
            # Publisher
            publisher_elem = metadata_root.find('.//mods:publisher', namespaces)
            if publisher_elem is not None and publisher_elem.text:
                publisher = publisher_elem.text.strip()
            
            # Place of publication
            place_elem = metadata_root.find('.//mods:placeTerm', namespaces)
            if place_elem is not None and place_elem.text:
                place = place_elem.text.strip()
            
            # Subjects
            for subject in metadata_root.findall('.//mods:subject', namespaces):
                topic = subject.find('.//mods:topic', namespaces)
                if topic is not None and topic.text:
                    subjects.append(topic.text.strip())
            
            # Identifiers
            isbn = None
            issn = None
            for identifier in metadata_root.findall('.//mods:identifier', namespaces):
                if identifier.get('type') == 'isbn' and identifier.text:
                    isbn = identifier.text.strip()
                elif identifier.get('type') == 'issn' and identifier.text:
                    issn = identifier.text.strip()
                elif identifier.get('type') == 'uri' and identifier.text:
                    urls.append(identifier.text.strip())
        
        elif metadata_prefix in ['marc', 'marc21', 'marcxml']:
            # MARC XML format
            datafields = metadata_root.findall('.//marc:datafield', namespaces)
            if not datafields:
                datafields = metadata_root.findall('.//*[local-name()="datafield"]')
            
            for field in datafields:
                tag = field.get('tag')
                
                # Title (245)
                if tag == '245':
                    title_parts = []
                    for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                        if subfield.text:
                            title_parts.append(subfield.text.strip())
                    for subfield in field.findall('.//marc:subfield[@code="b"]', namespaces):
                        if subfield.text:
                            title_parts.append(subfield.text.strip())
                    if title_parts:
                        title = ' '.join(title_parts)
                
                # Authors (100, 700)
                elif tag in ['100', '700']:
                    for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                        if subfield.text and subfield.text.strip():
                            authors.append(subfield.text.strip())
                
                # Publication info (260, 264)
                elif tag in ['260', '264']:
                    for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                        if subfield.text and subfield.text.strip():
                            place = subfield.text.strip()
                    for subfield in field.findall('.//marc:subfield[@code="b"]', namespaces):
                        if subfield.text and subfield.text.strip():
                            publisher = subfield.text.strip()
                    for subfield in field.findall('.//marc:subfield[@code="c"]', namespaces):
                        if subfield.text and subfield.text.strip():
                            year_match = re.search(r'\b(1\d{3}|20\d{2})\b', subfield.text)
                            if year_match:
                                year = year_match.group(1)
                
                # ISBN (020)
                elif tag == '020':
                    for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                        if subfield.text and subfield.text.strip():
                            isbn_match = re.search(r'(\d[\d\-X]+)', subfield.text)
                            if isbn_match:
                                isbn = isbn_match.group(1)
                
                # ISSN (022)
                elif tag == '022':
                    for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                        if subfield.text and subfield.text.strip():
                            issn = subfield.text.strip()
                
                # Subjects (6XX)
                elif tag.startswith('6'):
                    for subfield in field.findall('.//marc:subfield[@code="a"]', namespaces):
                        if subfield.text and subfield.text.strip():
                            subjects.append(subfield.text.strip())
                
                # URLs (856)
                elif tag == '856':
                    for subfield in field.findall('.//marc:subfield[@code="u"]', namespaces):
                        if subfield.text and subfield.text.strip():
                            urls.append(subfield.text.strip())
    
    # Clean up title (remove trailing punctuation)
    title = re.sub(r'[/\s:.]+$', '', title).strip()
    
    # Create BiblioRecord
    return BiblioRecord(
        id=id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        year=year,
        publisher_name=publisher,
        place_of_publication=place,
        isbn=isbn,
        issn=issn,
        urls=urls,
        subjects=subjects,
        raw_data=record.get('raw', '')
    )

# List of commonly used OAI-PMH endpoints
OAI_ENDPOINTS = {
    # National Libraries
    'dnb': {
        'name': 'Deutsche Nationalbibliothek',
        'url': 'https://services.dnb.de/oai/repository',
        'default_metadata_prefix': 'oai_dc',
        'description': 'The German National Library',
        'sets': {
            'dnb:reiheA': 'German National Bibliography Series A (new publications)',
            'dnb:reiheB': 'German National Bibliography Series B (new serials)',
            'dnb:reiheH': 'University Publications',
            'dnb:reiheO': 'Online Publications'
        }
    },
    'dnb_digital': {
        'name': 'Deutsche Nationalbibliothek (Digital Objects)',
        'url': 'https://services.dnb.de/oai2',
        'default_metadata_prefix': 'oai_dc',
        'description': 'Digital objects from the German National Library',
        'sets': {
            'dnb:digitalisate-oa': 'Digitized Public Domain Works'
        }
    },
    'loc': {
        'name': 'Library of Congress',
        'url': 'https://memory.loc.gov/cgi-bin/oai2_0',
        'default_metadata_prefix': 'oai_dc',
        'description': 'Library of Congress digital collections',
        'sets': {
            'lcbooks': 'Library of Congress Books',
            'lcmaps': 'Library of Congress Maps',
            'lcmss': 'Library of Congress Manuscripts'
        }
    },
    'europeana': {
        'name': 'Europeana',
        'url': 'https://api.europeana.eu/oai/record',
        'default_metadata_prefix': 'edm',
        'description': 'European digital cultural heritage',
        'sets': {}  # Sets are available via ListSets
    },
    'ddb': {
        'name': 'Deutsche Digitale Bibliothek',
        'url': 'https://oai.deutsche-digitale-bibliothek.de',
        'default_metadata_prefix': 'edm',
        'description': 'German Digital Library OAI-PMH interface',
        'sets': {}  # Sets are available via ListSets
    },
    
    # Universities
    'harvard': {
        'name': 'Harvard University Library',
        'url': 'https://dash.harvard.edu/oai/request',
        'default_metadata_prefix': 'oai_dc',
        'description': 'Harvard Digital Access to Scholarship',
        'sets': {}
    },
    'mit': {
        'name': 'MIT DSpace',
        'url': 'https://dspace.mit.edu/oai/request',
        'default_metadata_prefix': 'oai_dc',
        'description': 'MIT Open Access Articles',
        'sets': {}
    },
    'kitopen': {
        'name': 'KITopen (Karlsruher Institut für Technologie)',
        'url': 'https://dbkit.bibliothek.kit.edu/oai/',
        'default_metadata_prefix': 'oai_dc', 
        'description': 'Publications from Karlsruhe Institute of Technology',
        'sets': {}  # Sets are available via ListSets
    },
    
    # Other collections
    'arxiv': {
        'name': 'arXiv',
        'url': 'http://export.arxiv.org/oai2',
        'default_metadata_prefix': 'oai_dc',
        'description': 'arXiv open-access archive of scientific papers',
        'sets': {
            'physics': 'Physics',
            'math': 'Mathematics',
            'cs': 'Computer Science'
        }
    },
    'doaj': {
        'name': 'Directory of Open Access Journals',
        'url': 'https://www.doaj.org/oai',
        'default_metadata_prefix': 'oai_dc',
        'description': 'Directory of Open Access Journals',
        'sets': {}
    }
}