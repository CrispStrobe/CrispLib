#!/usr/bin/env python3
# ixtheo_library.py
"""
IxTheo Library - A specialized client for searching the Index Theologicus (IxTheo)

This module provides functionality to search the IxTheo theological database
and retrieve bibliographic data in various formats.
"""

import re
import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union, Tuple
import urllib.parse

import requests
from bs4 import BeautifulSoup

from sru_library import BiblioRecord

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ixtheo_library")

class IxTheoClient:
    """
    Client for searching the IxTheo theological database.
    """
    
    def __init__(self, timeout: int = 30, debug: bool = False, verify_ssl: bool = True):
        """
        Initialize the IxTheo client.
        
        Args:
            timeout: Request timeout in seconds
            debug: Whether to print debug information
            verify_ssl: Whether to verify SSL certificates
        """
        # IxTheo endpoints
        self.base_url = "https://ixtheo.de"
        self.search_url = f"{self.base_url}/Search/Results"
        self.export_url_template = f"{self.base_url}/Record/{{record_id}}/Export"
        
        # BSZ SRU endpoint (as fallback)
        self.bsz_sru_url = "https://sru.bsz-bw.de/swb"
        
        self.timeout = timeout
        self.debug = debug
        self.verify_ssl = verify_ssl
        
        # Initialize session
        self.session = requests.Session()
        
        # Set up session with browser-like headers
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive"
        })
        
        # Disable SSL verification if requested
        self.session.verify = verify_ssl
        if not verify_ssl:
            # Disable SSL warnings
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Initialize session (get cookies)
        self._initialize_session()
    
    def _initialize_session(self):
        """Initialize the session by visiting the main page and getting cookies"""
        try:
            self._debug_print("Initializing session...")
            
            # Visit the main page
            response = self.session.get(self.base_url, timeout=self.timeout)
            if response.status_code != 200:
                logger.warning(f"Could not access IxTheo website: {response.status_code}")
                return
                
            # Extract CSRF token if available
            self._extract_csrf_token(response.text)
            
            self._debug_print(f"Session initialized with cookies: {dict(self.session.cookies)}")
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error initializing session: {e}")
    
    def _extract_csrf_token(self, html_content):
        """
        Extract CSRF token from HTML content
        
        Args:
            html_content: HTML content to parse
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            csrf_input = soup.find('input', {'name': 'csrf'})
            if csrf_input and csrf_input.get('value'):
                self.csrf_token = csrf_input.get('value')
                self._debug_print(f"Found CSRF token: {self.csrf_token}")
                return True
        except Exception as e:
            logger.error(f"Error extracting CSRF token: {e}")
        
        self.csrf_token = None
        return False
    
    def search(self, query: str, search_type: str = "AllFields", page: int = 1, 
              limit: int = 20, sort: str = "relevance, year desc",
              filter_format: Optional[str] = None, 
              filter_language: Optional[str] = None,
              filter_topic: Optional[str] = None) -> Tuple[int, List[BiblioRecord]]:
        """
        Search IxTheo with the given parameters
        
        Args:
            query: Search query
            search_type: Type of search (e.g., AllFields, Title, Author)
            page: Page number
            limit: Results per page
            sort: Sort method
            filter_format: Filter by format
            filter_language: Filter by language
            filter_topic: Filter by topic
            
        Returns:
            Tuple of (total_results, list of BiblioRecord objects)
        """
        self._debug_print(f"Searching for: {query} (page {page})")
        
        # Prepare parameters
        params = {
            "lookfor": query,
            "type": search_type,
            "limit": limit,
            "sort": sort,
            "botprotect": ""  # Required to avoid bot detection
        }
        
        # Add filters if specified
        filter_params = []
        if filter_format:
            filter_params.append(f"format:{filter_format}")
        if filter_language:
            filter_params.append(f"language:{filter_language}")
        if filter_topic:
            filter_params.append(f"topic:{filter_topic}")
            
        # Add all filters
        if filter_params:
            params["filter[]"] = filter_params
        
        # Only add page parameter if greater than 1 to match IxTheo URL pattern
        if page > 1:
            params["page"] = page
        
        if hasattr(self, 'csrf_token') and self.csrf_token:
            params["csrf"] = self.csrf_token
        
        try:
            # Make request
            response = self.session.get(self.search_url, params=params, timeout=self.timeout)
            
            if response.status_code != 200:
                logger.error(f"Search failed with status code: {response.status_code}")
                return 0, []
            
            # Parse search results
            raw_results = self._parse_search_results(response.text, query, page, limit)
            
            if raw_results["status"] != "success":
                logger.error(f"Failed to parse search results: {raw_results.get('message', 'Unknown error')}")
                return 0, []
            
            # Convert raw results to BiblioRecord objects
            total_results = raw_results["total_results"]
            records = []
            
            for raw_record in raw_results["records"]:
                record_id = raw_record.get("id")
                
                # Generate clean authors
                authors = []
                for author in raw_record.get("authors", []):
                    if author and author.strip():
                        authors.append(author.strip())
                
                # Extract year from raw record
                year = raw_record.get("year")
                
                # Create BiblioRecord
                record = BiblioRecord(
                    id=record_id,
                    title=raw_record.get("title", "Unknown Title"),
                    authors=authors,
                    year=year,
                    subjects=raw_record.get("subjects", []),
                    format=", ".join(raw_record.get("formats", [])),
                    raw_data=str(raw_record)
                )
                
                records.append(record)
            
            return total_results, records
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Search request error: {e}")
            return 0, []
    
    def search_all_pages(self, query: str, search_type: str = "AllFields", max_results: Optional[int] = None, 
                        limit: int = 20, sort: str = "relevance, year desc",
                        filter_format: Optional[str] = None, 
                        filter_language: Optional[str] = None,
                        filter_topic: Optional[str] = None) -> Tuple[int, List[BiblioRecord]]:
        """
        Search all pages until we reach max_results or all results are fetched
        
        Args:
            query: Search query
            search_type: Type of search (e.g., AllFields, Title, Author)
            max_results: Maximum number of results to fetch
            limit: Results per page
            sort: Sort method
            filter_format: Filter by format
            filter_language: Filter by language
            filter_topic: Filter by topic
            
        Returns:
            Tuple of (total_results, list of BiblioRecord objects)
        """
        page = 1
        all_records = []
        total_results, records = self.search(
            query, search_type, page, limit, sort, 
            filter_format, filter_language, filter_topic
        )
        
        if not records:
            return 0, []
        
        all_records.extend(records)
        
        # Determine max pages to fetch
        if max_results is None:
            max_pages = (total_results + limit - 1) // limit  # Ceiling division
        else:
            max_pages = ((min(max_results, total_results) - 1) // limit) + 1
        
        # Fetch remaining pages
        for page in range(2, max_pages + 1):
            # Check if we've already reached max_results
            if max_results is not None and len(all_records) >= max_results:
                break
                
            # Add a small delay to avoid overwhelming the server
            time.sleep(1)
            
            _, page_records = self.search(
                query, search_type, page, limit, sort, 
                filter_format, filter_language, filter_topic
            )
            
            if not page_records:
                # No more results or error
                break
                
            all_records.extend(page_records)
        
        # Trim results to max_results if specified
        if max_results is not None and len(all_records) > max_results:
            all_records = all_records[:max_results]
            
        return total_results, all_records
    
    def _parse_search_results(self, html_content, query, page, limit):
        """
        Parse search results from HTML content
        
        Args:
            html_content: HTML content to parse
            query: Original search query
            page: Current page number
            limit: Results per page
            
        Returns:
            dict: Parsed search results
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            results = []
            
            # Get total results count
            total_results = 0
            stats_elements = soup.select('.search-stats, .js-search-stats')
            for stats_elem in stats_elements:
                text = stats_elem.get_text()
                if 'results of' in text:
                    try:
                        total_str = text.split('results of')[1].strip()
                        # Extract only digits
                        total_results = int(''.join(filter(str.isdigit, total_str)))
                        break
                    except (ValueError, IndexError):
                        pass
            
            # Extract search results
            result_items = soup.select('.result')
            self._debug_print(f"Found {len(result_items)} result items on the page")
            
            for item in result_items:
                # Get ID from different possible locations
                item_id = None
                
                # Try hidden input first
                hidden_id = item.select_one('.hiddenId')
                if hidden_id and hidden_id.get('value'):
                    item_id = hidden_id.get('value')
                
                # If still no ID, try from checkbox
                if not item_id:
                    checkbox = item.select_one('input.checkbox-select-item')
                    if checkbox and checkbox.get('value'):
                        # Value format is typically "Solr|ID"
                        checkbox_value = checkbox.get('value')
                        if '|' in checkbox_value:
                            item_id = checkbox_value.split('|')[1]
                
                # If still no ID, try from li id attribute
                if not item_id:
                    li_id = item.get('id')
                    if li_id and li_id.startswith('result'):
                        try:
                            # Extract the numeric part of the result ID
                            li_index = int(li_id[6:])
                            
                            # Find the corresponding hidden input in the form
                            hidden_inputs = soup.select('input[name="idsAll[]"]')
                            if li_index < len(hidden_inputs):
                                hidden_value = hidden_inputs[li_index].get('value')
                                if hidden_value and '|' in hidden_value:
                                    item_id = hidden_value.split('|')[1]
                        except (ValueError, IndexError):
                            pass
                
                if item_id:
                    # Get title
                    title_elem = item.select_one('.title')
                    title = title_elem.get_text(strip=True) if title_elem else "Unknown Title"
                    
                    # Get authors
                    authors = []
                    author_elem = item.select_one('.author')
                    if author_elem:
                        author_text = author_elem.get_text(strip=True)
                        # Handle different author formats
                        if '(' in author_text and ')' in author_text:
                            # Format: "Author Name (Author)" or similar
                            author_name = author_text.split('(')[0].strip()
                            authors.append(author_name)
                        else:
                            # Simple format or multiple authors
                            authors = [a.strip() for a in author_text.split(';') if a.strip()]
                    
                    # Get formats
                    formats = []
                    format_elements = item.select('.format')
                    for fmt in format_elements:
                        format_text = fmt.get_text(strip=True)
                        if format_text:
                            formats.append(format_text)
                    
                    # Get year 
                    year = None
                    year_elem = item.select_one('.publishDate')
                    if year_elem:
                        year_text = year_elem.get_text(strip=True)
                        # Try to extract year from text
                        year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
                        if year_match:
                            year = year_match.group(0)
                    
                    # Get subjects/topics
                    subjects = []
                    subject_elements = item.select('.subject a')
                    for subject_elem in subject_elements:
                        subject_text = subject_elem.get_text(strip=True)
                        if subject_text:
                            subjects.append(subject_text)
                    
                    # Build result object
                    result = {
                        "id": item_id,
                        "title": title,
                        "authors": authors,
                        "formats": formats,
                        "year": year,
                        "subjects": subjects
                    }
                    
                    results.append(result)
            
            return {
                "status": "success",
                "query": query,
                "total_results": total_results,
                "current_page": page,
                "results_per_page": limit,
                "records": results
            }
            
        except Exception as e:
            logger.error(f"Error parsing search results: {e}")
            import traceback
            traceback.print_exc()
            
            return {
                "status": "error",
                "message": str(e),
                "total_results": 0,
                "records": []
            }
    
    def get_export_data(self, record_id, export_format="RIS"):
        """
        Get export data for a specific record
        
        Args:
            record_id: The record ID
            export_format: The export format (RIS or MARC)
            
        Returns:
            str: The export data
        """
        self._debug_print(f"Getting {export_format} export for record: {record_id}")
        
        # IxTheo only supports RIS and MARC formats via direct export
        if export_format not in ["RIS", "MARC"]:
            export_format = "RIS"  # Default to RIS if unsupported format is requested
        
        # Generate export URL
        export_url = f"{self.export_url_template.format(record_id=record_id)}?style={export_format}"
        
        try:
            # Wait a moment to avoid overwhelming the server
            time.sleep(0.5)
            
            # Make request with headers that match a browser
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/plain, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": f"{self.base_url}/Record/{record_id}",
                "X-Requested-With": "XMLHttpRequest"
            }
            response = self.session.get(export_url, headers=headers, timeout=self.timeout)
            
            if response.status_code != 200:
                logger.error(f"Export failed with status code: {response.status_code}")
                return None
            
            # Check if response is empty
            if not response.text.strip():
                logger.warning(f"Export returned empty response for record {record_id}")
                return None
                
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Export request error for record {record_id}: {e}")
            return None
    
    def get_detailed_record(self, record_id):
        """
        Get detailed information for a specific record
        
        Args:
            record_id: The record ID
            
        Returns:
            BiblioRecord: The detailed record
        """
        self._debug_print(f"Getting detail for record: {record_id}")
        
        try:
            # Make request for record detail page
            response = self.session.get(f"{self.base_url}/Record/{record_id}", timeout=self.timeout)
            
            if response.status_code != 200:
                logger.error(f"Record detail request failed with status code: {response.status_code}")
                return None
            
            # Parse record details
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title
            title = "Unknown Title"
            title_elem = soup.select_one('.record .title')
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Extract authors
            authors = []
            author_elements = soup.select('.record .authors a')
            for author_elem in author_elements:
                author_text = author_elem.get_text(strip=True)
                if author_text:
                    authors.append(author_text)
            
            # Extract publication info
            publisher = None
            pub_date = None
            pub_place = None
            
            # Try to extract from different fields
            pub_info_elem = soup.select_one('.record .publisher')
            if pub_info_elem:
                pub_info = pub_info_elem.get_text(strip=True)
                # Try to parse publisher, place, and date
                # Format often like: "Publisher: Place, Date"
                if ':' in pub_info:
                    publisher_part = pub_info.split(':', 1)[1].strip()
                    if ',' in publisher_part:
                        publisher_parts = publisher_part.split(',')
                        publisher = publisher_parts[0].strip()
                        if len(publisher_parts) > 1:
                            pub_date = publisher_parts[-1].strip()
                            if len(publisher_parts) > 2:
                                pub_place = ",".join(publisher_parts[1:-1]).strip()
                    else:
                        publisher = publisher_part
            
            # Extract year from pub_date if not explicitly found
            year = None
            if pub_date:
                year_match = re.search(r'\b(19|20)\d{2}\b', pub_date)
                if year_match:
                    year = year_match.group(0)
            
            # Extract subjects
            subjects = []
            subject_elements = soup.select('.record .subject a')
            for subject_elem in subject_elements:
                subject_text = subject_elem.get_text(strip=True)
                if subject_text:
                    subjects.append(subject_text)
            
            # Extract abstract/summary
            abstract = None
            abstract_elem = soup.select_one('.record .summary')
            if abstract_elem:
                abstract = abstract_elem.get_text(strip=True)
            
            # Extract ISBN/ISSN
            isbn = None
            issn = None
            
            # Look for ISBN
            isbn_elem = soup.select_one('.record .isbn')
            if isbn_elem:
                isbn_text = isbn_elem.get_text(strip=True)
                isbn_match = re.search(r'\b\d[\d\-]+\d\b', isbn_text)
                if isbn_match:
                    isbn = isbn_match.group(0)
            
            # Look for ISSN
            issn_elem = soup.select_one('.record .issn')
            if issn_elem:
                issn_text = issn_elem.get_text(strip=True)
                issn_match = re.search(r'\b\d{4}-\d{3}[\dX]\b', issn_text)
                if issn_match:
                    issn = issn_match.group(0)
            
            # Create BiblioRecord
            record = BiblioRecord(
                id=record_id,
                title=title,
                authors=authors,
                year=year,
                publisher_name=publisher,
                place_of_publication=pub_place,
                isbn=isbn,
                issn=issn,
                subjects=subjects,
                abstract=abstract,
                raw_data=response.text
            )
            
            return record
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Record detail request error for record {record_id}: {e}")
            return None
    
    def _debug_print(self, message):
        """Print debug message if debug mode is enabled"""
        if self.debug:
            logger.debug(message)

    def _convert_ris_to_bibtex(self, ris_data, record_id):
        """
        Convert RIS data to BibTeX format
        
        Args:
            ris_data: RIS formatted data
            record_id: Record ID for reference
            
        Returns:
            str: BibTeX formatted data
        """
        self._debug_print(f"Converting RIS to BibTeX for record {record_id}")
        
        if not ris_data:
            self._debug_print("No RIS data to convert")
            return None
            
        # Parse RIS data
        ris_lines = ris_data.strip().split("\n")
        self._debug_print(f"Parsing {len(ris_lines)} lines of RIS data")
        
        # Initialize fields to extract
        entry_type = "misc"  # Default
        title = None
        authors = []
        year = None
        publisher = None
        place = None
        isbn = None
        issn = None
        journal = None
        volume = None
        issue = None
        pages = None
        start_page = None
        end_page = None
        doi = None
        url = None
        abstract = None
        
        # Extract data from RIS
        for line in ris_lines:
            line = line.strip()
            if not line:
                continue
                
            # Try to split line into tag and value
            if "  - " not in line:
                self._debug_print(f"Skipping invalid RIS line: {line}")
                continue
                
            parts = line.split("  - ", 1)
            if len(parts) != 2:
                self._debug_print(f"Skipping invalid RIS line after split: {line}")
                continue
                
            tag, value = parts[0].strip(), parts[1].strip()
            self._debug_print(f"Processing RIS tag: {tag} with value: {value}")
            
            if tag == "TY":
                # Map RIS type to BibTeX type
                if value == "JOUR":
                    entry_type = "article"
                    self._debug_print(f"Setting entry type to article based on JOUR")
                elif value == "BOOK":
                    entry_type = "book"
                    self._debug_print(f"Setting entry type to book based on BOOK")
                elif value == "CHAP":
                    entry_type = "incollection"
                    self._debug_print(f"Setting entry type to incollection based on CHAP")
                elif value == "CONF":
                    entry_type = "inproceedings"
                    self._debug_print(f"Setting entry type to inproceedings based on CONF")
                elif value == "THES":
                    entry_type = "phdthesis"
                    self._debug_print(f"Setting entry type to phdthesis based on THES")
                self._debug_print(f"Entry type set to: {entry_type}")
                
            elif tag == "TI" or tag == "T1":
                title = value
                self._debug_print(f"Title set to: {title}")
                
            elif tag == "AU":
                authors.append(value)
                self._debug_print(f"Added author: {value}")
                
            elif tag == "PY" or tag == "Y1":
                # Extract year
                year_match = re.search(r'\b(19|20)\d{2}\b', value)
                if year_match:
                    year = year_match.group(0)
                    self._debug_print(f"Year set to: {year}")
                    
            elif tag == "PB":
                publisher = value
                self._debug_print(f"Publisher set to: {publisher}")
                
            elif tag == "CY":
                place = value
                self._debug_print(f"Place set to: {place}")
                
            elif tag == "SN":
                # Could be ISBN or ISSN
                if re.search(r'\d{4}-\d{3}[\dX]', value):
                    issn = value
                    self._debug_print(f"ISSN set to: {issn}")
                else:
                    isbn = value
                    self._debug_print(f"ISBN set to: {isbn}")
                    
            elif tag == "JO" or tag == "T2":
                journal = value
                self._debug_print(f"Journal/Series set to: {journal}")
                
            elif tag == "VL":
                volume = value
                self._debug_print(f"Volume set to: {volume}")
                
            elif tag == "IS":
                issue = value
                self._debug_print(f"Issue set to: {issue}")
                
            elif tag == "SP":
                start_page = value
                self._debug_print(f"Start page set to: {start_page}")
                
            elif tag == "EP":
                end_page = value
                self._debug_print(f"End page set to: {end_page}")
                
            elif tag == "DO":
                doi = value
                self._debug_print(f"DOI set to: {doi}")
                
            elif tag == "UR":
                url = value
                self._debug_print(f"URL set to: {url}")
                
            elif tag == "AB":
                abstract = value
                self._debug_print(f"Abstract set")
        
        # Construct page range if we have both start and end pages
        if start_page and end_page:
            pages = f"{start_page}--{end_page}"
            self._debug_print(f"Page range set to: {pages}")
        elif start_page:
            pages = start_page
            self._debug_print(f"Single page set to: {pages}")
        
        # If no title was found, use "Unknown Title"
        if not title:
            title = "Unknown Title"
            self._debug_print("No title found, using 'Unknown Title'")
        
        # Create citation key from first author and year
        citation_key = "ixtheo"
        if authors and year:
            # Extract last name for the key
            first_author = authors[0]
            if ',' in first_author:
                last_name = first_author.split(',')[0].strip().lower()
                citation_key = f"{last_name}{year}"
            else:
                parts = first_author.split()
                if parts:
                    citation_key = f"{parts[-1].lower()}{year}"
            self._debug_print(f"Generated citation key: {citation_key}")
        else:
            citation_key = f"ixtheo_{record_id}"
            self._debug_print(f"No author/year, using ID-based citation key: {citation_key}")
        
        # Build BibTeX entry
        bibtex = [f"@{entry_type}{{{citation_key},"]
        
        # Add title
        if title:
            # Escape special characters in title
            title = title.replace("&", "\\&").replace("%", "\\%")
            bibtex.append(f"  title = {{{title}}},")
        
        # Add authors
        if authors:
            # Format authors for BibTeX
            formatted_authors = []
            for author in authors:
                # Ensure proper formatting (already in "lastname, firstname" in RIS)
                formatted_authors.append(author)
            
            bibtex.append(f"  author = {{{' and '.join(formatted_authors)}}},")
        
        # Add year
        if year:
            bibtex.append(f"  year = {{{year}}},")
        
        # Add journal for articles
        if entry_type == "article" and journal:
            bibtex.append(f"  journal = {{{journal}}},")
        elif journal and entry_type != "article":
            # For non-articles, add as series
            bibtex.append(f"  series = {{{journal}}},")
        
        # Add volume for articles or books with volumes
        if volume:
            bibtex.append(f"  volume = {{{volume}}},")
        
        # Add number/issue for articles
        if issue:
            bibtex.append(f"  number = {{{issue}}},")
        
        # Add pages
        if pages:
            bibtex.append(f"  pages = {{{pages}}},")
        
        # Add publisher
        if publisher:
            bibtex.append(f"  publisher = {{{publisher}}},")
        
        # Add address/place
        if place:
            bibtex.append(f"  address = {{{place}}},")
        
        # Add ISBN/ISSN
        if isbn:
            bibtex.append(f"  isbn = {{{isbn}}},")
        if issn:
            bibtex.append(f"  issn = {{{issn}}},")
        
        # Add DOI
        if doi:
            bibtex.append(f"  doi = {{{doi}}},")
        
        # Add URL
        if url:
            bibtex.append(f"  url = {{{url}}},")
        
        # Add abstract
        if abstract:
            # Limit abstract length to avoid issues with BibTeX
            if len(abstract) > 1000:
                abstract = abstract[:997] + "..."
            abstract = abstract.replace("&", "\\&").replace("%", "\\%")
            bibtex.append(f"  abstract = {{{abstract}}},")
        
        # Add note with record ID
        bibtex.append(f"  note = {{ID: {record_id}}}")
        
        # Close entry
        bibtex.append("}")
        
        result = "\n".join(bibtex)
        self._debug_print(f"Generated BibTeX for {record_id}:")
        self._debug_print(result)
        return result


# Add IxTheo to library_search.py functionality
class IxTheoSearchHandler:
    """
    Handler for IxTheo searches in library_search.py
    """
    
    def __init__(self, timeout=30, debug=False, verify_ssl=True):
        """
        Initialize the IxTheo search handler
        
        Args:
            timeout: Request timeout in seconds
            debug: Whether to print debug information
            verify_ssl: Whether to verify SSL certificates
        """
        self.client = IxTheoClient(timeout=timeout, debug=debug, verify_ssl=verify_ssl)
    
    def search(self, query=None, title=None, author=None, subject=None, 
              max_results=20, format_filter=None, language_filter=None):
        """
        Search IxTheo
        
        Args:
            query: General search query
            title: Title search
            author: Author search
            subject: Subject search
            max_results: Maximum number of results
            format_filter: Format filter
            language_filter: Language filter
            
        Returns:
            Tuple of (total_results, list of BiblioRecord objects)
        """
        # Determine search type and query
        search_type = "AllFields"
        search_query = query
        
        if title:
            search_type = "Title"
            search_query = title
        elif author:
            search_type = "Author"
            search_query = author
        elif subject:
            search_type = "Subject"
            search_query = subject
        
        # Perform search
        return self.client.search_all_pages(
            query=search_query,
            search_type=search_type,
            max_results=max_results,
            filter_format=format_filter,
            filter_language=language_filter
        )
    
    def get_record_with_export(self, record, export_format=None):
        """
        Get a record with export data
        
        Args:
            record: The BiblioRecord to enhance
            export_format: The export format requested (not used in this method)
            
        Returns:
            Enhanced BiblioRecord with complete metadata
        """
        if not record.id:
            logger.debug(f"Record has no ID, returning unmodified")
            return record
        
        # First get detailed record information
        logger.debug(f"Getting detail for record: {record.id}")
        detailed_record = self.client.get_detailed_record(record.id)
        
        # Get RIS export data - IxTheo only supports RIS and MARC directly
        logger.debug(f"Getting RIS export for record: {record.id}")
        ris_data = self.client.get_export_data(record.id, "RIS")
        
        # Debug output for RIS data
        if ris_data:
            logger.debug(f"RIS data received for {record.id}:")
            logger.debug(ris_data)
        else:
            logger.debug(f"No RIS data received for {record.id}")
        
        # Extract data from RIS to populate record fields
        if ris_data:
            # Parse RIS data to extract key fields
            record_type = None
            title = None
            authors = []
            year = None
            publisher = None
            place = None
            isbn = None
            issn = None
            journal = None
            volume = None
            issue = None
            start_page = None
            end_page = None
            language = None
            doi = None
            series_title = None
            series_editor = None
            
            # Simple RIS parser
            for line in ris_data.splitlines():
                line = line.strip()
                if not line or "  - " not in line:
                    continue
                
                parts = line.split("  - ", 1)
                if len(parts) != 2:
                    continue
                    
                tag, value = parts[0].strip(), parts[1].strip()
                logger.debug(f"Processing RIS tag: {tag} with value: {value}")
                
                if tag == "TY":  # Type
                    record_type = value
                    logger.debug(f"Record type set to: {record_type}")
                elif tag == "TI" or tag == "T1":  # Title
                    title = value
                    logger.debug(f"Title set to: {title}")
                elif tag == "AU":  # Author
                    authors.append(value)
                    logger.debug(f"Added author: {value}")
                elif tag == "PY" or tag == "Y1":  # Year
                    year_match = re.search(r'(\d{4})', value)
                    if year_match:
                        year = year_match.group(1)
                        logger.debug(f"Year set to: {year}")
                elif tag == "PB":  # Publisher
                    publisher = value
                    logger.debug(f"Publisher set to: {publisher}")
                elif tag == "CY":  # City/Place
                    place = value
                    logger.debug(f"Place set to: {place}")
                elif tag == "SN":  # ISBN/ISSN
                    if re.search(r'\d{4}-\d{3}[\dX]', value):
                        issn = value
                        logger.debug(f"ISSN set to: {issn}")
                    else:
                        isbn = value
                        logger.debug(f"ISBN set to: {isbn}")
                elif tag == "T2":  # Secondary Title - contains series/journal info
                    if record_type == "JOUR":
                        journal = value
                        logger.debug(f"Journal title set to: {value}")
                    else:
                        # For book chapters, T2 often contains the book title and editors
                        series_title = value
                        logger.debug(f"Series/Book title set to: {value}")
                        
                        # Try to extract editor information from the series title
                        editor_match = re.search(r'(.+?),\s+(.+?)(?:\s+\d{4}-)?\s+\(edt\)', value)
                        if editor_match:
                            # Extract editor name and clean up
                            series_editor = editor_match.groups()[0] + ', ' + editor_match.groups()[1]
                            # Remove birth dates
                            series_editor = re.sub(r'\s+\d{4}-(?:\d{4})?', '', series_editor)
                            logger.debug(f"Extracted series editor: {series_editor}")
                            
                            # Extract just the book title
                            book_title_match = re.search(r'\(edt\),\s+(.+)', value)
                            if book_title_match:
                                series_title = book_title_match.groups()[0].strip()
                                logger.debug(f"Cleaned series title: {series_title}")
                elif tag == "JO":  # Journal
                    journal = value
                    logger.debug(f"Journal title set to: {value}")
                elif tag == "VL":  # Volume
                    volume = value
                    logger.debug(f"Volume set to: {volume}")
                elif tag == "IS":  # Issue
                    issue = value
                    logger.debug(f"Issue set to: {issue}")
                elif tag == "SP":  # Start Page
                    start_page = value
                    logger.debug(f"Start page set to: {start_page}")
                elif tag == "EP":  # End Page
                    end_page = value
                    logger.debug(f"End page set to: {end_page}")
                elif tag == "LA":  # Language
                    language = value
                    logger.debug(f"Language set to: {language}")
                elif tag == "DO":  # DOI
                    doi = value
                    logger.debug(f"DOI set to: {doi}")
            
            # Create page range if we have both start and end pages
            pages = None
            if start_page and end_page:
                pages = f"{start_page}-{end_page}"
                logger.debug(f"Pages set to: {pages}")
            elif start_page:
                pages = start_page
                logger.debug(f"Pages set to: {start_page}")
            
            # Determine the format from record_type
            format_str = None
            if record_type == "JOUR":
                format_str = "Journal Article"
            elif record_type == "BOOK":
                format_str = "Book"
            elif record_type == "CHAP":
                format_str = "Book Chapter"
            
            # Create a new record with data from both RIS and detailed record
            enhanced_record = BiblioRecord(
                id=record.id,
                title=title or (detailed_record.title if detailed_record else record.title) or "Unknown Title",
                authors=authors or (detailed_record.authors if detailed_record else record.authors) or [],
                year=year or (detailed_record.year if detailed_record else record.year),
                publisher_name=publisher or (detailed_record.publisher_name if detailed_record else None),
                place_of_publication=place or (detailed_record.place_of_publication if detailed_record else None),
                isbn=isbn or (detailed_record.isbn if detailed_record else None),
                issn=issn or (detailed_record.issn if detailed_record else None),
                
                # For journal articles
                journal_title=journal if record_type == "JOUR" else None,
                volume=volume,
                issue=issue,
                
                # For book chapters, store book title in series field with editor info
                series=series_title,
                
                # Store editor info in raw_data
                raw_data={
                    "ris_data": ris_data,
                    "series_editor": series_editor
                } if series_editor else ris_data,
                
                # Store page range in extent for consistency
                extent=f"Pages {pages}" if pages else None,
                
                subjects=detailed_record.subjects if detailed_record and detailed_record.subjects else [],
                abstract=detailed_record.abstract if detailed_record and detailed_record.abstract else None,
                language=language,
                
                # Store record type to inform downstream formatting
                format=format_str if format_str else (record_type if record_type else None)
            )
            
            logger.debug(f"Enhanced record created: {enhanced_record}")
            
            # Log complete record details
            logger.debug(f"Enhanced record details for {record.id}:")
            logger.debug(f"  Title: {enhanced_record.title}")
            logger.debug(f"  Authors: {enhanced_record.authors}")
            logger.debug(f"  Year: {enhanced_record.year}")
            logger.debug(f"  Format/Type: {enhanced_record.format}")
            logger.debug(f"  Publisher: {enhanced_record.publisher_name}")
            logger.debug(f"  Place: {enhanced_record.place_of_publication}")
            logger.debug(f"  ISBN: {enhanced_record.isbn}")
            logger.debug(f"  ISSN: {enhanced_record.issn}")
            logger.debug(f"  Journal: {enhanced_record.journal_title}")
            logger.debug(f"  Series/Book title: {enhanced_record.series}")
            logger.debug(f"  Series/Book editor: {series_editor}")
            logger.debug(f"  Volume: {enhanced_record.volume}")
            logger.debug(f"  Issue: {enhanced_record.issue}")
            logger.debug(f"  Extent: {enhanced_record.extent}")
            
            return enhanced_record
        
        # If we have a detailed record but no RIS data
        if detailed_record:
            logger.debug(f"Using detailed record (no RIS data) for {record.id}")
            detailed_record.raw_data = detailed_record.raw_data or record.raw_data
            return detailed_record
        
        # If all else fails, return the original record
        logger.debug(f"No enhanced data available, returning original record for {record.id}")
        return record


# Define IxTheo endpoint information
IXTHEO_ENDPOINTS = {
    'ixtheo': {
        'name': 'Index Theologicus (IxTheo)',
        'url': 'https://ixtheo.de',
        'description': 'Specialized theological bibliography',
        'formats': ['Article', 'Book', 'Journal', 'Dissertation'],
        'languages': ['German', 'English', 'French', 'Italian', 'Spanish'],
        'export_formats': ['RIS', 'MARC'],
        'subjects': []
    }
}