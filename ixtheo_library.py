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
                    
                    # Get publisher name if available
                    publisher = None
                    publisher_elem = item.select_one('.publisher')
                    if publisher_elem:
                        publisher = publisher_elem.get_text(strip=True)
                    
                    # Build result object
                    result = {
                        "id": item_id,
                        "title": title,
                        "authors": authors,
                        "formats": formats,
                        "year": year,
                        "subjects": subjects,
                        "publisher": publisher
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
    
    def get_record_with_html(self, record_id):
    
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
            
            # Extract title - find the h3 tag with the title
            title = "Unknown Title"
            title_elem = soup.select_one('h3[property="name"]')
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Extract authors - look in the bibliographic details table
            authors = []
            for row in soup.select('table.table-striped tr'):
                header = row.select_one('th')
                if header and 'Author' in header.get_text():
                    author_cell = row.select_one('td')
                    if author_cell:
                        author_spans = author_cell.select('span[property="name"]')
                        for span in author_spans:
                            author_text = span.get_text(strip=True)
                            if author_text:
                                authors.append(author_text)
            
            # Extract format
            format_str = ""
            format_row = soup.select_one('table.table-striped tr th:-soup-contains("Format:")')
            if format_row:
                format_cell = format_row.find_next_sibling('td')
                if format_cell:
                    format_spans = format_cell.select('span.format')
                    for span in format_spans:
                        format_type = span.get_text(strip=True)
                        format_str += format_type
            
            # Extract language
            language = None
            language_row = soup.select_one('table.table-striped tr th:-soup-contains("Language:")')
            if language_row and language_row.find_next_sibling('td'):
                language = language_row.find_next_sibling('td').get_text(strip=True)
            
            # Extract publication info
            publisher = None
            pub_place = None
            year = None
            
            # Try finding the publication information
            published_row = soup.select_one('table.table-striped tr th:-soup-contains("Published:")')
            if published_row:
                published_cell = published_row.find_next_sibling('td')
                if published_cell:
                    # Extract location
                    location_elem = published_cell.select_one('span[property="location"]')
                    if location_elem:
                        pub_place = location_elem.get_text(strip=True)
                    
                    # Extract publisher
                    publisher_elem = published_cell.select_one('span[property="name"]')
                    if publisher_elem:
                        publisher = publisher_elem.get_text(strip=True)
                    
                    # Extract year
                    date_elem = published_cell.select_one('span[property="datePublished"]')
                    if date_elem:
                        year = date_elem.get_text(strip=True)
            
            # Extract subjects
            subjects = []
            # Find subject rows - there may be multiple
            subject_rows = soup.select('table.table-striped tr th:-soup-contains("Subject")')
            for row in subject_rows:
                subject_cell = row.find_next_sibling('td')
                if subject_cell:
                    # Get all subject links
                    for link in subject_cell.select('a[href*="/Search/Results"]'):
                        subject_text = link.get_text(strip=True)
                        if subject_text and subject_text not in subjects:
                            subjects.append(subject_text)
            
            # Extract ISBN/ISSN
            isbn = None
            issn = None
            
            isbn_row = soup.select_one('.description-tab table.table-striped tr th:-soup-contains("ISBN:")')
            if isbn_row:
                isbn_cell = isbn_row.find_next_sibling('td')
                if isbn_cell:
                    span = isbn_cell.select_one('span[property="isbn"]')
                    if span:
                        isbn = span.get_text(strip=True)
                    else:
                        isbn = isbn_cell.get_text(strip=True)
            
            issn_row = soup.select_one('.description-tab table.table-striped tr th:-soup-contains("ISSN:")')
            if issn_row:
                issn_cell = issn_row.find_next_sibling('td')
                if issn_cell:
                    span = issn_cell.select_one('span[property="issn"]')
                    if span:
                        issn = span.get_text(strip=True)
                    else:
                        issn = issn_cell.get_text(strip=True)
            
            # Extract physical description (extent)
            extent = None
            phys_desc_row = soup.select_one('.description-tab table.table-striped tr th:-soup-contains("Physical Description:")')
            if phys_desc_row:
                phys_desc_cell = phys_desc_row.find_next_sibling('td')
                if phys_desc_cell:
                    extent = phys_desc_cell.get_text(strip=True)
            
            # Extract series
            series = None
            series_row = soup.select_one('table.table-striped tr th:-soup-contains("Series")')
            if series_row:
                series_cell = series_row.find_next_sibling('td')
                if series_cell:
                    series_link = series_cell.select_one('a')
                    if series_link:
                        series = series_link.get_text(strip=True)
            
            # Extract journal info - volume, issue, pages
            journal_title = None
            volume = None
            issue = None
            pages = None
            
            journal_row = soup.select_one('table.table-striped tr th:-soup-contains("In:")')
            if journal_row:
                journal_cell = journal_row.find_next_sibling('td')
                if journal_cell:
                    journal_link = journal_cell.select_one('a')
                    if journal_link:
                        journal_title = journal_link.get_text(strip=True)
                    
                    # Try to extract volume, issue, pages from text after journal title
                    journal_info = journal_cell.get_text(strip=True)
                    
                    # Extract volume
                    vol_match = re.search(r'Volume:\s*(\d+)', journal_info) or re.search(r'Volume[^,]*?(\d+)', journal_info)
                    if vol_match:
                        volume = vol_match.group(1)
                    
                    # Extract issue
                    issue_match = re.search(r'Issue:\s*(\d+)', journal_info) or re.search(r'Issue[^,]*?(\d+)', journal_info)
                    if issue_match:
                        issue = issue_match.group(1)
                    
                    # Extract pages
                    pages_match = re.search(r'Pages:\s*([0-9-]+)', journal_info) or re.search(r'Pages[^,]*?([0-9-]+)', journal_info)
                    if pages_match:
                        pages = pages_match.group(1)
                    
                    # If we still don't have all the info, try another approach with more flexible regex
                    if not (volume and issue and pages):
                        parts = re.search(r'Year:\s*([\d]{4})(?:[^,]*?)(?:Volume:\s*(\d+))?(?:[^,]*?)(?:Issue:\s*(\d+))?(?:[^,]*?)(?:Pages:\s*([0-9-]+))?', journal_info)
                        if parts:
                            if not year and parts.group(1):
                                year = parts.group(1)
                            if not volume and parts.group(2):
                                volume = parts.group(2)
                            if not issue and parts.group(3):
                                issue = parts.group(3)
                            if not pages and parts.group(4):
                                pages = parts.group(4)
            
            # Extract abstract/summary
            abstract = None
            summary_row = soup.select_one('.description-tab table.table-striped tr th:-soup-contains("Summary:")')
            if summary_row:
                summary_cell = summary_row.find_next_sibling('td')
                if summary_cell:
                    abstract = summary_cell.get_text(strip=True)
            
            # Extract URLs (if any)
            urls = []
            url_row = soup.select_one('table.table-striped tr th:-soup-contains("Online Access:")')
            if url_row:
                url_cell = url_row.find_next_sibling('td')
                if url_cell:
                    for link in url_cell.select('a.fulltext'):
                        href = link.get('href')
                        if href and href.startswith('http'):
                            urls.append(href)
            
            # Create BiblioRecord with all extracted data
            record = BiblioRecord(
                id=record_id,
                title=title,
                authors=authors,
                year=year,
                publisher_name=publisher,
                place_of_publication=pub_place,
                isbn=isbn,
                issn=issn,
                urls=urls,
                abstract=abstract,
                language=language,
                format=format_str,
                subjects=subjects,
                series=series,
                extent=extent,
                journal_title=journal_title,
                volume=volume,
                issue=issue,
                pages=pages,
                raw_data=response.text
            )
            
            self._debug_print(f"Created detailed record: {record.title}")
            return record
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Record detail request error for record {record_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing record details for {record_id}: {e}")
            import traceback
            traceback.print_exc()
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
        editors = []  
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

            elif tag == "ED":
                editors.append(value)
                self._debug_print(f"Added editor: {value}")
                
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

        if editors:
            # Format editors for BibTeX
            formatted_editors = []
            for editor in editors:
                # Ensure proper formatting (already in "lastname, firstname" in RIS)
                formatted_editors.append(editor)
            
            bibtex.append(f"  editor = {{{' and '.join(formatted_editors)}}},")
        
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
    
    def get_record_with_marc(self, record):
        """
        Get a record with MARC export data
        
        Args:
            record: The BiblioRecord to enhance
            
        Returns:
            Enhanced BiblioRecord with complete metadata from MARC
        """
        if not record.id:
            logger.debug(f"Record has no ID, returning unmodified")
            return record
        
        # Get MARC export data
        logger.debug(f"Getting MARC export for record: {record.id}")
        marc_data = self.client.get_export_data(record.id, "MARC")
        
        # Debug output for MARC data
        if marc_data:
            logger.debug(f"MARC data received for {record.id}:")
            logger.debug(marc_data)
        else:
            logger.debug(f"No MARC data received for {record.id}")
            # If no MARC data, fallback to RIS
            logger.warning(f"MARC retrieval failed for record {record.id}, falling back to RIS")
            return self.get_record_with_ris(record)
        
        # Parse MARC data to extract key fields
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
        series = None
        language = None
        subjects = []
        abstract = None
        format_str = None
        
        # Basic MARC parsing - this is a simplified version
        # A more robust implementation would use the pymarc library
        try:
            # Look for title in 245 field
            title_match = re.search(r'=245\s+\d\d\s+\$a([^$]+)', marc_data)
            if title_match:
                title = title_match.group(1).strip()
                # Check for subtitle in $b
                subtitle_match = re.search(r'=245\s+\d\d\s+\$a[^$]+\$b([^$]+)', marc_data)
                if subtitle_match:
                    title += ': ' + subtitle_match.group(1).strip()
            
            # Look for authors in 100 and 700 fields
            author_matches = re.finditer(r'=(?:100|700)\s+\d\d\s+\$a([^$]+)', marc_data)
            for match in author_matches:
                author = match.group(1).strip()
                # Clean up author name
                author = re.sub(r',\s*\d{4}-', ',', author)  # Remove birth dates
                authors.append(author)
            
            # Look for publication info in 264 field
            pub_match = re.search(r'=264\s+\d\d\s+\$a([^$]+)\$b([^$]+)\$c([^$]+)', marc_data)
            if pub_match:
                place = pub_match.group(1).strip()
                publisher = pub_match.group(2).strip()
                pub_date = pub_match.group(3).strip()
                # Extract year from publication date
                year_match = re.search(r'(\d{4})', pub_date)
                if year_match:
                    year = year_match.group(1)
            
            # Look for ISBN in 020 field
            isbn_match = re.search(r'=020\s+\d\d\s+\$a([^$]+)', marc_data)
            if isbn_match:
                isbn = isbn_match.group(1).strip()
                # Clean up ISBN
                isbn = re.sub(r'\s*\(.+?\)', '', isbn)  # Remove qualifiers in parentheses
            
            # Look for ISSN in 022 field
            issn_match = re.search(r'=022\s+\d\d\s+\$a([^$]+)', marc_data)
            if issn_match:
                issn = issn_match.group(1).strip()
            
            # Look for series in 490 field
            series_match = re.search(r'=490\s+\d\d\s+\$a([^$]+)', marc_data)
            if series_match:
                series = series_match.group(1).strip()
            
            # Look for language in 041 field
            lang_match = re.search(r'=041\s+\d\d\s+\$a([^$]+)', marc_data)
            if lang_match:
                language_code = lang_match.group(1).strip()
                # Map language code to name if needed
                language_map = {
                    'eng': 'English',
                    'ger': 'German',
                    'fre': 'French',
                    'spa': 'Spanish',
                    'ita': 'Italian'
                    # Add more as needed
                }
                language = language_map.get(language_code, language_code)
            
            # Look for subjects in 650 fields
            subject_matches = re.finditer(r'=650\s+\d\d\s+\$a([^$]+)', marc_data)
            for match in subject_matches:
                subject = match.group(1).strip()
                subjects.append(subject)
            
            # Look for abstract in 520 field
            abstract_match = re.search(r'=520\s+\d\d\s+\$a([^$]+)', marc_data)
            if abstract_match:
                abstract = abstract_match.group(1).strip()
            
            # Determine format from leader
            leader_match = re.search(r'=LDR\s+(\d{24})', marc_data)
            if leader_match:
                leader = leader_match.group(1)
                type_code = leader[6] if len(leader) > 6 else '?'
                if type_code == 'a':
                    format_str = "Book"
                elif type_code == 's':
                    format_str = "Serial"
                elif type_code == 'm':
                    format_str = "Computer File"
                else:
                    format_str = "Unknown"
            
            # For journal articles, try to extract journal info
            if format_str == "Serial" or issn:
                # Look for journal title in 773 field (host item entry)
                journal_match = re.search(r'=773\s+\d\d\s+\$t([^$]+)', marc_data)
                if journal_match:
                    journal = journal_match.group(1).strip()
                    
                    # Look for volume
                    volume_match = re.search(r'=773\s+\d\d\s+.*\$g.*?(\d+)', marc_data)
                    if volume_match:
                        volume = volume_match.group(1)
                    
                    # Look for issue
                    issue_match = re.search(r'=773\s+\d\d\s+.*\$g.*?no\.\s*(\d+)', marc_data)
                    if issue_match:
                        issue = issue_match.group(1)
                    
                    # Look for pages
                    pages_match = re.search(r'=773\s+\d\d\s+.*\$g.*?p\.\s*(\d+(?:-\d+)?)', marc_data)
                    if pages_match:
                        pages = pages_match.group(1)
        
        except Exception as e:
            logger.error(f"Error parsing MARC data for record {record.id}: {e}")
            # Fallback to RIS if MARC parsing fails
            logger.warning(f"MARC parsing failed for record {record.id}, falling back to RIS")
            return self.get_record_with_ris(record)
        
        # Create a new record with data from MARC
        enhanced_record = BiblioRecord(
            id=record.id,
            title=title or record.title or "Unknown Title",
            authors=authors or record.authors or [],
            year=year or record.year,
            publisher_name=publisher or record.publisher_name,
            place_of_publication=place or record.place_of_publication,
            isbn=isbn or record.isbn,
            issn=issn or record.issn,
            
            # For journal articles
            journal_title=journal,
            volume=volume,
            issue=issue,
            pages=pages,
            
            # For books
            series=series,
            
            # Store MARC data for reference
            raw_data=marc_data,
            
            subjects=subjects or record.subjects or [],
            abstract=abstract or record.abstract,
            language=language or record.language,
            
            # Store record format
            format=format_str or record.format
        )
        
        logger.debug(f"Enhanced record created from MARC for {record.id}: {enhanced_record}")
        return enhanced_record
    
    def get_record_with_ris(self, record):
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
        detailed_record = self.client.get_record_with_html(record.id)
        
        # Get RIS export data - IxTheo only supports RIS and MARC directly
        logger.debug(f"Getting RIS export for record: {record.id}")
        ris_data = self.client.get_export_data(record.id, "RIS")
        
        # Debug output for RIS data
        if ris_data:
            logger.debug(f"RIS data received for {record.id}:")
            logger.debug(ris_data)
        else:
            logger.debug(f"No RIS data received for {record.id}")
            
            # If we have a detailed record but no RIS data
            if detailed_record:
                logger.debug(f"Using detailed record (no RIS data) for {record.id}")
                detailed_record.raw_data = detailed_record.raw_data or record.raw_data
                return detailed_record
            
            # If all else fails, return the original record
            logger.debug(f"No enhanced data available, returning original record for {record.id}")
            return record
        
        # Ensure RIS data is properly encoded as UTF-8
        if isinstance(ris_data, bytes):
            try:
                ris_data = ris_data.decode('utf-8')
            except UnicodeDecodeError:
                # Try different encodings if UTF-8 fails
                try:
                    ris_data = ris_data.decode('latin-1')
                except UnicodeDecodeError:
                    ris_data = ris_data.decode('utf-8', errors='replace')
        
        # Fix common umlaut encodings if they appear as escape sequences
        umlaut_replacements = {
            r'\u00fc': 'ü',
            r'\u00e4': 'ä',
            r'\u00f6': 'ö',
            r'\u00dc': 'Ü',
            r'\u00c4': 'Ä',
            r'\u00d6': 'Ö',
            r'\u00df': 'ß'
        }
        
        for escape_seq, umlaut in umlaut_replacements.items():
            ris_data = ris_data.replace(escape_seq, umlaut)
        
        # Extract data from RIS to populate record fields
        if ris_data:
            # Parse RIS data to extract key fields
            record_type = None
            title = None
            authors = []
            editors = []
            translators = []
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
            urls = []
            abstract = None
            
            # Simple RIS parser
            for line in ris_data.splitlines():
                line = line.strip()
                if not line or "  - " not in line:
                    continue
                
                parts = line.split("  - ", 1)
                if len(parts) != 2:
                    continue
                    
                tag, value = parts[0].strip(), parts[1].strip()
                # Ensure the value is properly encoded
                value = self._ensure_utf8(value)
                logger.debug(f"Processing RIS tag: {tag} with value: {value}")
                
                if tag == "TY":  # Type
                    record_type = value
                    logger.debug(f"Record type set to: {record_type}")
                elif tag in ["TI", "T1"]:  # Title
                    title = value
                    logger.debug(f"Title set to: {title}")
                elif tag == "AU" or tag == "A1":  # Author
                    authors.append(value)
                    logger.debug(f"Added author: {value}")
                elif tag == "A2" or tag == "ED":  # Editor
                    editors.append(value)
                    logger.debug(f"Added editor: {value}")
                elif tag == "A4":  # Translator
                    translators.append(value)
                    logger.debug(f"Added translator: {value}")
                elif tag in ["PY", "Y1"]:  # Year
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
                elif tag in ["T2", "JF", "JO", "JA"]:  # Secondary Title/Journal
                    if record_type == "JOUR":
                        journal = value
                        logger.debug(f"Journal title set to: {value}")
                    else:
                        # Process book chapter information from T2
                        # First, extract editors from T2 field if present
                        if tag == "T2":
                            # Look for patterns like "Author Name (edt)"
                            t2_value = value
                            editor_matches = re.finditer(r'([^,]+)(?:,\s+([^(]+))?\s+(?:\d{4}-)?\s*\(edt\)', t2_value)
                            for editor_match in editor_matches:
                                if editor_match.group(2):  # We have last name, first name
                                    editor_name = f"{editor_match.group(1).strip()}, {editor_match.group(2).strip()}"
                                else:  # Just have a name
                                    editor_name = editor_match.group(1).strip()
                                
                                # Remove birth dates if present
                                editor_name = re.sub(r'\s+\d{4}-(?:\d{4})?', '', editor_name)
                                
                                if editor_name not in editors:
                                    editors.append(editor_name)
                                    logger.debug(f"Extracted editor from T2: {editor_name}")
                            
                            # Now remove the editor information from the T2 field to get just the book title
                            clean_series_title = re.sub(r'([^,]+)(?:,\s+[^(]+)?\s+(?:\d{4}-)?\s*\(edt\),\s*', '', t2_value)
                            # Remove any trailing comma + space if it's at the beginning
                            clean_series_title = re.sub(r'^,\s*', '', clean_series_title)
                            series_title = clean_series_title
                            logger.debug(f"Series/Book title (cleaned) set to: {series_title}")
                        else:
                            # For other secondary title fields, just use as is
                            series_title = value
                            logger.debug(f"Series/Book title set to: {value}")
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
                elif tag == "UR":  # URL
                    urls.append(value)
                    logger.debug(f"Added URL: {value}")
                elif tag == "N2" or tag == "AB":  # Abstract
                    abstract = value
                    logger.debug(f"Abstract set to: {abstract}")
            
            # Create page range without "Pages " prefix
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
            elif record_type == "THES":
                format_str = "Thesis"
            elif record_type == "CONF":
                format_str = "Conference Paper"
            elif record_type == "RPRT":
                format_str = "Report"
            
            # Create a new record with data from both RIS and detailed record
            enhanced_record = BiblioRecord(
                id=record.id,
                title=title or (detailed_record.title if detailed_record else record.title) or "Unknown Title",
                authors=authors or (detailed_record.authors if detailed_record else record.authors) or [],
                editors=editors or (detailed_record.editors if detailed_record else []),
                translators=translators or [],
                year=year or (detailed_record.year if detailed_record else record.year),
                publisher_name=publisher or (detailed_record.publisher_name if detailed_record else None),
                place_of_publication=place or (detailed_record.place_of_publication if detailed_record else None),
                isbn=isbn or (detailed_record.isbn if detailed_record else None),
                issn=issn or (detailed_record.issn if detailed_record else None),
                urls=urls or [],
                
                # For journal articles
                journal_title=journal if record_type == "JOUR" else None,
                volume=volume,
                issue=issue,
                pages=pages,
                
                # For book chapters, store book title in series field
                series=series_title,
                
                raw_data=ris_data,
                
                # Create proper extent field without "Pages " prefix
                extent=pages if pages else None,
                
                subjects=detailed_record.subjects if detailed_record and detailed_record.subjects else [],
                abstract=abstract or (detailed_record.abstract if detailed_record and detailed_record.abstract else None),
                language=language,
                doi=doi,
                
                # Store record type to inform downstream formatting
                format=format_str if format_str else (record_type if record_type else None)
            )
            
            logger.debug(f"Enhanced record created: {enhanced_record}")
            
            # Log complete record details
            logger.debug(f"Enhanced record details for {record.id}:")
            logger.debug(f"  Title: {enhanced_record.title}")
            logger.debug(f"  Authors: {enhanced_record.authors}")
            logger.debug(f"  Editors: {enhanced_record.editors}")
            logger.debug(f"  Translators: {enhanced_record.translators}")
            logger.debug(f"  Year: {enhanced_record.year}")
            logger.debug(f"  Format/Type: {enhanced_record.format}")
            logger.debug(f"  Publisher: {enhanced_record.publisher_name}")
            logger.debug(f"  Place: {enhanced_record.place_of_publication}")
            logger.debug(f"  ISBN: {enhanced_record.isbn}")
            logger.debug(f"  ISSN: {enhanced_record.issn}")
            logger.debug(f"  Journal: {enhanced_record.journal_title}")
            logger.debug(f"  Series/Book title: {enhanced_record.series}")
            logger.debug(f"  URLs: {enhanced_record.urls}")
            logger.debug(f"  Volume: {enhanced_record.volume}")
            logger.debug(f"  Issue: {enhanced_record.issue}")
            logger.debug(f"  Pages: {enhanced_record.pages}")
            logger.debug(f"  DOI: {enhanced_record.doi}")
            
            return enhanced_record

    def _ensure_utf8(self, text):
        """
        Ensure that text is properly encoded as UTF-8, specifically handling umlauts
        
        Args:
            text: The text to ensure is UTF-8 encoded
            
        Returns:
            UTF-8 encoded text with proper handling of umlauts
        """
        if not text:
            return text
            
        # Handle specific umlaut escape sequences
        escape_chars = {
            r'\u00fc': 'ü',
            r'\u00e4': 'ä',
            r'\u00f6': 'ö',
            r'\u00dc': 'Ü',
            r'\u00c4': 'Ä',
            r'\u00d6': 'Ö',
            r'\u00df': 'ß',
            # Add additional special characters as needed
        }
        
        for escape_seq, char in escape_chars.items():
            text = text.replace(escape_seq, char)
        
        # Handle unicode escape sequences like \uXXXX
        # This regex finds patterns like \uXXXX where X is a hexadecimal digit
        def replace_unicode_escapes(match):
            try:
                # Convert the 4-digit hex code to an integer and then to a character
                return chr(int(match.group(1), 16))
            except:
                return match.group(0)  # Return the original match if conversion fails
        
        text = re.sub(r'\\u([0-9a-fA-F]{4})', replace_unicode_escapes, text)
        
        return text

# Define IxTheo endpoint information
IXTHEO_ENDPOINTS = {
    "ris": {
        "name": "Index Theologicus (IxTheo) - RIS format",
        "base_url": "https://ixtheo.de",
        "format": "ris"
    },
    "marc": {
        "name": "Index Theologicus (IxTheo) - MARC format",
        "base_url": "https://ixtheo.de",
        "format": "marc"
    },
    "html": {
        "name": "Index Theologicus (IxTheo) - HTML parsing",
        "base_url": "https://ixtheo.de",
        "format": "html"
    }
}