#!/usr/bin/env python3
# library_search.py
"""
Library Search - Command-line tool for searching library SRU and OAI-PMH endpoints

This script provides a command-line interface to search for books, journals, and
other materials across multiple library endpoints using both SRU and OAI-PMH protocols.
It also supports searching local Zotero libraries and exporting results in various formats.
"""

import argparse
import sys
import json
import logging
import time
import re
import os
import sqlite3
import urllib.parse
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import unicodedata

# Import library modules
from sru_library import SRUClient, BiblioRecord, SRU_ENDPOINTS
from oai_pmh_library import OAIClient, OAI_ENDPOINTS
from ixtheo_library import IxTheoSearchHandler, IXTHEO_ENDPOINTS

# Try to import optional dependencies
try:
    import pyzotero
    from pyzotero.zotero import Zotero
    ZOTERO_API_AVAILABLE = True
except ImportError:
    ZOTERO_API_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("library_search")


def list_endpoints(protocol=None):
    """Display information about available endpoints."""
    if protocol and protocol not in ['sru', 'oai', 'zotero', 'ixtheo']:
        logger.error(f"Unknown protocol: {protocol}")
        logger.info("Valid protocols are: sru, oai, zotero, ixtheo")
        return
    
    if not protocol or protocol == 'sru':
        print("\\nAvailable SRU Endpoints:\\n")
        print(f"{'ID':<10} {'Name':<40} {'Version':<10}")
        print("-" * 60)
        
        for id, info in SRU_ENDPOINTS.items():
            print(f"{id:<10} {info['name']:<40} {info.get('version', '1.1'):<10}")
    
    if not protocol or protocol == 'oai':
        print("\\nAvailable OAI-PMH Endpoints:\\n")
        print(f"{'ID':<12} {'Name':<40} {'Metadata Format':<16}")
        print("-" * 70)
        
        for id, info in OAI_ENDPOINTS.items():
            print(f"{id:<12} {info['name']:<40} {info.get('default_metadata_prefix', 'oai_dc'):<16}")
    
    if not protocol or protocol == 'zotero':
        print("\\nZotero Search:")
        print("-" * 60)
        print("To search a local Zotero database:")
        print("  --protocol zotero --zotero-path /path/to/zotero/zotero.sqlite")
        print("\\nTo search a Zotero library via API (requires API key):")
        print("  --protocol zotero --zotero-api-key YOUR_API_KEY --zotero-library-id LIBRARY_ID --zotero-library-type [user|group]")
    
    if not protocol or protocol == 'ixtheo':
        print("\\nIxTheo (Index Theologicus) Endpoint:")
        print("-" * 60)
        print(f"{'ID':<10} {'Name':<40} {'Description':<30}")
        print("-" * 80)
        
        for id, info in IXTHEO_ENDPOINTS.items():
            print(f"{id:<10} {info['name']:<40} {info['description']:<30}")
        
        print("\\nAvailable formats for filtering:")
        for id, info in IXTHEO_ENDPOINTS.items():
            if info.get('formats'):
                print(f"  {', '.join(info['formats'])}")
                break
        
        print("\\nAvailable languages for filtering:")
        for id, info in IXTHEO_ENDPOINTS.items():
            if info.get('languages'):
                print(f"  {', '.join(info['languages'])}")
                break
    
    print("\\nUse --info <endpoint_id> for more details about a specific endpoint.")


def show_endpoint_info(endpoint_id):
    """Show detailed information about a specific endpoint."""
    # Check SRU endpoints
    if endpoint_id in SRU_ENDPOINTS:
        info = SRU_ENDPOINTS[endpoint_id]
        print(f"\\n{info['name']} ({endpoint_id}) - SRU Protocol")
        print("=" * 50)
        print(f"URL: {info['url']}")
        print(f"Default Schema: {info.get('default_schema', 'None')}")
        print(f"SRU Version: {info.get('version', '1.1')}")
        print(f"Description: {info.get('description', 'No description available')}")
        
        print("\\nExample Queries:")
        for query_type, example in info.get('examples', {}).items():
            if isinstance(example, dict):
                # For advanced queries stored as dictionaries
                example_str = " AND ".join([f"{k}={v}" for k, v in example.items()])
            else:
                example_str = example
            print(f"  {query_type}: {example_str}")
        
        return
    
    # Check OAI-PMH endpoints
    if endpoint_id in OAI_ENDPOINTS:
        info = OAI_ENDPOINTS[endpoint_id]
        print(f"\\n{info['name']} ({endpoint_id}) - OAI-PMH Protocol")
        print("=" * 50)
        print(f"URL: {info['url']}")
        print(f"Default Metadata Format: {info.get('default_metadata_prefix', 'oai_dc')}")
        print(f"Description: {info.get('description', 'No description available')}")
        
        if info.get('sets'):
            print("\\nAvailable Sets:")
            for set_id, set_desc in info['sets'].items():
                print(f"  {set_id}: {set_desc}")
        
        print("\\nUsage Example:")
        print(f"  --endpoint {endpoint_id} --title 'Python' --protocol oai")
        print(f"  --endpoint {endpoint_id} --set {list(info.get('sets', {}).keys())[0] if info.get('sets') else 'NONE'} --protocol oai")
        
        return
    
    # Check IxTheo endpoints
    if endpoint_id in IXTHEO_ENDPOINTS:
        info = IXTHEO_ENDPOINTS[endpoint_id]
        print(f"\\n{info['name']} ({endpoint_id}) - Specialized Theological Database")
        print("=" * 60)
        print(f"URL: {info['url']}")
        print(f"Description: {info['description']}")
        
        if info.get('formats'):
            print("\\nAvailable Formats for Filtering:")
            print(f"  {', '.join(info['formats'])}")
        
        if info.get('languages'):
            print("\\nAvailable Languages for Filtering:")
            print(f"  {', '.join(info['languages'])}")
        
        if info.get('export_formats'):
            print("\\nSupported Export Formats:")
            print(f"  {', '.join(info['export_formats'])}")
        
        print("\\nUsage Example:")
        print(f"  --endpoint {endpoint_id} --title 'Bible' --protocol ixtheo --format-filter 'Article'")
        print(f"  --endpoint {endpoint_id} --author 'Smith' --protocol ixtheo --language-filter 'English' --get-export")
        
        return
    
    # If it's Zotero, show Zotero info
    if endpoint_id.lower() == 'zotero':
        print("\\nZotero - Local database or API")
        print("=" * 50)
        print("Zotero is a reference management software to manage bibliographic data.")
        print("\\nTo search a local Zotero database:")
        print("  --protocol zotero --zotero-path /path/to/zotero/zotero.sqlite --title 'Python'")
        print("\\nTo search a Zotero library via API (requires API key):")
        print("  --protocol zotero --zotero-api-key YOUR_API_KEY --zotero-library-id LIBRARY_ID --zotero-library-type [user|group] --title 'Python'")
        print("\\nFor more information on Zotero API, visit: https://www.zotero.org/support/dev/web_api/v3/start")
        return
    
    # Not found in either
    print(f"Error: Unknown endpoint '{endpoint_id}'")
    print("Use --list to see available endpoints")


def build_sru_query(args, endpoint_id):
    """
    Build an appropriate SRU query string for the given endpoint and search criteria.
    
    Args:
        args: Command line arguments
        endpoint_id: ID of the SRU endpoint
        
    Returns:
        Query string formatted for the specified endpoint
    """
    # Get endpoint info
    endpoint_info = SRU_ENDPOINTS.get(endpoint_id, {})
    examples = endpoint_info.get('examples', {})
    
    # For BNF endpoint, ensure we're using the correct schema
    if endpoint_id == 'bnf' and not args.schema:
        args.schema = 'dublincore'  # Override default schema for BNF
    
    # Use the examples as templates for how to format queries for this endpoint
    if args.isbn:
        if 'isbn' in examples:
            # Extract the format from the example
            example = examples['isbn']
            if '=' in example:
                parts = example.split('=')
                prefix = parts[0]
                # Check if the value is quoted in the example
                if len(parts) > 1 and (parts[1].startswith('"') or parts[1].startswith("'")):
                    return f"{prefix}=\"{args.isbn}\""
                else:
                    return f"{prefix}={args.isbn}"
        
        # Default formats if no example is available
        if endpoint_id == 'dnb':
            return f"ISBN={args.isbn}"
        elif endpoint_id == 'bnf':
            return f"bib.isbn any \"{args.isbn}\""
        else:
            return f"isbn={args.isbn}"
    
    if args.issn:
        if 'issn' in examples:
            # Extract the format from the example
            example = examples['issn']
            if '=' in example:
                parts = example.split('=')
                prefix = parts[0]
                # Check if the value is quoted in the example
                if len(parts) > 1 and (parts[1].startswith('"') or parts[1].startswith("'")):
                    return f"{prefix}=\"{args.issn}\""
                else:
                    return f"{prefix}={args.issn}"
        
        # Default formats if no example is available
        if endpoint_id == 'dnb' or endpoint_id == 'zdb':
            return f"ISS={args.issn}"
        elif endpoint_id == 'bnf':
            return f"bib.issn any \"{args.issn}\""
        else:
            return f"issn={args.issn}"
    
    if args.title:
        if 'title' in examples:
            # Extract the format from the example
            example = examples['title']
            if '=' in example:
                parts = example.split('=')
                prefix = parts[0]
                # Check if the value is quoted in the example
                if len(parts) > 1 and (parts[1].startswith('"') or parts[1].startswith("'")):
                    return f"{prefix}=\"{args.title}\""
                else:
                    return f"{prefix}={args.title}"
            else:
                # Handle "all" syntax (BNF)
                if ' all ' in example:
                    parts = example.split(' all ')
                    prefix = parts[0]
                    return f"{prefix} all \"{args.title}\""
                elif ' any ' in example:
                    parts = example.split(' any ')
                    prefix = parts[0]
                    return f"{prefix} any \"{args.title}\""
        
        # Default formats if no example is available
        if endpoint_id == 'dnb':
            return f"TIT={args.title}"
        elif endpoint_id == 'bnf':
            return f"bib.title any \"{args.title}\""
        else:
            return f"title=\"{args.title}\""
    
    if args.author:
        if 'author' in examples:
            # Extract the format from the example
            example = examples['author']
            if '=' in example:
                parts = example.split('=')
                prefix = parts[0]
                # Check if the value is quoted in the example
                if len(parts) > 1 and (parts[1].startswith('"') or parts[1].startswith("'")):
                    return f"{prefix}=\"{args.author}\""
                else:
                    return f"{prefix}={args.author}"
            else:
                # Handle "all" syntax (BNF)
                if ' all ' in example:
                    parts = example.split(' all ')
                    prefix = parts[0]
                    return f"{prefix} all \"{args.author}\""
                elif ' any ' in example:
                    parts = example.split(' any ')
                    prefix = parts[0]
                    return f"{prefix} any \"{args.author}\""
        
        # Default formats if no example is available
        if endpoint_id == 'dnb':
            return f"PER={args.author}"
        elif endpoint_id == 'bnf':
            return f"bib.author any \"{args.author}\""
        else:
            return f"author=\"{args.author}\""
    
    if args.year:
        if endpoint_id == 'dnb':
            return f"JHR={args.year}"
        elif endpoint_id == 'bnf':
            return f"bib.date any \"{args.year}\""
        else:
            return f"date={args.year}"
    
    # Advanced query logic remains unchanged
    if args.advanced:
        try:
            # Parse advanced query
            if isinstance(args.advanced, str):
                # If it's already a string, use it directly
                return args.advanced
            elif isinstance(args.advanced, dict):
                # If it's a dictionary, format according to endpoint
                if 'advanced' in examples and isinstance(examples['advanced'], dict):
                    # Use the format from the example
                    adv_example = examples['advanced']
                    adv_keys = list(adv_example.keys())
                    
                    # Map query keys to endpoint-specific keys if possible
                    query_parts = []
                    for k, v in args.advanced.items():
                        # Try to find a matching key in the example
                        endpoint_key = None
                        for ex_key in adv_keys:
                            if k.lower() in ex_key.lower() or ex_key.lower() in k.lower():
                                endpoint_key = ex_key
                                break
                        
                        if endpoint_key:
                            query_parts.append(f"{endpoint_key}={v}")
                        else:
                            # Use the key as-is if no match found
                            query_parts.append(f"{k}={v}")
                    
                    return " AND ".join(query_parts)
                else:
                    # Default format
                    if endpoint_id == 'dnb':
                        return " AND ".join([f"{k}={v}" for k, v in args.advanced.items()])
                    elif endpoint_id == 'bnf':
                        return " and ".join([f"bib.{k} any \"{v}\"" for k, v in args.advanced.items()])
                    else:
                        return " and ".join([f"{k}=\"{v}\"" for k, v in args.advanced.items()])
            else:
                logger.error(f"Invalid advanced query format: {args.advanced}")
                return ""
        except Exception as e:
            logger.error(f"Error parsing advanced query: {e}")
            return ""
    
    # If no specific search criteria were provided
    logger.error("No search criteria specified")
    return ""


def bibtex_escape(text):
    """Escape special characters for BibTeX."""
    if not text:
        return ""
    
    # Replace special characters with LaTeX equivalents
    replacements = {
        '&': r'\&',
        '%': r'\%',
        '$': r'\$',
        '#': r'\#',
        '_': r'\_',
        '{': r'\{',
        '}': r'\}',
        '~': r'\textasciitilde{}',
        '^': r'\textasciicircum{}',
        '\\': r'\textbackslash{}',
        '<': r'\textless{}',
        '>': r'\textgreater{}'
    }
    
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)
    
    # Handle accented characters by maintaining them (Unicode in BibTeX)
    return text


def clean_key(text):
    """Generate a clean citation key from text."""
    if not text:
        return "unknown"
    
    # Normalize and remove accents
    text = unicodedata.normalize('NFKD', text)
    text = ''.join([c for c in text if not unicodedata.combining(c)])
    
    # Remove non-alphanumeric characters and convert to lowercase
    text = re.sub(r'[^\w\s]', '', text).lower()
    
    # Replace spaces with underscores and truncate
    text = re.sub(r'\s+', '_', text)[:30]
    
    return text


def format_record_bibtex(record):
    """
    Format a BiblioRecord as BibTeX.
    
    Args:
        record: BiblioRecord object
        
    Returns:
        BibTeX formatted string
    """
    # Use the new bibtex_from_record function from sru_library
    from sru_library import bibtex_from_record
    return bibtex_from_record(record)


def format_record_ris(record):
    """
    Format a BiblioRecord as RIS (Research Information Systems) format.
    
    Args:
        record: BiblioRecord object
        
    Returns:
        RIS formatted string
    """
    # Determine record type
    if record.issn:
        record_type = "JOUR"  # Journal article
    elif record.series:
        record_type = "CHAP"  # Book chapter
    else:
        record_type = "BOOK"  # Book
    
    # Start building RIS entry
    ris = ["TY  - " + record_type]
    
    # Add ID
    ris.append(f"ID  - {record.id}")
    
    # Add title
    ris.append(f"TI  - {record.title}")
    
    # Add authors
    for author in record.authors:
        # For RIS, typically format is "lastname, firstname"
        if ',' in author:
            ris.append(f"AU  - {author}")
        else:
            # Convert "firstname lastname" to "lastname, firstname"
            parts = author.split()
            if len(parts) > 1:
                last_name = parts[-1]
                first_names = ' '.join(parts[:-1])
                ris.append(f"AU  - {last_name}, {first_names}")
            else:
                ris.append(f"AU  - {author}")

    # Add editors if present
    for editor in record.editors:
        # For RIS, typically format is "lastname, firstname"
        if ',' in editor:
            ris.append(f"ED  - {editor}")
        else:
            # Convert "firstname lastname" to "lastname, firstname"
            parts = editor.split()
            if len(parts) > 1:
                last_name = parts[-1]
                first_names = ' '.join(parts[:-1])
                ris.append(f"ED  - {last_name}, {first_names}")
            else:
                ris.append(f"ED  - {editor}")
    
    # Add year
    if record.year:
        ris.append(f"PY  - {record.year}")
        ris.append(f"Y1  - {record.year}///")  # Year with // for month/day
    
    # Add publisher
    if record.publisher_name:
        ris.append(f"PB  - {record.publisher_name}")
    
    # Add place of publication
    if record.place_of_publication:
        ris.append(f"CY  - {record.place_of_publication}")
    
    # Add ISBN
    if record.isbn:
        ris.append(f"SN  - {record.isbn}")
    
    # Add ISSN
    if record.issn:
        ris.append(f"SN  - {record.issn}")
    
    # Add edition
    if record.edition:
        ris.append(f"ET  - {record.edition}")
    
    # Add series
    if record.series:
        ris.append(f"T2  - {record.series}")
    
    # Add language
    if record.language:
        ris.append(f"LA  - {record.language}")
    
    # Add URLs
    for url in record.urls:
        ris.append(f"UR  - {url}")
    
    # Add abstract
    if record.abstract:
        ris.append(f"AB  - {record.abstract}")
    
    # Add keywords (from subjects)
    for subject in record.subjects:
        ris.append(f"KW  - {subject}")
    
    # Add note with format info
    if record.format:
        ris.append(f"N1  - Format: {record.format}")
    
    # Add extent information
    if record.extent:
        ris.append(f"N1  - Extent: {record.extent}")
    
    # End record
    ris.append("ER  - ")
    
    return "\n".join(ris)


def format_record(record, format_type='text', include_raw=False, verbose=False):
    """
    Format a bibliographic record for display.
    
    Args:
        record: BiblioRecord object
        format_type: 'text', 'json', 'bibtex', 'ris', or 'zotero'
        include_raw: Whether to include raw XML data
        verbose: Whether to show detailed debugging info
        
    Returns:
        Formatted record string
    """
    if format_type == 'json':
        data = record.to_dict()
        if include_raw or verbose:
            data['raw_data'] = record.raw_data
        return json.dumps(data, indent=2)
    
    elif format_type == 'bibtex':
        return format_record_bibtex(record)
    
    elif format_type == 'ris':
        return format_record_ris(record)
    
    elif format_type == 'zotero':
        # For Zotero format, we'll use JSON with specific Zotero-compatible fields
        zotero_data = {
            "itemType": "book" if not record.issn else "journalArticle",
            "title": record.title,
            "creators": [],
            "date": record.year,
            "publisher": record.publisher_name,
            "place": record.place_of_publication,
            "ISBN": record.isbn,
            "ISSN": record.issn,
            "series": record.series,
            "edition": record.edition,
            "language": record.language,
            "url": record.urls[0] if record.urls else "",
            "abstractNote": record.abstract,
            "tags": [{"tag": subject} for subject in record.subjects],
            "notes": []
        }
        
        # Format creators for Zotero (need firstName, lastName fields)
        for author in record.authors:
            creator = {}
            if ',' in author:
                parts = author.split(',', 1)
                creator = {
                    "creatorType": "author",
                    "lastName": parts[0].strip(),
                    "firstName": parts[1].strip() if len(parts) > 1 else ""
                }
            else:
                parts = author.split()
                if len(parts) > 1:
                    creator = {
                        "creatorType": "author",
                        "lastName": parts[-1],
                        "firstName": ' '.join(parts[:-1])
                    }
                else:
                    creator = {
                        "creatorType": "author",
                        "lastName": author,
                        "firstName": ""
                    }
            zotero_data["creators"].append(creator)

        # Format editors for Zotero
        for editor in record.editors:
            creator = {}
            if ',' in editor:
                parts = editor.split(',', 1)
                creator = {
                    "creatorType": "editor",
                    "lastName": parts[0].strip(),
                    "firstName": parts[1].strip() if len(parts) > 1 else ""
                }
            else:
                parts = editor.split()
                if len(parts) > 1:
                    creator = {
                        "creatorType": "editor",
                        "lastName": parts[-1],
                        "firstName": ' '.join(parts[:-1])
                    }
                else:
                    creator = {
                        "creatorType": "editor",
                        "lastName": editor,
                        "firstName": ""
                    }
            zotero_data["creators"].append(creator)
        
        return json.dumps(zotero_data, indent=2)
    
    # Default to text format with improved layout
    result = []
    result.append(f"Title: {record.title}")
    
    if record.authors:
        # Properly format authors list
        result.append(f"Author(s): {', '.join(record.authors)}")

    if record.editors:
        # Properly format editors list
        result.append(f"Editor(s): {', '.join(record.editors)}")
    
    if record.year:
        result.append(f"Year: {record.year}")
    
    # Format place and publisher separately
    if record.place_of_publication:
        result.append(f"Place of Publication: {record.place_of_publication}")
    
    if record.publisher_name:
        result.append(f"Publisher: {record.publisher_name}")
    
    if record.edition:
        result.append(f"Edition: {record.edition}")
    
    if record.series:
        result.append(f"Series: {record.series}")
    
    if record.extent:
        result.append(f"Extent: {record.extent}")
    
    if record.isbn:
        result.append(f"ISBN: {record.isbn}")
    
    if record.issn:
        result.append(f"ISSN: {record.issn}")
    
    if record.language:
        result.append(f"Language: {record.language}")
    
    if record.subjects:
        # Limit to 5 subjects but indicate if there are more
        if len(record.subjects) > 5:
            subjects_text = ", ".join(record.subjects[:5]) + f", ... ({len(record.subjects) - 5} more)"
        else:
            subjects_text = ", ".join(record.subjects)
        result.append(f"Subjects: {subjects_text}")
    
    if record.urls:
        # Format URLs for better readability (one per line if there are multiple)
        if len(record.urls) == 1:
            result.append(f"URL: {record.urls[0]}")
        elif len(record.urls) > 1:
            result.append("URLs:")
            for url in record.urls:
                result.append(f"  - {url}")
    
    if record.abstract:
        # Truncate long abstracts
        abstract = record.abstract
        if len(abstract) > 300:
            abstract = abstract[:297] + "..."
        result.append(f"Abstract: {abstract}")
    
    # Show raw data in verbose mode
    if include_raw or verbose:
        result.append("\nRaw Data:")
        raw_data = record.raw_data
        # Limit raw data length to prevent overwhelming the terminal
        if raw_data and len(raw_data) > 2000:
            raw_data = raw_data[:1997] + "..."
        result.append(raw_data)
    
    return "\n".join(result)

def search_ixtheo_endpoint(args):
    """
    Search the IxTheo endpoint with the given parameters.
    
    Args:
        args: Command line arguments
        
    Returns:
        True if search was successful, False otherwise
    """
    endpoint_id = args.endpoint
    if endpoint_id not in IXTHEO_ENDPOINTS:
        logger.error(f"Unknown IxTheo endpoint: {endpoint_id}")
        logger.info("Use --list --protocol ixtheo to see available IxTheo endpoints")
        return False
    
    # Get endpoint info
    endpoint_info = IXTHEO_ENDPOINTS[endpoint_id]
    logger.info(f"Using {endpoint_info['name']} ({endpoint_id}) via IxTheo protocol")
    
    # Create IxTheo search handler
    ixtheo_handler = IxTheoSearchHandler(
        timeout=args.timeout,
        debug=args.verbose,
        verify_ssl=not args.no_verify_ssl
    )
    
    # Perform search
    logger.info(f"Searching IxTheo with parameters:")
    if args.title:
        logger.info(f"  Title: {args.title}")
    if args.author:
        logger.info(f"  Author: {args.author}")
    if args.subject:
        logger.info(f"  Subject: {args.subject}")
    if args.format_filter:
        logger.info(f"  Format filter: {args.format_filter}")
    if args.language_filter:
        logger.info(f"  Language filter: {args.language_filter}")
    
    start_time = time.time()
    
    try:
        # Execute search
        total_results, records = ixtheo_handler.search(
            query=args.advanced,
            title=args.title,
            author=args.author,
            subject=args.subject,
            max_results=args.max_records,
            format_filter=args.format_filter,
            language_filter=args.language_filter
        )
        
        end_time = time.time()
        search_time = end_time - start_time
        
        if total_results == 0 or not records:
            logger.warning("No results found")
            return False
        
        logger.info(f"Found {total_results} results, showing {len(records)} ({search_time:.2f} seconds)")
        
        # If we need to get export data for each record
        if args.get_export:
            logger.info(f"Retrieving {args.format.upper()} export data for each record...")
            
            # Map output format to IxTheo export format
            export_format_map = {
                'bibtex': 'BibTeX',
                'ris': 'RIS',
                'marc': 'MARC',
                'text': 'BibTeX',  # Default for text output
                'json': 'BibTeX',  # Default for JSON output
                'zotero': 'BibTeX'  # Default for Zotero output
            }
            
            export_format = export_format_map.get(args.format.lower(), 'BibTeX')
            
            # Get export data for each record
            for i, record in enumerate(records):
                if args.verbose:
                    print(f"Getting export data for record {i+1}/{len(records)}...")
                
                records[i] = ixtheo_handler.get_record_with_export(record, export_format)
        
        # Handle output to file if specified
        if args.output:
            return save_results_to_file(records, args.output, args.format, args.raw, args.verbose)
        
        # Display results
        for i, record in enumerate(records, 1):
            print(f"\n--- Result {i} of {len(records)} ---")
            print(format_record(record, args.format, args.raw, args.verbose))
        
        # Show pagination info if applicable
        if total_results > len(records):
            remaining = total_results - len(records)
            if remaining > 0:
                print(f"\nThere are approximately {remaining} more results available.")
                print("Use --max-records to adjust the number of results returned.")
        
        return True
    
    except Exception as e:
        logger.error(f"Error performing IxTheo search: {e}")
        import traceback
        traceback.print_exc()
        return False
    
def search_sru_endpoint(args):
    """
    Search a library SRU endpoint with the given parameters.
    
    Args:
        args: Command line arguments
        
    Returns:
        True if search was successful, False otherwise
    """
    endpoint_id = args.endpoint
    if endpoint_id not in SRU_ENDPOINTS:
        logger.error(f"Unknown SRU endpoint: {endpoint_id}")
        logger.info("Use --list --protocol sru to see available SRU endpoints")
        return False
    
    # Get endpoint info
    endpoint_info = SRU_ENDPOINTS[endpoint_id]
    logger.info(f"Using {endpoint_info['name']} ({endpoint_id}) via SRU protocol")
    
    # Build query
    query = build_sru_query(args, endpoint_id)
    if not query:
        logger.error("Failed to build SRU query")
        return False
    
    # Create SRU client
    sru_client = SRUClient(
        base_url=endpoint_info['url'],
        default_schema=endpoint_info.get('default_schema'),
        version=endpoint_info.get('version', '1.1'),
        timeout=args.timeout
    )
    
    logger.info(f"Searching with SRU query: {query}")
    start_time = time.time()
    
    try:
        # Execute search
        total, records = sru_client.search(
            query=query,
            schema=args.schema,
            max_records=args.max_records,
            start_record=args.start_record
        )
        
        end_time = time.time()
        search_time = end_time - start_time
        
        if total == 0 or not records:
            # Check for diagnostics in case of BNF error with schema
            if endpoint_id == 'bnf' and args.schema == 'marcxchange':
                logger.warning("The BNF catalog reported an issue with the marcxchange schema. Try using a different schema, such as 'dublincore'.")
                logger.info("Example: --endpoint bnf --title \"Python\" --schema dublincore")
            else:
                logger.warning("No results found")
            return False
        
        logger.info(f"Found {total} results, showing {len(records)} ({search_time:.2f} seconds)")
        
        # Handle output to file if specified
        if args.output:
            return save_results_to_file(records, args.output, args.format, args.raw, args.verbose)
        
        # Display results to console
        for i, record in enumerate(records, 1):
            print(f"\n--- Result {i} of {len(records)} ---")
            print(format_record(record, args.format, args.raw, args.verbose))
        
        # Show pagination info if applicable
        if total > len(records):
            remaining = total - (args.start_record - 1 + len(records))
            if remaining > 0:
                print(f"\nThere are {remaining} more results available.")
                print(f"Use --start-record {args.start_record + len(records)} to see the next page.")
        
        return True
    
    except Exception as e:
        logger.error(f"Error performing SRU search: {e}")
        return False


def search_oai_endpoint(args):
    """
    Search a library OAI-PMH endpoint with the given parameters.
    
    Args:
        args: Command line arguments
        
    Returns:
        True if search was successful, False otherwise
    """
    endpoint_id = args.endpoint
    if endpoint_id not in OAI_ENDPOINTS:
        logger.error(f"Unknown OAI-PMH endpoint: {endpoint_id}")
        logger.info("Use --list --protocol oai to see available OAI-PMH endpoints")
        return False
    
    # Get endpoint info
    endpoint_info = OAI_ENDPOINTS[endpoint_id]
    logger.info(f"Using {endpoint_info['name']} ({endpoint_id}) via OAI-PMH protocol")
    
    # Create OAI client
    oai_client = OAIClient(
        base_url=endpoint_info['url'],
        default_metadata_prefix=endpoint_info.get('default_metadata_prefix', 'oai_dc'),
        timeout=args.timeout
    )
    
    # Build search parameters
    metadata_prefix = args.metadata_prefix or endpoint_info.get('default_metadata_prefix', 'oai_dc')
    
    # Determine date parameters
    from_date = None
    until_date = None
    
    if args.from_date:
        from_date = args.from_date
    
    if args.until_date:
        until_date = args.until_date
    
    # Prepare search query if applicable
    search_query = {}
    if args.title:
        search_query['title'] = args.title
    if args.author:
        search_query['author'] = args.author
    if args.isbn:
        search_query['isbn'] = args.isbn
    if args.issn:
        search_query['issn'] = args.issn
    if args.year:
        search_query['year'] = args.year
    
    logger.info(f"Searching OAI-PMH endpoint with:")
    logger.info(f"  Set: {args.set or 'None'}")
    logger.info(f"  From date: {from_date or 'None'}")
    logger.info(f"  Until date: {until_date or 'None'}")
    logger.info(f"  Metadata format: {metadata_prefix}")
    if search_query:
        logger.info(f"  Search terms: {search_query}")
    
    start_time = time.time()
    
    try:
        # Execute search
        total, records = oai_client.search(
            query=search_query,
            metadata_prefix=metadata_prefix,
            set_spec=args.set,
            from_date=from_date,
            until_date=until_date,
            max_results=args.max_records
        )
        
        end_time = time.time()
        search_time = end_time - start_time
        
        if total == 0 or not records:
            logger.warning("No results found")
            return False
        
        logger.info(f"Found {total} results, showing {len(records)} ({search_time:.2f} seconds)")
        
        # Handle output to file if specified
        if args.output:
            return save_results_to_file(records, args.output, args.format, args.raw, args.verbose)
        
        # Display results
        for i, record in enumerate(records, 1):
            print(f"\n--- Result {i} of {len(records)} ---")
            print(format_record(record, args.format, args.raw, args.verbose))
        
        # Show pagination info if applicable
        if total > len(records):
            remaining = total - len(records)
            if remaining > 0:
                print(f"\nThere are approximately {remaining} more results available.")
                print("Use --max-records to adjust the number of results returned.")
        
        return True
    
    except Exception as e:
        logger.error(f"Error performing OAI-PMH search: {e}")
        return False


def search_zotero(args):
    """
    Search a Zotero library (local database or API).
    
    Args:
        args: Command line arguments
        
    Returns:
        True if search was successful, False otherwise
    """
    # Determine if we're using local database or API
    use_api = args.zotero_api_key and args.zotero_library_id and args.zotero_library_type
    use_local = args.zotero_path
    
    if not (use_api or use_local):
        logger.error("For Zotero searches, specify either:")
        logger.error("  1. Local database: --zotero-path /path/to/zotero.sqlite")
        logger.error("  2. API: --zotero-api-key KEY --zotero-library-id ID --zotero-library-type [user|group]")
        return False
    
    if use_api and not ZOTERO_API_AVAILABLE:
        logger.error("Zotero API search requires the pyzotero library.")
        logger.error("Install it with: pip install pyzotero")
        return False
    
    logger.info(f"Searching Zotero via {'API' if use_api else 'local database'}")
    
    if use_api:
        return search_zotero_api(args)
    else:
        return search_zotero_local(args)


def search_zotero_api(args):
    """
    Search a Zotero library using the API.
    
    Args:
        args: Command line arguments
        
    Returns:
        True if search was successful, False otherwise
    """
    try:
        # Create Zotero API client
        zot = Zotero(
            library_id=args.zotero_library_id,
            library_type=args.zotero_library_type,
            api_key=args.zotero_api_key
        )
        
        # Prepare search parameters
        search_params = {}
        
        # Add any query parameters
        query_terms = []
        if args.title:
            query_terms.append(args.title)
        if args.author:
            query_terms.append(args.author)
        
        if query_terms:
            search_params['q'] = ' '.join(query_terms)
        
        # Add specific search fields
        if args.isbn:
            search_params['isbn'] = args.isbn
        
        # Execute search
        logger.info(f"Searching Zotero with parameters: {search_params}")
        start_time = time.time()
        
        # Get items with the specified parameters
        if search_params:
            items = zot.items(q=search_params.get('q'), **{k: v for k, v in search_params.items() if k != 'q'})
        else:
            # If no specific search, get recent items
            items = zot.top(limit=args.max_records)
        
        end_time = time.time()
        search_time = end_time - start_time
        
        if not items:
            logger.warning("No results found")
            return False
        
        # Convert Zotero items to BiblioRecords
        records = []
        for item in items:
            if item.get('data', {}).get('itemType') in ['book', 'journalArticle', 'bookSection', 'conferencePaper']:
                data = item.get('data', {})
                
                # Extract authors
                authors = []
                for creator in data.get('creators', []):
                    if 'name' in creator:
                        authors.append(creator['name'])
                    elif 'lastName' in creator and 'firstName' in creator:
                        authors.append(f"{creator['lastName']}, {creator['firstName']}")
                
                # Create BiblioRecord
                record = BiblioRecord(
                    id=item.get('key', ''),
                    title=data.get('title', 'Untitled'),
                    authors=authors,
                    year=data.get('date', '').split('-')[0] if data.get('date') else None,
                    publisher_name=data.get('publisher', None),
                    place_of_publication=data.get('place', None),
                    isbn=data.get('ISBN', None),
                    issn=data.get('ISSN', None),
                    urls=[data.get('url', '')] if data.get('url') else [],
                    abstract=data.get('abstractNote', None),
                    language=data.get('language', None),
                    series=data.get('series', None),
                    edition=data.get('edition', None),
                    subjects=[tag.get('tag', '') for tag in data.get('tags', [])],
                    raw_data=json.dumps(item, indent=2)
                )
                records.append(record)
        
        logger.info(f"Found {len(items)} items, showing {len(records)} compatible records ({search_time:.2f} seconds)")
        
        # Handle output to file if specified
        if args.output:
            return save_results_to_file(records, args.output, args.format, args.raw, args.verbose)
        
        # Display results
        for i, record in enumerate(records[:args.max_records], 1):
            print(f"\n--- Result {i} of {len(records)} ---")
            print(format_record(record, args.format, args.raw, args.verbose))
        
        return True
    
    except Exception as e:
        logger.error(f"Error searching Zotero API: {e}")
        return False


def search_zotero_local(args):
    """
    Search a local Zotero database.
    
    Args:
        args: Command line arguments
        
    Returns:
        True if search was successful, False otherwise
    """
    try:
        # Check if the database file exists
        if not os.path.isfile(args.zotero_path):
            logger.error(f"Zotero database not found at: {args.zotero_path}")
            return False
        
        # Connect to the SQLite database
        conn = sqlite3.connect(args.zotero_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Build the search query
        query = """
        SELECT i.itemID, i.key, idata.fieldName, idata.value
        FROM items i
        JOIN itemData idata ON i.itemID = idata.itemID
        JOIN itemDataValues idv ON idata.valueID = idv.valueID
        JOIN fields f ON idata.fieldID = f.fieldID
        JOIN itemTypes it ON i.itemTypeID = it.itemTypeID
        WHERE i.itemTypeID IN (1, 2, 3, 7)  -- book, article, bookSection, conferencePaper
          AND i.itemID NOT IN (SELECT itemID FROM deletedItems)
        """
        
        # Add search conditions
        conditions = []
        params = []
        
        if args.title:
            conditions.append("(f.fieldName = 'title' AND idv.value LIKE ?)")
            params.append(f"%{args.title}%")
        
        if args.author:
            # Creator search is more complex as it's in a different table
            creator_query = """
            i.itemID IN (
                SELECT itemID FROM creators c
                JOIN creatorData cd ON c.creatorDataID = cd.creatorDataID
                WHERE (cd.lastName LIKE ? OR cd.firstName LIKE ?)
            )
            """
            conditions.append(creator_query)
            params.append(f"%{args.author}%")
            params.append(f"%{args.author}%")
        
        if args.isbn:
            conditions.append("(f.fieldName = 'ISBN' AND idv.value LIKE ?)")
            params.append(f"%{args.isbn}%")
        
        if args.issn:
            conditions.append("(f.fieldName = 'ISSN' AND idv.value LIKE ?)")
            params.append(f"%{args.issn}%")
        
        if args.year:
            conditions.append("(f.fieldName = 'date' AND idv.value LIKE ?)")
            params.append(f"%{args.year}%")
        
        # Add conditions to the query
        if conditions:
            query += " AND (" + " OR ".join(conditions) + ")"
        
        # Add limit
        query += " LIMIT ?"
        params.append(args.max_records * 20)  # Higher limit for raw fields
        
        # Execute the query
        logger.info(f"Executing SQL query against local Zotero database")
        start_time = time.time()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Group by itemID to build complete records
        items_data = {}
        for row in rows:
            item_id = row['itemID']
            if item_id not in items_data:
                items_data[item_id] = {
                    'key': row['key'],
                    'fields': {}
                }
            items_data[item_id]['fields'][row['fieldName']] = row['value']
        
        # Get creator data for each item
        for item_id in items_data:
            cursor.execute("""
            SELECT c.orderIndex, cd.lastName, cd.firstName
            FROM creators c
            JOIN creatorData cd ON c.creatorDataID = cd.creatorDataID
            WHERE c.itemID = ?
            ORDER BY c.orderIndex
            """, (item_id,))
            
            creators = cursor.fetchall()
            items_data[item_id]['creators'] = creators
        
        # Get tags for each item
        for item_id in items_data:
            cursor.execute("""
            SELECT t.name
            FROM tags t
            JOIN itemTags it ON t.tagID = it.tagID
            WHERE it.itemID = ?
            """, (item_id,))
            
            tags = cursor.fetchall()
            items_data[item_id]['tags'] = [tag['name'] for tag in tags]
        
        # Convert to BiblioRecords
        records = []
        for item_id, data in items_data.items():
            fields = data['fields']
            
            # Create author list
            authors = []
            for creator in data.get('creators', []):
                last_name = creator['lastName'] or ''
                first_name = creator['firstName'] or ''
                if last_name or first_name:
                    authors.append(f"{last_name}, {first_name}".strip())
            
            # Extract year from date
            year = None
            if 'date' in fields:
                year_match = re.search(r'\b(1\d{3}|20\d{2})\b', fields['date'])
                if year_match:
                    year = year_match.group(1)
            
            # Create BiblioRecord
            record = BiblioRecord(
                id=data['key'],
                title=fields.get('title', 'Untitled'),
                authors=authors,
                year=year,
                publisher_name=fields.get('publisher'),
                place_of_publication=fields.get('place'),
                isbn=fields.get('ISBN'),
                issn=fields.get('ISSN'),
                urls=[fields.get('url')] if fields.get('url') else [],
                abstract=fields.get('abstractNote'),
                language=fields.get('language'),
                series=fields.get('series'),
                edition=fields.get('edition'),
                subjects=data.get('tags', []),
                raw_data=json.dumps(fields, indent=2)
            )
            records.append(record)
        
        end_time = time.time()
        search_time = end_time - start_time
        
        conn.close()
        
        if not records:
            logger.warning("No results found")
            return False
        
        logger.info(f"Found {len(records)} records ({search_time:.2f} seconds)")
        
        # Handle output to file if specified
        if args.output:
            return save_results_to_file(records, args.output, args.format, args.raw, args.verbose)
        
        # Display results
        for i, record in enumerate(records[:args.max_records], 1):
            print(f"\n--- Result {i} of {min(len(records), args.max_records)} ---")
            print(format_record(record, args.format, args.raw, args.verbose))
        
        return True
    
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error searching local Zotero database: {e}")
        return False


def save_results_to_file(records, filename, format_type='text', include_raw=False, verbose=False):
    """
    Save search results to a file.
    
    Args:
        records: List of BiblioRecord objects
        filename: Output filename
        format_type: 'text', 'json', 'bibtex', 'ris', or 'zotero'
        include_raw: Whether to include raw data
        verbose: Whether to include verbose output
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Determine file extension based on format if not provided
        base, ext = os.path.splitext(filename)
        if not ext:
            if format_type == 'json' or format_type == 'zotero':
                filename = f"{filename}.json"
            elif format_type == 'bibtex':
                filename = f"{filename}.bib"
            elif format_type == 'ris':
                filename = f"{filename}.ris"
            else:
                filename = f"{filename}.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            # Special handling for certain formats
            if format_type == 'json':
                # For JSON, create a list of record dictionaries
                data = [record.to_dict() for record in records]
                if include_raw:
                    for i, record in enumerate(records):
                        data[i]['raw_data'] = record.raw_data
                json.dump(data, f, indent=2)
            
            elif format_type == 'zotero':
                # For Zotero format, create a list of Zotero-compatible items
                zotero_items = []
                for record in records:
                    item_type = "book" if not record.issn else "journalArticle"
                    
                    # Format creators
                    creators = []
                    for author in record.authors:
                        creator = {}
                        if ',' in author:
                            parts = author.split(',', 1)
                            creator = {
                                "creatorType": "author",
                                "lastName": parts[0].strip(),
                                "firstName": parts[1].strip() if len(parts) > 1 else ""
                            }
                        else:
                            parts = author.split()
                            if len(parts) > 1:
                                creator = {
                                    "creatorType": "author",
                                    "lastName": parts[-1],
                                    "firstName": ' '.join(parts[:-1])
                                }
                            else:
                                creator = {
                                    "creatorType": "author",
                                    "lastName": author,
                                    "firstName": ""
                                }
                        creators.append(creator)

                    # Add editors
                    for editor in record.editors:
                        creator = {}
                        if ',' in editor:
                            parts = editor.split(',', 1)
                            creator = {
                                "creatorType": "editor",
                                "lastName": parts[0].strip(),
                                "firstName": parts[1].strip() if len(parts) > 1 else ""
                            }
                        else:
                            parts = editor.split()
                            if len(parts) > 1:
                                creator = {
                                    "creatorType": "editor",
                                    "lastName": parts[-1],
                                    "firstName": ' '.join(parts[:-1])
                                }
                            else:
                                creator = {
                                    "creatorType": "editor",
                                    "lastName": editor,
                                    "firstName": ""
                                }
                        creators.append(creator)
                    
                    # Create Zotero item
                    zotero_item = {
                        "itemType": item_type,
                        "title": record.title,
                        "creators": creators,
                        "date": record.year,
                        "publisher": record.publisher_name,
                        "place": record.place_of_publication,
                        "ISBN": record.isbn,
                        "ISSN": record.issn,
                        "series": record.series,
                        "edition": record.edition,
                        "language": record.language,
                        "url": record.urls[0] if record.urls else "",
                        "abstractNote": record.abstract,
                        "tags": [{"tag": subject} for subject in record.subjects],
                        "notes": []
                    }
                    zotero_items.append(zotero_item)
                
                json.dump(zotero_items, f, indent=2)
            
            else:
                # For other formats, write each record one by one
                for record in records:
                    f.write(format_record(record, format_type, include_raw, verbose))
                    f.write("\n\n")
        
        logger.info(f"Saved {len(records)} records to {filename}")
        return True
    
    except Exception as e:
        logger.error(f"Error saving to file: {e}")
        return False


def create_custom_endpoint(args):
    """Create a custom SRU or OAI-PMH endpoint configuration."""
    name = args.name
    url = args.url
    protocol = args.protocol or 'sru'  # Default to SRU if not specified
    
    # Validate URL
    if not url.startswith('http'):
        logger.error("URL must start with http:// or https://")
        return False
    
    # Create endpoint ID from name
    endpoint_id = name.lower().replace(' ', '_')
    
    if protocol == 'sru':
        # SRU-specific parameters
        version = args.version or '1.1'
        schema = args.schema
        
        # Create SRU endpoint info
        SRU_ENDPOINTS[endpoint_id] = {
            'name': name,
            'url': url,
            'default_schema': schema,
            'version': version,
            'description': 'Custom SRU endpoint'
        }
        
        print(f"Created custom SRU endpoint '{endpoint_id}'")
        print(f"Use it with: --endpoint {endpoint_id} --protocol sru")
    
    elif protocol == 'oai':
        # OAI-PMH-specific parameters
        metadata_prefix = args.metadata_prefix or 'oai_dc'
        
        # Create OAI endpoint info
        OAI_ENDPOINTS[endpoint_id] = {
            'name': name,
            'url': url,
            'default_metadata_prefix': metadata_prefix,
            'description': 'Custom OAI-PMH endpoint',
            'sets': {}
        }
        
        print(f"Created custom OAI-PMH endpoint '{endpoint_id}'")
        print(f"Use it with: --endpoint {endpoint_id} --protocol oai")
    
    else:
        logger.error(f"Unknown protocol: {protocol}")
        logger.info("Valid protocols are: sru, oai")
        return False
    
    return True


def explore_endpoint(args):
    """Explore available sets and metadata formats for an OAI-PMH endpoint."""
    endpoint_id = args.endpoint
    if endpoint_id not in OAI_ENDPOINTS:
        logger.error(f"Unknown OAI-PMH endpoint: {endpoint_id}")
        logger.info("Use --list --protocol oai to see available OAI-PMH endpoints")
        return False
    
    # Get endpoint info
    endpoint_info = OAI_ENDPOINTS[endpoint_id]
    print(f"\nExploring {endpoint_info['name']} ({endpoint_id})")
    print("=" * 50)
    
    # Create OAI client
    oai_client = OAIClient(
        base_url=endpoint_info['url'],
        default_metadata_prefix=endpoint_info.get('default_metadata_prefix', 'oai_dc'),
        timeout=args.timeout
    )
    
    # Get repository information
    print("\nRepository Information:")
    try:
        repo_info = oai_client.identify()
        if repo_info and not repo_info.get('error'):
            for key, value in repo_info.items():
                print(f"  {key}: {value}")
        else:
            print("  Could not retrieve repository information")
    except Exception as e:
        print(f"  Error: {e}")
    
    # List metadata formats
    print("\nAvailable Metadata Formats:")
    try:
        formats = oai_client.list_metadata_formats()
        if formats:
            if isinstance(formats, list) and formats[0].get('error'):
                print(f"  Error: {formats[0]['error']}")
            else:
                for fmt in formats:
                    print(f"  {fmt.get('metadataPrefix', 'Unknown')}:")
                    print(f"    Schema: {fmt.get('schema', 'Not specified')}")
        else:
            print("  No metadata formats available or an error occurred")
    except Exception as e:
        print(f"  Error: {e}")
    
    # List sets
    print("\nAvailable Sets:")
    try:
        sets = oai_client.list_sets()
        if sets:
            if isinstance(sets, list) and sets[0].get('error'):
                if sets[0]['error'].get('code') == 'noSetHierarchy':
                    print("  This repository does not support sets")
                else:
                    print(f"  Error: {sets[0]['error']}")
            else:
                for s in sets[:20]:  # Show up to 20 sets
                    print(f"  {s.get('setSpec', 'Unknown')}: {s.get('setName', 'No name')}")
                if len(sets) > 20:
                    print(f"  ... and {len(sets) - 20} more sets")
        else:
            print("  No sets available or an error occurred")
    except Exception as e:
        print(f"  Error: {e}")
    
    return True


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Search library SRU, OAI-PMH and specialized endpoints for books, journals, and other materials.',
        epilog='Example: library_search.py --endpoint dnb --title "Python Programming" --protocol sru'
    )
    
    # Endpoint selection
    parser.add_argument('--endpoint', default='dnb',
                        help='Endpoint to search (use --list to see available endpoints)')
    parser.add_argument('--list', action='store_true',
                        help='List available endpoints and exit')
    parser.add_argument('--info', metavar='ENDPOINT_ID',
                        help='Show detailed information about a specific endpoint and exit')
    parser.add_argument('--explore', action='store_true',
                        help='Explore available sets and metadata formats for an OAI-PMH endpoint')
    
    # Protocol selection
    parser.add_argument('--protocol', choices=['sru', 'oai', 'zotero', 'ixtheo'], default='sru',
                        help='Protocol to use (default: sru)')
    
    # Custom endpoint creation
    parser.add_argument('--create-endpoint', action='store_true',
                        help='Create a custom endpoint')
    parser.add_argument('--name', help='Name for the custom endpoint')
    parser.add_argument('--url', help='URL for the custom endpoint')
    parser.add_argument('--version', default='1.1',
                        help='SRU version for the custom endpoint')
    
    # Search parameters
    search_group = parser.add_argument_group('Search Parameters')
    search_group.add_argument('--title', help='Search by title')
    search_group.add_argument('--author', help='Search by author')
    search_group.add_argument('--isbn', help='Search by ISBN')
    search_group.add_argument('--issn', help='Search by ISSN')
    search_group.add_argument('--year', help='Search by publication year')
    search_group.add_argument('--subject', help='Search by subject/topic')
    search_group.add_argument('--advanced', 
                              help='Advanced search with custom query string or JSON parameters')
    search_group.add_argument('--max-records', type=int, default=10,
                              help='Maximum number of records to return')
    search_group.add_argument('--start-record', type=int, default=1,
                              help='Start record position (for pagination)')
    
    # SRU specific parameters
    sru_group = parser.add_argument_group('SRU Protocol Parameters')
    sru_group.add_argument('--schema',
                        help='Record schema (overrides endpoint default)')
    
    # OAI-PMH specific parameters
    oai_group = parser.add_argument_group('OAI-PMH Protocol Parameters')
    oai_group.add_argument('--metadata-prefix',
                        help='Metadata prefix (overrides endpoint default)')
    oai_group.add_argument('--set',
                        help='Set to search within')
    oai_group.add_argument('--from-date',
                        help='From date (YYYY-MM-DD)')
    oai_group.add_argument('--until-date',
                        help='Until date (YYYY-MM-DD)')
    
    # Zotero specific parameters
    zotero_group = parser.add_argument_group('Zotero Parameters')
    zotero_group.add_argument('--zotero-path',
                          help='Path to local Zotero database (zotero.sqlite)')
    zotero_group.add_argument('--zotero-api-key',
                          help='Zotero API key for accessing online library')
    zotero_group.add_argument('--zotero-library-id',
                          help='Zotero library ID')
    zotero_group.add_argument('--zotero-library-type', choices=['user', 'group'], default='user',
                          help='Zotero library type (user or group)')
    
    # IxTheo specific parameters
    ixtheo_group = parser.add_argument_group('IxTheo Parameters')
    ixtheo_group.add_argument('--format-filter',
                           help='Filter by format (e.g., "Article", "Book")')
    ixtheo_group.add_argument('--language-filter',
                           help='Filter by language (e.g., "German", "English")')
    ixtheo_group.add_argument('--get-export',
                           action='store_true',
                           help='Retrieve export data for each record')
    
    # Output format
    output_group = parser.add_argument_group('Output Parameters')
    output_group.add_argument('--format', choices=['text', 'json', 'bibtex', 'ris', 'zotero'], default='text',
                        help='Output format')
    output_group.add_argument('--output',
                        help='Output file for results')
    output_group.add_argument('--raw', action='store_true',
                        help='Include raw record data in output')
    
    # Other options
    parser.add_argument('--timeout', type=int, default=30,
                        help='Request timeout in seconds')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    parser.add_argument('--no-verify-ssl', action='store_true',
                        help='Disable SSL certificate verification')
    
    args = parser.parse_args()
    
    # Parse advanced search parameter if it's JSON
    if args.advanced and args.advanced.startswith('{'):
        try:
            args.advanced = json.loads(args.advanced)
        except json.JSONDecodeError:
            # Keep as string if not valid JSON
            pass
    
    return args


def main():
    """Main function."""
    args = parse_args()
    
    # Set log level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # List endpoints if requested
    if args.list:
        list_endpoints(args.protocol)
        sys.exit(0)
    
    # Show endpoint info if requested
    if args.info:
        show_endpoint_info(args.info)
        sys.exit(0)
    
    # Explore OAI-PMH endpoint if requested
    if args.explore and args.protocol == 'oai':
        success = explore_endpoint(args)
        sys.exit(0 if success else 1)
    
    # Create custom endpoint if requested
    if args.create_endpoint:
        if not (args.name and args.url):
            logger.error("--name and --url are required to create a custom endpoint")
            sys.exit(1)
        
        if create_custom_endpoint(args):
            sys.exit(0)
        else:
            sys.exit(1)
    
    # Check if any search criteria were specified
    if not any([
        args.title, args.author, args.isbn, args.issn, args.year, args.subject, args.advanced,
        # OAI-PMH specific criteria can also be valid search parameters
        (args.protocol == 'oai' and (args.set or args.from_date or args.until_date))
    ]):
        logger.error("No search criteria specified. Use --help to see available options.")
        sys.exit(1)
    
    # Perform search based on protocol
    if args.protocol == 'sru':
        success = search_sru_endpoint(args)
    elif args.protocol == 'oai':
        success = search_oai_endpoint(args)
    elif args.protocol == 'zotero':
        success = search_zotero(args)
    elif args.protocol == 'ixtheo':
        success = search_ixtheo_endpoint(args)
    else:
        logger.error(f"Unknown protocol: {args.protocol}")
        logger.info("Valid protocols are: sru, oai, zotero, ixtheo")
        success = False
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()