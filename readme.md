# Enhanced Library Search Framework

A comprehensive command-line tool for searching bibliographic data from various library sources, including:

- SRU (Search/Retrieve via URL) endpoints
- OAI-PMH (Open Archives Initiative Protocol for Metadata Harvesting) repositories
- Local Zotero databases
- Zotero Web API
- Index Theologicus (IxTheo) specialized database

## Features

- Search for books, journals, and other materials across multiple library endpoints
- Support for common search fields: title, author, ISBN, ISSN, year, subject
- Multiple output formats: plain text, JSON, BibTeX, RIS, MARC, and Zotero-compatible JSON
- Save search results to local files
- Explore OAI-PMH endpoints (sets, metadata formats)
- Create custom endpoints
- Search local Zotero databases
- Access remote Zotero libraries via API
- Search specialized theological literature via IxTheo
- Advanced filtering by format, language, and topic

## Requirements

- Python 3.6+
- Required dependencies:
  - `requests` (for HTTP requests)
  - `beautifulsoup4` (for HTML parsing)

- Optional dependencies:
  - `pyzotero` (for accessing Zotero Web API)
  - `lxml` (for better XML parsing)

## Installation

```bash
# Install required dependencies
pip install requests beautifulsoup4

# Install optional dependencies for Zotero API support
pip install pyzotero lxml
```

## Modules

The framework consists of four main Python modules:

1. `sru_library.py` - Handles SRU protocol communication
2. `oai_pmh_library.py` - Handles OAI-PMH protocol communication
3. `ixtheo_library.py` - Handles IxTheo specialized database searches
4. `library_search.py` - Command-line interface integrating all backends

## Usage Examples

### Basic Searches

Search for books with "Python" in the title in the German National Library:

```bash
python library_search.py --endpoint dnb --title "Python" --protocol sru
```

Search for books by "Einstein" in the Library of Congress:

```bash
python library_search.py --endpoint loc --author "Einstein" --protocol sru
```

Search for a specific ISBN:

```bash
python library_search.py --endpoint dnb --isbn "9783658310844" --protocol sru
```

### Output Formats

Get results in BibTeX format:

```bash
python library_search.py --endpoint dnb --title "Python" --format bibtex
```

Save results to a RIS file:

```bash
python library_search.py --endpoint dnb --title "Python" --format ris --output python_books.ris
```

Export results in Zotero-compatible format:

```bash
python library_search.py --endpoint dnb --title "Python" --format zotero --output python_books.json
```

### OAI-PMH Specific Searches

Search for items in a specific OAI set:

```bash
python library_search.py --endpoint dnb --protocol oai --set dnb:reiheA --from-date 2023-01-01
```

Explore available sets and formats in an OAI-PMH repository:

```bash
python library_search.py --endpoint europeana --protocol oai --explore
```

### IxTheo Searches

Search for theological literature on a specific topic:

```bash
python library_search.py --endpoint ixtheo --protocol ixtheo --title "Jesus" --format-filter "Article"
```

Search for works by a specific author and get BibTeX export data:

```bash
python library_search.py --endpoint ixtheo --protocol ixtheo --author "Barth" --get-export --format bibtex
```

Filter by language:

```bash
python library_search.py --endpoint ixtheo --protocol ixtheo --subject "Bible" --language-filter "English"
```

### Zotero Searches

Search a local Zotero database:

```bash
python library_search.py --protocol zotero --zotero-path /path/to/zotero/zotero.sqlite --title "Python"
```

Search a Zotero library via the API:

```bash
python library_search.py --protocol zotero --zotero-api-key YOUR_API_KEY --zotero-library-id YOUR_LIBRARY_ID --zotero-library-type user --title "Python"
```

### Other Features

List available endpoints:

```bash
python library_search.py --list
```

Get detailed information about an endpoint:

```bash
python library_search.py --info ixtheo
```

Create a custom endpoint:

```bash
python library_search.py --create-endpoint --name "My Library" --url "https://mylibrary.org/sru" --protocol sru
```

## Supported Endpoints

### SRU Endpoints

- Deutsche Nationalbibliothek (DNB)
- Bibliothèque nationale de France (BNF)
- Library of Congress (LOC)
- ZDB - German Union Catalogue of Serials
- More can be added as custom endpoints

### OAI-PMH Endpoints

- Deutsche Nationalbibliothek (DNB)
- Deutsche Nationalbibliothek Digital Objects
- Library of Congress
- Europeana
- Deutsche Digitale Bibliothek (DDB)
- Harvard University Library
- MIT DSpace
- KITopen (Karlsruher Institut für Technologie)
- arXiv
- Directory of Open Access Journals (DOAJ)

### Specialized Endpoints

- IxTheo (Index Theologicus) - Specialized theological database
- Zotero - Reference management software

## Advanced Usage

### Search Parameters

- `--max-records` - Maximum number of records to return (default: 10)
- `--start-record` - Start record position for pagination (default: 1)
- `--timeout` - Request timeout in seconds (default: 30)
- `--verbose` - Enable verbose output
- `--no-verify-ssl` - Disable SSL certificate verification

### SRU-specific Parameters

- `--schema` - Record schema (overrides endpoint default)

### OAI-PMH-specific Parameters

- `--metadata-prefix` - Metadata prefix (overrides endpoint default)
- `--set` - Set to search within
- `--from-date` - Start date (YYYY-MM-DD)
- `--until-date` - End date (YYYY-MM-DD)

### IxTheo-specific Parameters

- `--format-filter` - Filter by format (e.g., "Article", "Book")
- `--language-filter` - Filter by language (e.g., "German", "English")
- `--get-export` - Retrieve export data for each record

### Zotero-specific Parameters

- `--zotero-path` - Path to local Zotero database (zotero.sqlite)
- `--zotero-api-key` - Zotero API key for accessing online library
- `--zotero-library-id` - Zotero library ID
- `--zotero-library-type` - Zotero library type ('user' or 'group')

## Extending the Framework

### Adding a New SRU Endpoint

1. Add an entry to the `SRU_ENDPOINTS` dictionary in `sru_library.py`:

```python
'my_library': {
    'name': 'My Library',
    'url': 'https://mylibrary.org/sru',
    'default_schema': 'marcxml',
    'description': 'My custom library catalog',
    'version': '1.1',
    'examples': {
        'title': 'title="Python"',
        'author': 'author="Einstein"',
        'isbn': 'isbn=9781234567890',
        'advanced': 'title="Python" and author="Rossum"'
    }
}
```

### Adding a New OAI-PMH Endpoint

1. Add an entry to the `OAI_ENDPOINTS` dictionary in `oai_pmh_library.py`:

```python
'my_repository': {
    'name': 'My OAI Repository',
    'url': 'https://myrepository.org/oai',
    'default_metadata_prefix': 'oai_dc',
    'description': 'My custom OAI-PMH repository',
    'sets': {
        'my_set': 'My Collection Set'
    }
}
```

## License

This project is available under the Apache 2.0 License.

## Acknowledgments

- The SRU and OAI-PMH protocols for library interoperability
- The IxTheo project at University Library Tübingen for specialized theological bibliographic data
- Zotero for reference management