# Library Search Framework

A comprehensive command-line tool for searching bibliographic data from various library sources, including:

- SRU (Search/Retrieve via URL) endpoints
- OAI-PMH (Open Archives Initiative Protocol for Metadata Harvesting) repositories
- Local Zotero databases
- Zotero Web API

## Features

- Search for books, journals, and other materials across multiple library endpoints
- Support for common search fields: title, author, ISBN, ISSN, year
- Multiple output formats: plain text, JSON, BibTeX, RIS, and Zotero-compatible JSON
- Save search results to local files
- Explore OAI-PMH endpoints (sets, metadata formats)
- Create custom endpoints
- Search local Zotero databases
- Access remote Zotero libraries via API

## Requirements

- Python 3.6+
- Required dependencies:
  - `requests` (for HTTP requests)

- Optional dependencies:
  - `pyzotero` (for accessing Zotero Web API)

## Installation

```bash
# Install required dependencies
pip install requests

# Install optional dependencies for Zotero API support
pip install pyzotero
```

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
python library_search.py --info dnb
```

Create a custom endpoint:

```bash
python library_search.py --create-endpoint --name "My Library" --url "https://mylibrary.org/sru" --protocol sru
```

## Structure

The framework consists of three main Python modules:

1. `sru_library.py` - Handles SRU protocol communication
2. `oai_pmh_library.py` - Handles OAI-PMH protocol communication
3. `library_search.py` - Command-line interface integrating both backends plus Zotero support

## Advanced Usage

### Search Parameters

- `--max-records` - Maximum number of records to return (default: 10)
- `--start-record` - Start record position for pagination (default: 1)
- `--timeout` - Request timeout in seconds (default: 30)
- `--verbose` - Enable verbose output

### SRU-specific Parameters

- `--schema` - Record schema (overrides endpoint default)

### OAI-PMH-specific Parameters

- `--metadata-prefix` - Metadata prefix (overrides endpoint default)
- `--set` - Set to search within
- `--from-date` - Start date (YYYY-MM-DD)
- `--until-date` - End date (YYYY-MM-DD)

### Zotero-specific Parameters

- `--zotero-path` - Path to local Zotero database (zotero.sqlite)
- `--zotero-api-key` - Zotero API key for accessing online library
- `--zotero-library-id` - Zotero library ID
- `--zotero-library-type` - Zotero library type ('user' or 'group')

## License

This project is available under the MIT License.
