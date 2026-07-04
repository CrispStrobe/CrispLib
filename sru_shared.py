"""Shared SRU parsers for CrispLib + citer (PLAN 8.5).

Canonical copy lives in CrispLib/sru_shared.py; scripts/sync-endpoints.sh copies
it byte-for-byte to citer/lib/sru_shared.py. Both repos import BiblioRecord + the
parse functions from here and register them on their own SRUClient, so the SRU
parsing logic lives in ONE place and can no longer drift (the parser-parity
golden guards the output). Edit here, then run scripts/sync-endpoints.sh.
"""
import re
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)


@dataclass
class BiblioRecord:
    """Data class for bibliographic records."""
    id: str
    title: str
    # Separate authors and editors
    authors: List[str] = field(default_factory=list)
    editors: List[str] = field(default_factory=list)  # New field for editors
    translators: List[str] = field(default_factory=list)  # New field for translators
    contributors: List[Dict[str, str]] = field(default_factory=list)  # People with specific roles
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
    schema: Optional[str] = None  # Store the schema used for parsing
    
    # Journal related fields
    journal_title: Optional[str] = None  # For journal articles 
    volume: Optional[str] = None  # Volume number
    issue: Optional[str] = None   # Issue number
    pages: Optional[str] = None   # Page range
    doi: Optional[str] = None     # Digital Object Identifier
    document_type: Optional[str] = None  # Article, Book, etc.
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "authors": self.authors,
            "editors": self.editors,
            "translators": self.translators,
            "contributors": self.contributors,
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
            "edition": self.edition,
            "journal_title": self.journal_title,
            "volume": self.volume,
            "issue": self.issue,
            "pages": self.pages,
            "doi": self.doi,
            "document_type": self.document_type,
            "schema": self.schema
        }
    
    def __str__(self) -> str:
        """String representation of the record."""
        authors_str = ", ".join(self.authors) if self.authors else ""
        editors_str = ", ".join(self.editors) if self.editors else ""
        credit_parts = []
        if authors_str:
            credit_parts.append(f"by {authors_str}")
        if editors_str:
            credit_parts.append(f"edited by {editors_str}")
        
        credit = " ".join(credit_parts) if credit_parts else "Unknown"
        year_str = self.year or "n.d."
        
        # For journal articles
        if self.journal_title:
            journal_info = self.journal_title
            if self.volume:
                journal_info += f", {self.volume}"
                if self.issue:
                    journal_info += f"({self.issue})"
            if self.pages:
                journal_info += f", pp. {self.pages}"
            return f"{self.title} {credit} ({year_str}, {journal_info})"
        
        # For books and other materials
        pub_info = []
        if self.place_of_publication:
            pub_info.append(self.place_of_publication)
        if self.publisher_name:
            pub_info.append(self.publisher_name)
        
        pub_str = ": ".join(pub_info) if pub_info else "Unknown"
        
        return f"{self.title} {credit} ({year_str}, {pub_str})"

    def get_citation_key(self) -> str:
        """Generate a sensible BibTeX citation key."""
        # Get first author's last name or 'unknown' if no authors
        if self.authors:
            # Extract last name (after the last comma or the whole name if no comma)
            first_author = self.authors[0]
            
            # Clean the name first - remove any role indicators
            first_author = re.sub(r'\s*\[[^\]]*\]', '', first_author)
            # Fix broken brackets
            first_author = re.sub(r'\]\s*$', '', first_author)
            first_author = re.sub(r'^\s*\[', '', first_author)
            
            if ',' in first_author:
                author_key = first_author.split(',')[0].strip().lower()
            else:
                # Take the last word as the last name
                parts = first_author.split()
                author_key = parts[-1].lower() if parts else 'unknown'
        elif self.editors:
            # Use first editor if no authors
            first_editor = self.editors[0]
            
            # Clean the name first
            first_editor = re.sub(r'\s*\[[^\]]*\]', '', first_editor)
            # Fix broken brackets
            first_editor = re.sub(r'\]\s*$', '', first_editor)
            first_editor = re.sub(r'^\s*\[', '', first_editor)
            
            if ',' in first_editor:
                author_key = first_editor.split(',')[0].strip().lower()
            else:
                parts = first_editor.split()
                author_key = parts[-1].lower() if parts else 'editor'
        else:
            author_key = 'unknown'
        
        # Remove any non-alphanumeric characters
        author_key = re.sub(r'[^a-z0-9]', '', author_key)
        
        # If author_key is empty after cleaning, use 'unknown'
        if not author_key:
            author_key = 'unknown'
        
        # Add year if available
        if self.year:
            return f"{author_key}{self.year}"
        return author_key
    
    def get_citation_key_old(self) -> str:
        """Generate a sensible BibTeX citation key."""
        # Get first author's last name or 'unknown' if no authors
        if self.authors:
            # Extract last name (after the last comma or the whole name if no comma)
            first_author = self.authors[0]
            if ',' in first_author:
                author_key = first_author.split(',')[0].strip().lower()
            else:
                # Take the last word as the last name
                parts = first_author.split()
                author_key = parts[-1].lower() if parts else 'unknown'
        elif self.editors:
            # Use first editor if no authors
            first_editor = self.editors[0]
            if ',' in first_editor:
                author_key = first_editor.split(',')[0].strip().lower()
            else:
                parts = first_editor.split()
                author_key = parts[-1].lower() if parts else 'editor'
        else:
            author_key = 'unknown'
        
        # Remove any non-alphanumeric characters
        author_key = re.sub(r'[^a-z0-9]', '', author_key)
        
        # Add year if available
        if self.year:
            return f"{author_key}{self.year}"
        return author_key


_RELATOR_WORDS = (
    r"Auteur|[ÉE]diteur|Traducteur|Pr[ée]facier|Postfacier|Collaborateur|"
    r"Illustrateur|Annotateur|Directeur|R[ée]alisateur|Sc[ée]nariste|Adaptateur|"
    r"Compositeur|Act(?:eur|rice)|Interpr[èe]te|Chanteu(?:r|se)|Photographe|"
    r"Dessinateur|Graveur|Metteur\s+en\s+sc[èe]ne|Producteur|Narrateur|"
    r"Chor[ée]graphe|Danseu(?:r|se)|Musicien|Arrangeur|Parolier|Distributeur|"
    r"Imprimeur|Cartographe|Lithographe|Peintre|Sculpteur|Architecte|R[ée]dacteur|"
    r"Monteur|Commissaire|Conseiller|"
    r"Herausgeber(?:in)?|[ÜU]bersetzer(?:in)?|Verfasser(?:in)?|Mitwirkende[rn]?|"
    r"Bearbeiter|Erz[äa]hler|Komponist|Regisseur|Schauspieler|"
    r"Author|Editor|Translator|Contributor|Illustrator|Narrator|Composer|"
    r"Director|Screenwriter|Producer|Performer|Actor|Photographer|Cartographer"
)
_ROLE_SUFFIX_RE = re.compile(r'\.\s*(?:' + _RELATOR_WORDS + r')\b[^.]*$', re.IGNORECASE)


def clean_person_name(name):
    """Strip life dates and role phrases that DC/RDF sources (esp. BnF) append to
    creator names, e.g. "Habermas, Jürgen (1929-2026). Auteur du texte"."""
    if not name:
        return name
    n = name.strip()
    # Life-date parenthetical anywhere: "(1929-2026)", "(1956-)", "(1956-....)",
    # "(1963-.... ; actrice)". Requires 3-4 leading digits + a dash.
    n = re.sub(r'\s*\(\s*\d{3,4}\s*[-–][^)]*\)', '', n)
    # Trailing controlled relator phrase after a period.
    n = _ROLE_SUFFIX_RE.sub('', n)
    # Bare trailing life-date range: "Einstein, Albert 1879-1955".
    n = re.sub(r',?\s*\d{3,4}\s*[-–]\s*\d{0,4}\s*$', '', n)
    return re.sub(r'[,;\s]+$', '', n.strip())


def map_dc_type(dc_type_text):
    """Map dc:type free text (dcmitype terms + BnF French labels, possibly several
    joined) to a document_type for non-text material, or '' for text/unknown.
    'video' is tested before 'image' so 'moving image' wins."""
    t = (dc_type_text or '').lower()
    if re.search(r'moving image|image anim|\bvideo\b|\bfilm\b', t):
        return "Video"
    if re.search(r'\bsound\b|\baudio\b|\bmusic\b|musique|enregistrement sonore', t):
        return "Audio"
    if re.search(r'still image|image fixe|photograph|\bartwork\b', t):
        return "Image"
    if re.search(r'cartograph|\bmap\b|\bcarte\b', t):
        return "Map"
    if re.search(r'software|logiciel', t):
        return "Software"
    if re.search(r'\bdataset\b|données de (?:la )?recherche', t):
        return "Dataset"
    return ""


def infer_document_type(document_type, isbn, issn, journal_title, format_str=None):
    """Fill in a document type from available clues when the source lacks one."""
    if document_type:
        return document_type
    fmt = (format_str or '').lower()
    if journal_title:
        return "Journal Article"
    if 'book' in fmt or 'buch' in fmt or 'monogr' in fmt:
        return "Book"
    if isbn:
        return "Book"
    if issn:
        return "Journal"
    return "Book"


def parse_dublin_core(raw_record, namespaces):
    """Parse Dublin Core format records."""
    data = raw_record['data']
    record_id = raw_record.get('id', 'unknown')
    
    # Add Dublin Core namespace if not present
    ns = namespaces.copy()
    if 'dc' not in ns:
        ns['dc'] = 'http://purl.org/dc/elements/1.1/'
    if 'dcterms' not in ns:
        ns['dcterms'] = 'http://purl.org/dc/terms/'
    
    # Find title
    title_elem = data.find('.//dc:title', ns)
    title = title_elem.text.strip() if title_elem is not None and title_elem.text else "Untitled"
    
    # Find authors and contributors with proper separation
    authors = []
    editors = []
    translators = []
    contributors = []
    
    # Keep track of seen names to avoid duplicates
    seen_names = set()
    
    # Process creators (authors)
    creator_elems = data.findall('.//dc:creator', ns)
    for elem in creator_elems:
        if elem.text and elem.text.strip():
            name = clean_person_name(elem.text.strip())

            # Check if it's an editor
            if re.search(r'\b(?:ed(?:itor)?|hrsg|hg|edit\.)\b', name.lower(), re.IGNORECASE) or "(ed" in name.lower():
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
                
            # Regular author (not an editor or translator)
            if name not in seen_names:
                authors.append(name)
                seen_names.add(name)
    
    # Process contributors (could be editors for book chapters)
    contributor_elems = data.findall('.//dc:contributor', ns)
    for elem in contributor_elems:
        if elem.text and elem.text.strip():
            contributor = clean_person_name(elem.text.strip())

            # Check if it's an editor
            if re.search(r'\b(?:ed(?:itor)?|hrsg|hg)\b', contributor.lower(), re.IGNORECASE) or "(ed" in contributor.lower():
                # Clean editor name
                clean_name = re.sub(r'\s*[\(\[][^)]*(?:ed|hrsg|edit|hg)[^)]*[\)\]]', '', contributor)
                clean_name = re.sub(r'\s*(?:ed|hrsg|edit|hg)\.?(?:\s+|$)', '', clean_name)
                clean_name = clean_name.strip()
                
                if clean_name and clean_name not in seen_names:
                    editors.append(clean_name)
                    seen_names.add(clean_name)
                continue
                
            # Check if it's a translator
            if re.search(r'\b(?:trans|transl|translator|übersetz|übers)\b', contributor.lower(), re.IGNORECASE):
                # Clean translator name
                clean_name = re.sub(r'\s*[\(\[][^)]*(?:trans|übersetz)[^)]*[\)\]]', '', contributor)
                clean_name = re.sub(r'\s*(?:trans|transl|translator|übersetz|übers)\.?(?:\s+|$)', '', clean_name)
                clean_name = clean_name.strip()
                
                if clean_name and clean_name not in seen_names:
                    translators.append(clean_name)
                    seen_names.add(clean_name)
                continue
                
            # Other contributors with no specific role identified
            if contributor not in seen_names:
                contributors.append({"name": contributor, "role": "contributor"})
                seen_names.add(contributor)
    
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
    
    # Find identifiers (ISBN, ISSN, DOI)
    isbn = None
    issn = None
    doi = None
    
    identifier_elems = data.findall('.//dc:identifier', ns)
    for elem in identifier_elems:
        if not elem.text:
            continue
            
        id_text = elem.text.strip().lower()
        
        # Extract ISBN
        if 'isbn' in id_text:
            match = re.search(r'(?:isbn[:\s]*)?(\d[\d\-X]+)', id_text)
            if match:
                isbn = match.group(1)
                
        # Extract ISSN
        elif 'issn' in id_text:
            match = re.search(r'(?:issn[:\s]*)?(\d{4}-\d{3}[\dX])', id_text)
            if match:
                issn = match.group(1)
                
        # Extract DOI
        elif 'doi' in id_text or 'doi.org' in id_text:
            # DOI format is flexible, but commonly 10.NNNN/rest
            match = re.search(r'(?:doi[:\s]*)?(?:https?://doi.org/)?(\d+\.\d+/[^\s]+)', id_text)
            if match:
                doi = match.group(1)
    
    # Find subjects
    subjects = []
    subject_elems = data.findall('.//dc:subject', ns)
    for elem in subject_elems:
        if elem.text and elem.text.strip():
            subjects.append(elem.text.strip())
    
    # Find description (abstract)
    abstract = None
    description_elem = data.find('.//dc:description', ns)
    if description_elem is not None and description_elem.text:
        abstract = description_elem.text.strip()
    
    # Find language
    language = None
    language_elem = data.find('.//dc:language', ns)
    if language_elem is not None and language_elem.text:
        language = language_elem.text.strip()
    
    # Find format
    format_str = None
    format_elem = data.find('.//dc:format', ns)
    if format_elem is not None and format_elem.text:
        format_str = format_elem.text.strip()

    # dc:type material typing (BnF etc. emit "moving image"/"image animée", …).
    dc_type_text = ' | '.join(
        (e.text or '').strip().lower()
        for e in data.findall('.//dc:type', ns) if e.text
    )
    av_type = map_dc_type(dc_type_text)

    # Find extent (pages, etc.)
    extent = None
    extent_elem = data.find('.//dcterms:extent', ns)
    if extent_elem is not None and extent_elem.text:
        extent = extent_elem.text.strip()
        
        # Try to extract page range
        pages = None
        page_match = re.search(r'(\d+)(?:\s*[-–]\s*(\d+))?\s*p', extent)
        if page_match:
            if page_match.group(2):  # Range of pages
                pages = f"{page_match.group(1)}-{page_match.group(2)}"
            else:  # Single page count
                pages = page_match.group(1)
    
    # Find source (could contain book or journal info)
    source_elem = data.find('.//dc:source', ns)
    source = source_elem.text.strip() if source_elem is not None and source_elem.text else None
    
    # Initialize journal/book related variables
    journal_title = None
    volume = None
    issue = None
    series = None
    pages = None
    
    # Process source to extract journal info if available
    if source:
        # Check for journal pattern like "Journal Name, Vol. X, No. Y, pp. Z-W"
        journal_match = re.search(r'([^,]+),\s*(?:Vol(?:ume)?\.?\s*(\d+))?,?\s*(?:No\.?\s*(\d+))?,?\s*(?:pp\.?\s*(\d+(?:-\d+)?))?', source)
        if journal_match:
            journal_title = journal_match.group(1).strip()
            volume = journal_match.group(2)
            issue = journal_match.group(3)
            if journal_match.group(4):
                pages = journal_match.group(4)
                
        # If not journal, might be a book chapter or series
        elif 'in:' in source.lower() or 'in ' in source.lower():
            book_match = re.search(r'(?:in:?|In:?)\s*([^,]+)', source)
            if book_match:
                series = book_match.group(1).strip()
    
    # Determine document type from available info
    document_type = None
    if journal_title and (volume or issue):
        document_type = "Journal Article"
    elif av_type:
        document_type = av_type
    elif series:
        document_type = "Book Chapter"
    elif format_str and 'book' in format_str.lower():
        document_type = "Book"
    # Fall back to ISBN/ISSN/format so DC records aren't left type-less.
    document_type = infer_document_type(document_type, isbn, issn, journal_title, format_str)

    # Create and return BiblioRecord with all extracted info
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        contributors=contributors,
        year=year,
        publisher_name=publisher,
        place_of_publication=None,  # Dublin Core doesn't typically have this
        isbn=isbn,
        issn=issn,
        urls=[],  # URLs should be extracted separately
        abstract=abstract,
        language=language,
        format=format_str,
        subjects=subjects,
        series=series,  # Use series field for book title in case of book chapter
        extent=extent,
        edition=None,  # Edition info not typically in basic Dublin Core
        
        # Journal article fields
        journal_title=journal_title,
        volume=volume,
        issue=issue,
        pages=pages,
        doi=doi,
        document_type=document_type,
        schema=raw_record.get('schema')
    )


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
    record = data
    
    # Check if data element is already a record element
    marc_record_tags = [
        '{http://www.loc.gov/MARC21/slim}record',
        '{info:lc/xmlns/marcxchange-v2}record'
    ]
    
    if not any(record.tag == tag for tag in marc_record_tags):
        # Try to find record element if data is not already a record
        for prefix in ['marc', 'mxc']:
            record_elem = data.find(f'.//{prefix}:record', ns)
            if record_elem is not None:
                record = record_elem
                break
                
        # If still not found, match by local tag name. ElementTree/defusedxml have
        # no local-name() XPath function (it raises "invalid predicate"), so iterate.
        if record == data:
            for elem in data.iter():
                if isinstance(elem.tag, str) and elem.tag.rsplit('}', 1)[-1] == 'record':
                    record = elem
                    break
    
    # Some SRU servers (e.g. hebis / PICA-CBS) return namespace-LESS MARCXML
    # (<record>/<datafield> with no MARC21 namespace). Bind the 'marc' prefix to
    # whatever namespace the datafields actually use so every lookup below matches.
    def _detect_marc_ns(rec):
        for el in rec.iter():
            if isinstance(el.tag, str) and el.tag.rsplit('}', 1)[-1] == 'datafield':
                return el.tag[1:el.tag.index('}')] if el.tag.startswith('{') else ''
        return None
    _detected = _detect_marc_ns(record)
    if _detected is not None and _detected not in (ns.get('marc'), ns.get('mxc')):
        ns['marc'] = _detected

    # Helper function to find datafields
    def find_datafields(tag, code):
        fields = []
        for prefix in ['marc', 'mxc']:
            elems = record.findall(f'.//{prefix}:datafield[@tag="{tag}"]/{prefix}:subfield[@code="{code}"]', ns)
            if elems:
                fields.extend([f.text.strip() for f in elems if f.text and f.text.strip()])
        return fields
    
    # Find title (MARC field 245 subfield a)
    title = "Untitled"
    title_fields = find_datafields("245", "a")
    if title_fields:
        title = title_fields[0]
        # Some titles end with / or : or other punctuation
        title = re.sub(r'[/:]$', '', title).strip()
    
    # Find subtitle if present (245 subfield b)
    subtitle_fields = find_datafields("245", "b")
    if subtitle_fields:
        title += ": " + subtitle_fields[0].strip()
    
    # Find authors, editors, translators, and other contributors with proper separation
    authors = []
    editors = []
    translators = []
    contributors = []
    
    # Keep track of seen names to avoid duplicates
    seen_names = set()
    
    # Creator (100)
    for prefix in ['marc', 'mxc']:
        creator_fields = record.findall(f'.//{prefix}:datafield[@tag="100"]', ns)
        for fld in creator_fields:
            name_subfield = fld.find(f'./{prefix}:subfield[@code="a"]', ns)
            if name_subfield is not None and name_subfield.text:
                name = name_subfield.text.strip()
                # Check for role in subfield e
                role_subfield = fld.find(f'./{prefix}:subfield[@code="e"]', ns)
                role = role_subfield.text.strip().lower() if role_subfield is not None and role_subfield.text else ''
                
                if role:
                    if any(r in role for r in ['edit', 'hrsg', 'hg', 'herausg']):
                        if name not in seen_names:
                            editors.append(name)
                            seen_names.add(name)
                    elif any(r in role for r in ['transl', 'übers']):
                        if name not in seen_names:
                            translators.append(name)
                            seen_names.add(name)
                    elif any(r in role for r in ['verf', 'author', 'autor', 'creator']):
                        # Author relator across languages (DNB/K10plus: "Verfasser")
                        if name not in seen_names:
                            authors.append(name)
                            seen_names.add(name)
                    else:
                        # Other contributor role
                        if name not in seen_names:
                            contributors.append({"name": name, "role": role})
                            seen_names.add(name)
                else:
                    # No specific role, assume author
                    if name not in seen_names:
                        authors.append(name)
                        seen_names.add(name)
    
    # Contributors (700)
    for prefix in ['marc', 'mxc']:
        contributor_fields = record.findall(f'.//{prefix}:datafield[@tag="700"]', ns)
        for fld in contributor_fields:
            name_subfield = fld.find(f'./{prefix}:subfield[@code="a"]', ns)
            if name_subfield is not None and name_subfield.text:
                name = name_subfield.text.strip()
                # Check for role in subfield e
                role_subfield = fld.find(f'./{prefix}:subfield[@code="e"]', ns)
                role = role_subfield.text.strip().lower() if role_subfield is not None and role_subfield.text else ''
                
                if role:
                    if any(r in role for r in ['edit', 'hrsg', 'hg', 'herausg']):
                        if name not in seen_names:
                            editors.append(name)
                            seen_names.add(name)
                    elif any(r in role for r in ['transl', 'übers']):
                        if name not in seen_names:
                            translators.append(name)
                            seen_names.add(name)
                    elif any(r in role for r in ['verf', 'author', 'autor', 'creator']):
                        # Author relator across languages (DNB/K10plus: "Verfasser")
                        if name not in seen_names:
                            authors.append(name)
                            seen_names.add(name)
                    else:
                        # Other contributor role
                        if name not in seen_names:
                            contributors.append({"name": name, "role": role})
                            seen_names.add(name)
                else:
                    # No specific role, assume author/contributor
                    if name not in seen_names:
                        authors.append(name)
                        seen_names.add(name)
    
    # If no authors but we have editors, use editors
    if not authors and editors:
        # We'll leave editors in editors field, they'll be displayed as "edited by"
        pass
    
    # Find year (MARC field 260/264 subfield c)
    year = None
    for tag in ['260', '264']:
        date_fields = find_datafields(tag, "c")
        if date_fields:
            date_text = date_fields[0]
            # Extract year
            match = re.search(r'\b(1\d{3}|20\d{2})\b', date_text)
            if match:
                year = match.group(1)
                break
    
    # Find publisher (MARC field 260/264 subfield b)
    publisher = None
    for tag in ['260', '264']:
        publisher_fields = find_datafields(tag, "b")
        if publisher_fields:
            publisher = publisher_fields[0]
            # Some publishers end with , or : or other punctuation
            publisher = re.sub(r'[,:]$', '', publisher).strip()
            break
    
    # Find place of publication (MARC field 260/264 subfield a)
    place = None
    for tag in ['260', '264']:
        place_fields = find_datafields(tag, "a")
        if place_fields:
            place = place_fields[0]
            # Some places end with : or other punctuation
            place = re.sub(r':$', '', place).strip()
            break
    
    # Find ISBN (MARC field 020 subfield a)
    isbn = None
    isbn_fields = find_datafields("020", "a")
    if isbn_fields:
        isbn_text = isbn_fields[0]
        # Extract just the ISBN part
        match = re.search(r'(\d[\d\-X]+)', isbn_text)
        if match:
            isbn = match.group(1)
    
    # Find ISSN (MARC field 022 subfield a)
    issn = None
    issn_fields = find_datafields("022", "a")
    if issn_fields:
        issn = issn_fields[0]
    
    # Find DOI (MARC field 024 subfield a, with indicator 7 and subfield 2 = doi)
    doi = None
    for prefix in ['marc', 'mxc']:
        # ElementTree supports chained predicates [@a][@b] but not [@a and @b].
        doi_fields = record.findall(f'.//{prefix}:datafield[@tag="024"][@ind1="7"]', ns)
        for fld in doi_fields:
            subfield_2 = fld.find(f'./{prefix}:subfield[@code="2"]', ns)
            subfield_a = fld.find(f'./{prefix}:subfield[@code="a"]', ns)
            
            if (subfield_2 is not None and subfield_2.text and 
                subfield_2.text.strip().lower() == "doi" and
                subfield_a is not None and subfield_a.text):
                doi = subfield_a.text.strip()
                break
    
    # Find subjects (MARC 650/651 topical/geographic, 653 uncontrolled index terms)
    subjects = []
    for tag in ['650', '651', '653']:
        for s in find_datafields(tag, "a"):
            if s not in subjects:
                subjects.append(s)
    # DDC / classification (082 Dewey, 084 other classification e.g. German "sdnb")
    for tag in ['082', '084']:
        for code in find_datafields(tag, "a"):
            tagged = f"DDC:{code}" if tag == '082' else code
            if tagged not in subjects:
                subjects.append(tagged)

    # Abstract / summary (MARC 520 subfield a)
    abstract = None
    abstract_fields = find_datafields("520", "a")
    if abstract_fields:
        abstract = " ".join(abstract_fields).strip()

    # Corporate authors (110 main / 710 added entry). Kept as full-name strings.
    for tag, bucket in [("110", authors), ("710", contributors)]:
        for prefix in ['marc', 'mxc']:
            for fld in record.findall(f'.//{prefix}:datafield[@tag="{tag}"]', ns):
                nm = fld.find(f'./{prefix}:subfield[@code="a"]', ns)
                if nm is not None and nm.text and nm.text.strip():
                    name = nm.text.strip().rstrip('.,;')
                    if name in seen_names:
                        continue
                    seen_names.add(name)
                    if bucket is authors:
                        authors.append(name)
                    else:
                        contributors.append({"name": name, "role": "corporate"})

    # Find language (MARC field 041 subfield a or 008 positions 35-37)
    language = None
    language_fields = find_datafields("041", "a")
    if language_fields:
        language = language_fields[0]
    
    # Find series (MARC field 490 or 830)
    series = None
    series_fields = find_datafields("490", "a") or find_datafields("830", "a")
    if series_fields:
        series = series_fields[0]
    
    # Find extent/pagination (MARC field 300 subfield a)
    extent = None
    extent_fields = find_datafields("300", "a")
    if extent_fields:
        extent = extent_fields[0]
    
    # Extract page information from extent
    pages = None
    if extent:
        page_match = re.search(r'(\d+)(?:\s*[-–]\s*(\d+))?\s*p', extent)
        if page_match:
            if page_match.group(2):  # Range of pages
                pages = f"{page_match.group(1)}-{page_match.group(2)}"
            else:  # Single page count
                pages = page_match.group(1)
    
    # Find edition (MARC field 250 subfield a)
    edition = None
    edition_fields = find_datafields("250", "a")
    if edition_fields:
        edition = edition_fields[0]
    
    # Find URLs (MARC field 856 subfield u)
    urls = find_datafields("856", "u")
    
    # Find if this is a journal article or book chapter
    # Host item entry is in MARC field 773
    journal_title = None
    volume = None
    issue = None
    
    for prefix in ['marc', 'mxc']:
        host_item_fields = record.findall(f'.//{prefix}:datafield[@tag="773"]', ns)
        for fld in host_item_fields:
            # Title of host item (journal or book title)
            title_subfield = fld.find(f'./{prefix}:subfield[@code="t"]', ns)
            if title_subfield is not None and title_subfield.text:
                host_title = title_subfield.text.strip()

                g_subfield = fld.find(f'./{prefix}:subfield[@code="g"]', ns)
                vol_text = g_subfield.text.strip() if (g_subfield is not None and g_subfield.text) else ''
                # $7 position 3 = host bibliographic level ('s' = serial -> journal,
                # 'm' = monograph -> chapter). Authoritative; fall back to $g sniffing
                # (English + German forms like "78(2024), 3, Seite 205-213" carry no keyword).
                link7_sub = fld.find(f'./{prefix}:subfield[@code="7"]', ns)
                link7 = (link7_sub.text or '').strip() if link7_sub is not None else ''
                host_bib_level = link7[3].lower() if len(link7) >= 4 else ''
                issn_sub = fld.find(f'./{prefix}:subfield[@code="x"]', ns)
                host_issn = (issn_sub.text or '').strip() if issn_sub is not None else None

                if host_bib_level == 's':
                    is_journal = True
                elif host_bib_level == 'm':
                    is_journal = False
                else:
                    is_journal = bool(re.search(r'vol|issue|no\.?|nr\.?|number|band|bd\.?|jg\.?|jahrg|heft|\(\d{4}\)', vol_text, re.IGNORECASE))

                if is_journal:
                    journal_title = host_title
                    if host_issn and not issn:
                        issn = host_issn
                    vol_match = (re.search(r'(?:vol(?:ume)?|bd\.?|band|jg\.?|jahrg(?:ang)?)\.?\s*(\d+)', vol_text, re.IGNORECASE)
                                 or re.search(r'(\d+)\s*\(\d{4}\)', vol_text)
                                 or re.search(r'^\s*(\d+)\b', vol_text))
                    if vol_match:
                        volume = vol_match.group(1)
                    issue_match = (re.search(r'(?:no|nr|issue|num|heft|h)\.?\s*(\d+)', vol_text, re.IGNORECASE)
                                   or re.search(r'\)\s*,\s*(\d+)', vol_text))
                    if issue_match:
                        issue = issue_match.group(1)
                    page_match = re.search(r'\b(?:seite|pages?|pp?|s)\.?\s*(\d+)(?:\s*[-–]\s*(\d+))?', vol_text, re.IGNORECASE)
                    if page_match:
                        pages = f"{page_match.group(1)}-{page_match.group(2)}" if page_match.group(2) else page_match.group(1)
                else:
                    # Monograph host -> book chapter
                    series = host_title
    
    # Determine document type. The leader is authoritative: position 6 = type of
    # record (material), position 7 = bibliographic level (monograph/serial/part).
    material_type = None
    biblio_level = None
    for prefix in ['marc', 'mxc']:
        leader_elem = record.find(f'.//{prefix}:leader', ns)
        if leader_elem is not None and leader_elem.text and len(leader_elem.text) >= 8:
            material_type = leader_elem.text[6]
            biblio_level = leader_elem.text[7]
            break

    # Non-textual material types take precedence over the bibliographic level.
    NONTEXT = {'c': 'Score', 'd': 'Score', 'e': 'Map', 'f': 'Map',
               'g': 'Video', 'i': 'Audio Recording', 'j': 'Music Recording',
               'k': 'Image', 'm': 'Electronic Resource', 'o': 'Kit', 'r': 'Object'}
    document_type = None
    if material_type in NONTEXT:
        document_type = NONTEXT[material_type]
    elif biblio_level == 'm':
        document_type = "Book"            # standalone monograph (even if in a series)
    elif biblio_level == 's':
        document_type = "Journal"         # the serial itself
    elif biblio_level in ('a', 'b'):
        # Component part: journal article if the host is a serial, else a book chapter.
        document_type = "Journal Article" if journal_title else "Book Chapter"
    elif biblio_level == 'c':
        document_type = "Book"            # collection
    # Fallbacks when the leader is missing/uninformative.
    if not document_type:
        if journal_title:
            document_type = "Journal Article"
        elif isbn:
            document_type = "Book"
        elif issn:
            document_type = "Journal"
        elif series:
            document_type = "Book Chapter"
        else:
            document_type = "Book"
    
    # Create BiblioRecord
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        contributors=contributors,
        year=year,
        publisher_name=publisher,
        place_of_publication=place,
        isbn=isbn,
        issn=issn,
        urls=urls,
        abstract=abstract,  # MARC 520 summary/abstract, if present
        language=language,
        format=document_type,  # Use determined document type
        subjects=subjects,
        series=series,  # For book chapters, store the book title in series field
        extent=extent,
        edition=edition,
        
        # Journal article fields
        journal_title=journal_title,
        volume=volume,
        issue=issue,
        pages=pages,
        doi=doi,
        document_type=document_type,
        schema=raw_record.get('schema')
    )


def parse_rdfxml(raw_record, namespaces):
    """Parse RDF/XML format records (like those from DNB)."""
    data = raw_record['data']
    record_id = raw_record.get('id', 'unknown')
    
    # Debug log to see what we're parsing
    logger.debug(f"Parsing RDFxml record {record_id}")
    
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
    
    # Log raw data for verbose debugging
    logger.debug(f"Raw record data for {record_id}:")
    logger.debug(raw_record['raw_xml'][:500] + "..." if len(raw_record['raw_xml']) > 500 else raw_record['raw_xml'])
    
    # Find description element
    desc = data.find('.//rdf:Description', ns)
    if desc is None:
        logger.warning(f"No RDF:Description found in record {record_id}")
        return None
    
    # Find title - direct approach 
    title = "Untitled"
    title_elem = desc.find('./dc:title', ns)
    if title_elem is not None and title_elem.text:
        title = title_elem.text.strip()
        logger.debug(f"Found title: {title}")
    else:
        logger.warning(f"No title found for record {record_id}")
    
    # Find alternative titles
    alt_title_elem = desc.find('./dcterms:alternative', ns)
    if alt_title_elem is not None and alt_title_elem.text:
        alt_title = alt_title_elem.text.strip()
        if alt_title and ":" not in title:
            title = f"{title}: {alt_title}"
            logger.debug(f"Added alternative title, full title is now: {title}")
    
    # Initialize contributor lists
    authors = []
    editors = []
    translators = []
    contributors = []
    
    seen_names = set()  # Track seen names to avoid duplicates
    
    # Function to clean author name (remove duplicate entries, trailing commas, etc.)
    def clean_author_name(name):
        # Remove trailing commas and whitespace
        name = re.sub(r',\s*$', '', name.strip())
        return name
    
    # Helper function to detect and process contributor roles
    def process_name_with_role(name, seen_names_set):
        """
        Process a name string, detect role patterns, clean the name,
        and return the cleaned name and detected role.
        
        Args:
            name: The raw name string potentially containing role information
            seen_names_set: Set to track seen names (to avoid duplicates)
            
        Returns:
            tuple: (cleaned_name, role, is_duplicate)
        """
        if name is None:
            return None, None, True
            
        name = name.strip()
        if not name:
            return None, None, True
        
        # Detect editor patterns (German and English)
        editor_patterns = [
            r'\([Hh]g\.?\)', r'\([Hh]rsg\.?\)', r'\([Ee]d\.?\)', r'\([Ee]ditor[s]?\)',
            r'\b[Hh]g\.', r'\b[Hh]rsg\.', r'\b[Ee]d\.', r'\b[Ee]ditor[s]?\b',
            r'[,\s]+[Hh]g\.', r'[,\s]+[Hh]rsg\.', r'[,\s]+[Ee]d\.', r'[,\s]+[Ee]ditor[s]?'
        ]
        
        # Detect translator patterns (German and English)
        translator_patterns = [
            r'\([Üü]bers\.?\)', r'\([Tt]rans\.?\)', r'\([Tt]ranslator[s]?\)',
            r'\b[Üü]bers\.', r'\b[Tt]rans\.', r'\b[Tt]ranslator[s]?\b',
            r'[,\s]+[Üü]bers\.', r'[,\s]+[Tt]rans\.', r'[,\s]+[Tt]ranslator[s]?'
        ]
        
        # Check for editor patterns
        is_editor = any(re.search(pattern, name) for pattern in editor_patterns)
        
        # Check for translator patterns
        is_translator = any(re.search(pattern, name) for pattern in translator_patterns)
        
        # Determine role
        if is_editor:
            role = "editor"
        elif is_translator:
            role = "translator"
        else:
            role = "author"
        
        # Clean the name by removing role designations
        if is_editor:
            # Remove editor designations
            for pattern in editor_patterns:
                name = re.sub(pattern, '', name)
        
        if is_translator:
            # Remove translator designations
            for pattern in translator_patterns:
                name = re.sub(pattern, '', name)
        
        # Clean up remaining punctuation/whitespace
        name = re.sub(r'\(\s*\)', '', name)  # Empty parentheses
        name = re.sub(r'\s+', ' ', name)     # Multiple spaces
        name = re.sub(r'[\s,;:\.]+$', '', name)  # Trailing punctuation/whitespace
        name = re.sub(r'^[\s,;:\.]+', '', name)  # Leading punctuation/whitespace
        name = clean_person_name(name.strip())

        if not name:
            return None, None, True

        # Duplicate check on a normalized key so "Jürgen Habermas" and
        # "Habermas, Jürgen" are recognized as the same person.
        norm_key = ' '.join(sorted(re.sub(r'[^\w\s]', ' ', name.lower()).split()))
        is_duplicate = norm_key in seen_names_set
        if not is_duplicate:
            seen_names_set.add(norm_key)

        return name, role, is_duplicate
    
    # Process P60327 field (contributor statement)
    contributor_statement = desc.find('./rdau:P60327', ns)
    if contributor_statement is not None and contributor_statement.text:
        statement_text = contributor_statement.text.strip()
        logger.debug(f"Author statement (P60327): {statement_text}")
        
        # First check for specific editorial patterns
        if "herausgegeben von" in statement_text:
            # Extract editors from the editorial statement
            editor_match = re.search(r'herausgegeben von\s+(.+?)(?:;|$)', statement_text)
            if editor_match:
                editors_text = editor_match.group(1).strip()
                # Split by "und" or "and" or commas
                editor_names = re.split(r'\s+(?:und|and)\s+|,\s*', editors_text)
                for name in editor_names:
                    clean_name, role, is_duplicate = process_name_with_role(name, seen_names)
                    if clean_name and not is_duplicate:
                        editors.append(clean_name)
                        logger.debug(f"Added editor from 'herausgegeben von': {clean_name}")
                
                # Remove the processed part to avoid duplication
                statement_text = statement_text.replace(editor_match.group(0), "")
                
        # Check for (Hg.) pattern - use a separate regex to catch all variations
        hg_pattern = re.compile(r'([^,;]+(?:\([Hh]g\.?\))[^,;]*)')
        editor_matches = hg_pattern.findall(statement_text)
        
        for match in editor_matches:
            clean_name, role, is_duplicate = process_name_with_role(match, seen_names)
            if clean_name and not is_duplicate and role == "editor":
                editors.append(clean_name)
                logger.debug(f"Added editor from (Hg.) pattern: {clean_name}")
                
            # Remove this match from the statement
            statement_text = statement_text.replace(match, '')
        
        # Clean up the statement after removing processed parts
        statement_text = re.sub(r'[,;]\s*[,;]', ',', statement_text)  # Clean up double separators
        statement_text = statement_text.strip(',; ')
        
        # Check for translator patterns
        if "Übers." in statement_text or "Übertragung" in statement_text or "übersetzt" in statement_text:
            # Extract translators
            trans_match = re.search(r'(?:Übers|Übertragung|übersetzt)[^:]*[:\.]\s*([^\.]+)', statement_text, re.IGNORECASE)
            if trans_match:
                translator_text = trans_match.group(1).strip()
                trans_names = re.split(r'\s+(?:und|and)\s+|,\s*', translator_text)
                for name in trans_names:
                    clean_name, role, is_duplicate = process_name_with_role(name, seen_names)
                    if clean_name and not is_duplicate:
                        translators.append(clean_name)
                        logger.debug(f"Added translator: {clean_name}")
                
                # Remove the processed part
                statement_text = statement_text.replace(trans_match.group(0), "")
        
        # Process any remaining names in the statement as authors
        if statement_text.strip():
            # Split by common separators
            name_parts = re.split(r',\s*|\s*;\s*|\s+und\s+|\s+and\s+', statement_text)
            for part in name_parts:
                if part.strip():
                    clean_name, role, is_duplicate = process_name_with_role(part, seen_names)
                    if clean_name and not is_duplicate:
                        if role == "editor":
                            editors.append(clean_name)
                            logger.debug(f"Added editor from remaining parts: {clean_name}")
                        elif role == "translator":
                            translators.append(clean_name)
                            logger.debug(f"Added translator from remaining parts: {clean_name}")
                        else:
                            authors.append(clean_name)
                            logger.debug(f"Added author from remaining parts: {clean_name}")
    
    # Extract authors from creator elements
    for creator_path in ['./dcterms:creator', './dc:creator']:
        creator_elems = desc.findall(creator_path, ns)
        for creator_elem in creator_elems:
            creator_resource = creator_elem.get('{'+ns['rdf']+'}resource')
            if creator_resource:
                logger.debug(f"Found creator resource: {creator_resource}")
                creator_desc = data.find(f'.//rdf:Description[@rdf:about="{creator_resource}"]', ns)
                if creator_desc is not None:
                    name_elem = creator_desc.find('./gndo:preferredName', ns)
                    if name_elem is not None and name_elem.text:
                        clean_name, role, is_duplicate = process_name_with_role(name_elem.text, seen_names)
                        if clean_name and not is_duplicate:
                            if role == "editor":
                                editors.append(clean_name)
                                logger.debug(f"Added editor from resource: {clean_name}")
                            elif role == "translator":
                                translators.append(clean_name)
                                logger.debug(f"Added translator from resource: {clean_name}")
                            else:
                                authors.append(clean_name)
                                logger.debug(f"Added author from resource: {clean_name}")
                continue
                
            # If creator contains text directly
            if creator_elem.text and creator_elem.text.strip():
                clean_name, role, is_duplicate = process_name_with_role(creator_elem.text, seen_names)
                if clean_name and not is_duplicate:
                    if role == "editor":
                        editors.append(clean_name)
                        logger.debug(f"Added editor from direct text: {clean_name}")
                    elif role == "translator":
                        translators.append(clean_name)
                        logger.debug(f"Added translator from direct text: {clean_name}")
                    else:
                        authors.append(clean_name)
                        logger.debug(f"Added author from direct text: {clean_name}")
                continue
                
            # If creator contains nested elements
            nested_nodes = creator_elem.findall('.//*', ns)
            for node in nested_nodes:
                if 'preferredName' in node.tag and node.text:
                    clean_name, role, is_duplicate = process_name_with_role(node.text, seen_names)
                    if clean_name and not is_duplicate:
                        if role == "editor":
                            editors.append(clean_name)
                            logger.debug(f"Added editor from nested element: {clean_name}")
                        elif role == "translator":
                            translators.append(clean_name)
                            logger.debug(f"Added translator from nested element: {clean_name}")
                        else:
                            authors.append(clean_name)
                            logger.debug(f"Added author from nested element: {clean_name}")
                    break
    
    # Extract authors from marcRole elements - using specific role codes
    role_mapping = {
        'aut': 'author',
        'cre': 'author',
        'edt': 'editor',
        'hrg': 'editor',  # German editor role
        'trl': 'translator',
        'ths': 'author',  # Thesis advisor typically listed as author
        'ctb': 'contributor'  # General contributor
    }
    
    for role_code, role_type in role_mapping.items():
        role_elems = desc.findall(f'./marcRole:{role_code}', ns)
        for role_elem in role_elems:
            # Resource reference
            resource = role_elem.get('{'+ns['rdf']+'}resource')
            if resource:
                desc_elem = data.find(f'.//rdf:Description[@rdf:about="{resource}"]', ns)
                if desc_elem is not None:
                    name_elem = desc_elem.find('./gndo:preferredName', ns)
                    if name_elem is not None and name_elem.text:
                        clean_name, detected_role, is_duplicate = process_name_with_role(name_elem.text, seen_names)
                        # Use the role from marcRole if detected_role is "author" (default)
                        # Otherwise, use the detected role (e.g., if name contains editor pattern)
                        actual_role = detected_role if detected_role != "author" else role_type
                        
                        if clean_name and not is_duplicate:
                            if actual_role == "editor":
                                editors.append(clean_name)
                                logger.debug(f"Added editor from marcRole:{role_code}: {clean_name}")
                            elif actual_role == "translator":
                                translators.append(clean_name)
                                logger.debug(f"Added translator from marcRole:{role_code}: {clean_name}")
                            elif actual_role == "author":
                                authors.append(clean_name)
                                logger.debug(f"Added author from marcRole:{role_code}: {clean_name}")
                            else:
                                contributors.append({"name": clean_name, "role": actual_role})
                                logger.debug(f"Added contributor from marcRole:{role_code}: {clean_name}")
                continue
            
            # Nested description
            for node_desc in role_elem.findall('./rdf:Description', ns):
                name_elem = node_desc.find('./gndo:preferredName', ns)
                if name_elem is not None and name_elem.text:
                    clean_name, detected_role, is_duplicate = process_name_with_role(name_elem.text, seen_names)
                    actual_role = detected_role if detected_role != "author" else role_type
                    
                    if clean_name and not is_duplicate:
                        if actual_role == "editor":
                            editors.append(clean_name)
                            logger.debug(f"Added editor from nested marcRole:{role_code}: {clean_name}")
                        elif actual_role == "translator":
                            translators.append(clean_name)
                            logger.debug(f"Added translator from nested marcRole:{role_code}: {clean_name}")
                        elif actual_role == "author":
                            authors.append(clean_name)
                            logger.debug(f"Added author from nested marcRole:{role_code}: {clean_name}")
                        else:
                            contributors.append({"name": clean_name, "role": actual_role})
                            logger.debug(f"Added contributor from nested marcRole:{role_code}: {clean_name}")
            
            # NodeID reference
            node_id = role_elem.get('{'+ns['rdf']+'}nodeID')
            if node_id:
                node_desc = data.find(f'.//rdf:Description[@rdf:nodeID="{node_id}"]', ns)
                if node_desc is not None:
                    name_elem = node_desc.find('./gndo:preferredName', ns)
                    if name_elem is not None and name_elem.text:
                        clean_name, detected_role, is_duplicate = process_name_with_role(name_elem.text, seen_names)
                        actual_role = detected_role if detected_role != "author" else role_type
                        
                        if clean_name and not is_duplicate:
                            if actual_role == "editor":
                                editors.append(clean_name)
                                logger.debug(f"Added editor from marcRole:{role_code} nodeID: {clean_name}")
                            elif actual_role == "translator":
                                translators.append(clean_name)
                                logger.debug(f"Added translator from marcRole:{role_code} nodeID: {clean_name}")
                            elif actual_role == "author":
                                authors.append(clean_name)
                                logger.debug(f"Added author from marcRole:{role_code} nodeID: {clean_name}")
                            else:
                                contributors.append({"name": clean_name, "role": actual_role})
                                logger.debug(f"Added contributor from marcRole:{role_code} nodeID: {clean_name}")
    
    logger.debug(f"Final author list: {authors}")
    logger.debug(f"Final editor list: {editors}")
    logger.debug(f"Final translator list: {translators}")
    
    # Find year
    year = None
    issued_elem = desc.find('./dcterms:issued', ns)
    if issued_elem is not None and issued_elem.text:
        # Extract year
        match = re.search(r'\b(1\d{3}|20\d{2})\b', issued_elem.text)
        if match:
            year = match.group(1)
            logger.debug(f"Found year: {year}")
    
    # Find publisher - separately handling name and place
    publisher_name = None
    publisher_elem = desc.find('./dc:publisher', ns)
    if publisher_elem is not None and publisher_elem.text:
        publisher_name = publisher_elem.text.strip()
        logger.debug(f"Found publisher: {publisher_name}")
    
    # Find place of publication
    places = []
    place_elems = desc.findall('./rdau:P60163', ns)
    for place_elem in place_elems:
        if place_elem is not None and place_elem.text and place_elem.text.strip():
            places.append(place_elem.text.strip())
    
    place_of_publication = ", ".join(places) if places else None
    if place_of_publication:
        logger.debug(f"Found place of publication: {place_of_publication}")
    
    # Check for publication statement that might have both
    pub_statement = desc.find('./rdau:P60333', ns)
    if pub_statement is not None and pub_statement.text:
        statement = pub_statement.text.strip()
        logger.debug(f"Found publication statement: {statement}")
        if not place_of_publication or not publisher_name:
            parts = statement.split(" : ", 1)
            if len(parts) > 1:
                if not place_of_publication:
                    place_of_publication = parts[0].strip()
                    logger.debug(f"Extracted place from statement: {place_of_publication}")
                if not publisher_name:
                    pub_part = parts[1].strip()
                    pub_part = re.sub(r',?\s*\[\d{4}\]$', '', pub_part)
                    publisher_name = pub_part
                    logger.debug(f"Extracted publisher from statement: {publisher_name}")
    
    # Find edition
    edition = None
    edition_elem = desc.find('./bibo:edition', ns)
    if edition_elem is not None and edition_elem.text:
        edition = edition_elem.text.strip()
        logger.debug(f"Found edition: {edition}")
    
    # Find extent (number of pages, etc.)
    extent = None
    extent_elem = desc.find('./isbd:P1053', ns) or desc.find('./dcterms:extent', ns)
    if extent_elem is not None and extent_elem.text:
        extent = extent_elem.text.strip()
        logger.debug(f"Found extent: {extent}")
    
    # Try to extract page info from extent
    pages = None
    if extent:
        page_match = re.search(r'(\d+)(?:\s*[-–]\s*(\d+))?\s*(?:p|pages|S)', extent, re.IGNORECASE)
        if page_match:
            if page_match.group(2):  # Range
                pages = f"{page_match.group(1)}-{page_match.group(2)}"
            else:  # Single page count
                pages = page_match.group(1)
            logger.debug(f"Extracted pages from extent: {pages}")
    
    # Find document type
    document_type = None
    type_elem = desc.find('./dcterms:type', ns) or desc.find('./dc:type', ns)
    if type_elem is not None:
        # Check for resource reference
        resource = type_elem.get('{'+ns['rdf']+'}resource')
        if resource:
            # Extract type from URI
            type_parts = resource.split('/')
            if type_parts:
                document_type = type_parts[-1]
                logger.debug(f"Found document type from resource: {document_type}")
        # Or direct text
        elif type_elem.text:
            document_type = type_elem.text.strip()
            logger.debug(f"Found document type from text: {document_type}")
    
    # Find series and journal info
    series = None
    journal_title = None
    volume = None
    issue = None
    
    # Check for series
    series_elem = desc.find('./dcterms:isPartOf', ns)
    if series_elem is not None:
        # Text content
        if series_elem.text:
            series = series_elem.text.strip()
            logger.debug(f"Found series: {series}")
        # Resource reference
        else:
            resource = series_elem.get('{'+ns['rdf']+'}resource')
            if resource:
                logger.debug(f"Found series resource: {resource}")
                # Find the referenced resource
                series_desc = data.find(f'.//rdf:Description[@rdf:about="{resource}"]', ns)
                if series_desc is not None:
                    # Try to get the title
                    title_elem = series_desc.find('./dc:title', ns) or series_desc.find('./dcterms:title', ns)
                    if title_elem is not None and title_elem.text:
                        series = title_elem.text.strip()
                        logger.debug(f"Extracted series from resource: {series}")
    
    # Check for biblio:Journal relation
    journal_elem = desc.find('./bibo:Journal', ns)
    if journal_elem is not None:
        resource = journal_elem.get('{'+ns['rdf']+'}resource')
        if resource:
            logger.debug(f"Found journal resource: {resource}")
            # Find the referenced resource
            journal_desc = data.find(f'.//rdf:Description[@rdf:about="{resource}"]', ns)
            if journal_desc is not None:
                # Try to get the title
                title_elem = journal_desc.find('./dc:title', ns) or journal_desc.find('./dcterms:title', ns)
                if title_elem is not None and title_elem.text:
                    journal_title = title_elem.text.strip()
                    document_type = "Journal Article"
                    logger.debug(f"Found journal title: {journal_title}")
    
    # Volume and issue
    volume_elem = desc.find('./bibo:volume', ns)
    if volume_elem is not None and volume_elem.text:
        volume = volume_elem.text.strip()
        logger.debug(f"Found volume: {volume}")
    
    issue_elem = desc.find('./bibo:issue', ns)
    if issue_elem is not None and issue_elem.text:
        issue = issue_elem.text.strip()
        logger.debug(f"Found issue: {issue}")
    
    # Check if this is a book chapter
    chapter_elem = desc.find('./bibo:chapter', ns)
    if chapter_elem is not None or (document_type and "chapter" in document_type.lower()):
        document_type = "Book Chapter"
        logger.debug("Determined document is a book chapter")
        
        # Series field will be used for book title
    
    # Find ISBN
    isbn = None
    for isbn_field in ['isbn13', 'isbn10', 'isbn', 'gtin14']:
        isbn_elem = desc.find(f'./bibo:{isbn_field}', ns)
        if isbn_elem is not None and isbn_elem.text:
            isbn = isbn_elem.text.strip()
            logger.debug(f"Found ISBN ({isbn_field}): {isbn}")
            break
    
    # Find ISSN
    issn = None
    issn_elem = desc.find('./bibo:issn', ns)
    if issn_elem is not None and issn_elem.text:
        issn = issn_elem.text.strip()
        logger.debug(f"Found ISSN: {issn}")
    
    # Find DOI
    doi = None
    doi_elem = desc.find('./bibo:doi', ns)
    if doi_elem is not None and doi_elem.text:
        doi = doi_elem.text.strip()
        logger.debug(f"Found DOI: {doi}")
    
    # Find subjects
    subjects = []
    seen_subjects = set()
    subject_elems = desc.findall('./dcterms:subject', ns)
    for elem in subject_elems:
        resource = elem.get('{'+ns['rdf']+'}resource')
        if resource:
            subject = resource.split('/')[-1]
            if subject and subject not in seen_subjects:
                subjects.append(subject)
                seen_subjects.add(subject)
                logger.debug(f"Found subject from resource: {subject}")
                continue
        
        if elem.text and elem.text.strip():
            subject = elem.text.strip()
            if subject not in seen_subjects:
                subjects.append(subject)
                seen_subjects.add(subject)
                logger.debug(f"Found subject from text: {subject}")
    
    # Also check dc:subject
    dc_subject_elems = desc.findall('./dc:subject', ns)
    for elem in dc_subject_elems:
        if elem.text and elem.text.strip():
            subject = elem.text.strip()
            if subject not in seen_subjects:
                subjects.append(subject)
                seen_subjects.add(subject)
                logger.debug(f"Found dc:subject: {subject}")
    
    logger.debug(f"Found {len(subjects)} subjects")
    
    # Find language
    language = None
    language_elem = desc.find('./dcterms:language', ns)
    if language_elem is not None:
        resource = language_elem.get('{'+ns['rdf']+'}resource')
        if resource:
            parts = resource.split('/')
            if parts:
                language = parts[-1]
                logger.debug(f"Found language from resource: {language}")
        elif language_elem.text and language_elem.text.strip():
            language = language_elem.text.strip()
            logger.debug(f"Found language from text: {language}")
    
    # Find abstract/description
    abstract = None
    for desc_tag in ['description', 'abstract', 'P60493']:
        for ns_prefix in ['dc', 'dcterms', 'rdau']:
            desc_elem = desc.find(f'./{ns_prefix}:{desc_tag}', ns)
            if desc_elem is not None and desc_elem.text:
                abstract = desc_elem.text.strip()
                logger.debug(f"Found abstract from {ns_prefix}:{desc_tag}: {abstract[:100]}...")
                break
        if abstract:
            break
    
    # Find URLs
    urls = []
    seen_urls = set()
    
    for primaryTopic_elem in desc.findall('./foaf:primaryTopic', ns):
        resource = primaryTopic_elem.get('{'+ns['rdf']+'}resource')
        if resource and resource.startswith('http') and resource not in seen_urls:
            urls.append(resource)
            seen_urls.add(resource)
            logger.debug(f"Found URL from primaryTopic: {resource}")
    
    for like_elem in desc.findall('./umbel:isLike', ns):
        resource = like_elem.get('{'+ns['rdf']+'}resource')
        if resource and resource.startswith('http') and resource not in seen_urls:
            urls.append(resource)
            seen_urls.add(resource)
            logger.debug(f"Found URL from isLike: {resource}")
    
    logger.debug(f"Found {len(urls)} URLs")
    
    # Log record summary
    logger.debug(f"Record summary for {record_id}:")
    logger.debug(f"  Title: {title}")
    logger.debug(f"  Author count: {len(authors)}")
    logger.debug(f"  Editor count: {len(editors)}")
    logger.debug(f"  Year: {year}")
    logger.debug(f"  Publisher: {publisher_name}")
    logger.debug(f"  Type: {document_type}")
    
    # Deduplicate authors by normalized name (P60327 statement vs dcterms:creator
    # can list the same person as "Jürgen Habermas" and "Habermas, Jürgen").
    _seen = set()
    _deduped = []
    for a in authors:
        k = ' '.join(sorted(re.sub(r'[^\w\s]', ' ', a.lower()).split()))
        if k and k not in _seen:
            _seen.add(k)
            _deduped.append(a)
    authors = _deduped

    # Fall back to ISBN/ISSN when the RDF type was generic/missing.
    if not document_type or document_type.lower() in ('document', 'text', 'resource'):
        document_type = infer_document_type(None, isbn, issn, None)

    # Create and return BiblioRecord
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        contributors=contributors,
        year=year,
        publisher_name=publisher_name,
        place_of_publication=place_of_publication,
        isbn=isbn,
        issn=issn,
        urls=urls,
        abstract=abstract,
        language=language,
        format=document_type,
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
