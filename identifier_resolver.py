"""
identifier_resolver.py — Resolve bibliographic identifiers (DOI, PMID, ISBN, URL)
to normalized metadata dicts. Pure-stdlib + `requests`; no heavy deps.

This adds the one capability CrispLib previously lacked compared to citer:
turning a known identifier into a citation record.

Public API:
    resolve_identifier(identifier, type=None, timeout=15) -> dict
    detect_identifier_type(identifier)                    -> str | None

Returned dict shape (subset, all keys optional except `source`):
    {
      "source":   "crossref" | "ncbi" | "openlibrary" | "citoid",
      "type":     "article" | "book" | "webpage" | ...,
      "title":    str,
      "authors":  [str, ...],          # "Family, Given"
      "year":     str,
      "publisher": str,
      "journal":  str,
      "volume":   str,
      "issue":    str,
      "pages":    str,
      "doi":      str,
      "isbn":     str,
      "pmid":     str,
      "url":      str,
      "abstract": str,
      "raw":      <provider response>,
    }
"""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import quote as _url_quote

import requests

USER_AGENT = "CrispLib-IdentifierResolver/1.0 (+https://github.com/CrispStrobe/CrispLib)"

_DOI_RE = re.compile(r"\b(10\.\d{4,9}/[-._;()/:A-Z0-9]+)\b", re.IGNORECASE)
_ISBN_RE = re.compile(r"\b(?:97[89][- ]?)?(?:\d[- ]?){9}[\dxX]\b")
_PMID_RE = re.compile(r"^\d{1,9}$")
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


# ── Detection ────────────────────────────────────────────────────────────────


def detect_identifier_type(identifier: str) -> Optional[str]:
    """Heuristically classify an identifier string."""
    s = (identifier or "").strip()
    if not s:
        return None
    if _URL_RE.match(s):
        # DOI URL?
        if "doi.org/" in s.lower() or _DOI_RE.search(s):
            return "doi"
        return "url"
    if s.lower().startswith("doi:"):
        return "doi"
    if _DOI_RE.search(s):
        return "doi"
    if s.lower().startswith(("pmid:", "pmid ")):
        return "pmid"
    if s.lower().startswith(("pmc", "pmcid")):
        return "pmcid"
    digits = re.sub(r"[- ]", "", s)
    if re.fullmatch(r"\d{9}[\dxX]|\d{13}", digits):
        return "isbn"
    if _PMID_RE.match(s):
        return "pmid"
    return None


# ── DOI (Crossref via doi.org content negotiation) ───────────────────────────


def resolve_doi(doi: str, timeout: int = 15) -> dict:
    """Fetch CSL-JSON metadata for a DOI from doi.org / Crossref."""
    m = _DOI_RE.search(doi)
    doi_clean = m.group(1) if m else doi.strip().lstrip("doi:").strip().lstrip("/")
    r = requests.get(
        f"https://doi.org/{doi_clean}",
        headers={
            "Accept": "application/vnd.citationstyles.csl+json",
            "User-Agent": USER_AGENT,
        },
        timeout=timeout,
        allow_redirects=True,
    )
    r.raise_for_status()
    j = r.json()

    authors = []
    for a in j.get("author", []) or []:
        fam = a.get("family", "")
        giv = a.get("given", "")
        if fam or giv:
            authors.append(f"{fam}, {giv}".strip(", "))

    year = ""
    for k in ("issued", "published-print", "published-online", "published"):
        if k in j and j[k].get("date-parts"):
            year = str(j[k]["date-parts"][0][0])
            break

    return {
        "source": "crossref",
        "type": j.get("type", ""),
        "title": (j.get("title") or [""])[0] if isinstance(j.get("title"), list) else j.get("title", ""),
        "authors": authors,
        "year": year,
        "publisher": j.get("publisher", ""),
        "journal": (j.get("container-title") or [""])[0] if isinstance(j.get("container-title"), list) else j.get("container-title", ""),
        "volume": str(j.get("volume", "")),
        "issue": str(j.get("issue", "")),
        "pages": str(j.get("page", "")),
        "doi": j.get("DOI", doi_clean),
        "isbn": (j.get("ISBN") or [""])[0] if isinstance(j.get("ISBN"), list) else "",
        "url": j.get("URL", f"https://doi.org/{doi_clean}"),
        "abstract": j.get("abstract", ""),
        "raw": j,
    }


# ── PubMed (NCBI E-utilities) ────────────────────────────────────────────────


def resolve_pmid(pmid: str, timeout: int = 15) -> dict:
    """Fetch metadata for a PubMed ID via NCBI esummary."""
    pid = re.sub(r"\D", "", pmid)
    r = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
        params={"db": "pubmed", "id": pid, "retmode": "json"},
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    r.raise_for_status()
    j = r.json()
    result = (j.get("result") or {}).get(pid) or {}
    if not result:
        raise ValueError(f"PMID {pid} not found")

    authors = [a.get("name", "") for a in result.get("authors", []) if a.get("name")]
    pubdate = result.get("pubdate", "")
    year = pubdate.split(" ")[0] if pubdate else ""

    doi = ""
    for aid in result.get("articleids", []):
        if aid.get("idtype") == "doi":
            doi = aid.get("value", "")
            break

    return {
        "source": "ncbi",
        "type": "article-journal",
        "title": result.get("title", ""),
        "authors": authors,
        "year": year,
        "journal": result.get("fulljournalname") or result.get("source", ""),
        "volume": result.get("volume", ""),
        "issue": result.get("issue", ""),
        "pages": result.get("pages", ""),
        "doi": doi,
        "pmid": pid,
        "url": f"https://pubmed.ncbi.nlm.nih.gov/{pid}/",
        "raw": result,
    }


# ── ISBN (Open Library) ──────────────────────────────────────────────────────


def resolve_isbn(isbn: str, timeout: int = 15) -> dict:
    """Fetch book metadata for an ISBN via Open Library."""
    isbn_clean = re.sub(r"[- ]", "", isbn).upper()
    r = requests.get(
        "https://openlibrary.org/api/books",
        params={
            "bibkeys": f"ISBN:{isbn_clean}",
            "format": "json",
            "jscmd": "data",
        },
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    r.raise_for_status()
    j = r.json()
    key = f"ISBN:{isbn_clean}"
    if key not in j:
        raise ValueError(f"ISBN {isbn_clean} not found in Open Library")
    book = j[key]

    authors = [a.get("name", "") for a in book.get("authors", []) if a.get("name")]
    publishers = [p.get("name", "") for p in book.get("publishers", []) if p.get("name")]
    publish_date = book.get("publish_date", "")
    year_match = re.search(r"\b(\d{4})\b", publish_date)

    return {
        "source": "openlibrary",
        "type": "book",
        "title": book.get("title", ""),
        "authors": authors,
        "year": year_match.group(1) if year_match else "",
        "publisher": ", ".join(publishers),
        "isbn": isbn_clean,
        "pages": str(book.get("number_of_pages", "")),
        "url": book.get("url", f"https://openlibrary.org/isbn/{isbn_clean}"),
        "raw": book,
    }


# ── URL (Wikipedia Citoid) ───────────────────────────────────────────────────


def resolve_url(url: str, timeout: int = 15) -> dict:
    """Resolve a generic URL to citation metadata via Wikipedia's Citoid API."""
    r = requests.get(
        f"https://en.wikipedia.org/api/rest_v1/data/citation/mediawiki/{_url_quote(url, safe='')}",
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=timeout,
    )
    r.raise_for_status()
    arr = r.json()
    if not arr:
        raise ValueError(f"Citoid returned no data for {url}")
    item = arr[0]

    authors = []
    for a in item.get("author", []) or []:
        if isinstance(a, list) and len(a) >= 2:
            authors.append(f"{a[1]}, {a[0]}".strip(", "))
        elif isinstance(a, str):
            authors.append(a)

    date = item.get("date", "")
    year_match = re.search(r"\b(\d{4})\b", date) if date else None

    return {
        "source": "citoid",
        "type": item.get("itemType", "webpage"),
        "title": item.get("title", ""),
        "authors": authors,
        "year": year_match.group(1) if year_match else "",
        "publisher": item.get("publisher") or item.get("websiteTitle", ""),
        "journal": item.get("publicationTitle", ""),
        "volume": item.get("volume", ""),
        "issue": item.get("issue", ""),
        "pages": item.get("pages", ""),
        "doi": item.get("DOI", ""),
        "isbn": item.get("ISBN", [""])[0] if isinstance(item.get("ISBN"), list) else item.get("ISBN", ""),
        "url": item.get("url", url),
        "abstract": item.get("abstractNote", ""),
        "raw": item,
    }


# ── Public dispatcher ────────────────────────────────────────────────────────


def resolve_identifier(identifier: str, id_type: Optional[str] = None, timeout: int = 15) -> dict:
    """
    Resolve any supported identifier to a normalized metadata dict.

    Args:
        identifier: DOI, PMID, ISBN, or URL.
        id_type: Optional explicit hint ("doi" | "pmid" | "isbn" | "url").
                 If omitted, type is auto-detected.
        timeout: HTTP timeout in seconds.

    Raises:
        ValueError: if the type cannot be detected or the lookup fails.
    """
    t = (id_type or detect_identifier_type(identifier) or "").lower()
    if t == "doi":
        return resolve_doi(identifier, timeout=timeout)
    if t == "pmid":
        return resolve_pmid(identifier, timeout=timeout)
    if t == "isbn":
        return resolve_isbn(identifier, timeout=timeout)
    if t == "url":
        return resolve_url(identifier, timeout=timeout)
    raise ValueError(
        f"Unrecognized identifier: {identifier!r}. "
        "Pass id_type=doi|pmid|isbn|url to override detection."
    )


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        print("Usage: python identifier_resolver.py <doi|pmid|isbn|url> [type]")
        sys.exit(1)
    ident = sys.argv[1]
    typ = sys.argv[2] if len(sys.argv) > 2 else None
    out = resolve_identifier(ident, id_type=typ)
    out.pop("raw", None)
    print(json.dumps(out, indent=2, ensure_ascii=False))
