"""Loads the shared endpoints.json manifest and exposes SRU/OAI/IxTheo endpoint
dicts + namespaces. The manifest uses the TypeScript field names (camelCase);
here they are mapped to the snake_case keys the Python code expects. Edit
endpoints.json (kept identical across all three repos), not this file."""
import json
import os

_KEY_MAP = {
    'defaultSchema': 'default_schema',
    'defaultMetadataPrefix': 'default_metadata_prefix',
    'queryParams': 'query_params',
    'baseUrl': 'base_url',
    'availableSchemas': 'available_schemas',
}


def _snake(ep):
    return {_KEY_MAP.get(k, k): v for k, v in ep.items()}


_MANIFEST = json.load(open(os.path.join(os.path.dirname(__file__), 'endpoints.json'), encoding='utf-8'))
MANIFEST_NAMESPACES = _MANIFEST['namespaces']
SRU_ENDPOINTS = {k: _snake(v) for k, v in _MANIFEST['sru'].items()}
OAI_ENDPOINTS = {k: _snake(v) for k, v in _MANIFEST['oai'].items()}
IXTHEO_ENDPOINTS = {k: _snake(v) for k, v in _MANIFEST['ixtheo'].items()}
