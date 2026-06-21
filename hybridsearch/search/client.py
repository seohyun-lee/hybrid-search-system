"""OpenSearch client factory. Single place that knows connection settings."""
from __future__ import annotations

from opensearchpy import OpenSearch

from .. import config


def get_client() -> OpenSearch:
    return OpenSearch(
        hosts=[{"host": config.OPENSEARCH_HOST, "port": config.OPENSEARCH_PORT}],
        http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
        use_ssl=config.OPENSEARCH_USE_SSL,
        verify_certs=False,
        ssl_show_warn=False,
    )
