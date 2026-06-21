"""OpenSearch client factory. Single place that knows how to connect.

Target = AWS Managed OpenSearch Service (HTTPS + IAM SigV4 signing). Local
dockerized OpenSearch (http, security disabled) is kept only for dev. The auth
mode is chosen by config.OPENSEARCH_AUTH:

    iam   -> AWS Managed domain. Requests signed with the ambient AWS credentials
             (EC2 instance role / env / ~/.aws). No username/password anywhere.
    basic -> HTTPS + username/password (FGAC master user).
    local -> dockerized OpenSearch over http, security off (dev only).
"""
from __future__ import annotations

from opensearchpy import OpenSearch, RequestsHttpConnection

from .. import config


def _aws_client() -> OpenSearch:
    """AWS Managed OpenSearch over HTTPS, authenticated with IAM SigV4."""
    import boto3
    from opensearchpy import AWSV4SignerAuth

    credentials = boto3.Session().get_credentials()
    if credentials is None:
        raise RuntimeError(
            "No AWS credentials found for OPENSEARCH_AUTH=iam. Configure an EC2 "
            "instance role, env vars, or ~/.aws (and the domain access policy / "
            "es:ESHttp* permission)."
        )
    auth = AWSV4SignerAuth(credentials, config.AWS_REGION, config.OPENSEARCH_AWS_SERVICE)
    return OpenSearch(
        hosts=[{"host": config.OPENSEARCH_HOST, "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        pool_maxsize=20,
    )


def _basic_client() -> OpenSearch:
    """HTTPS + username/password (FGAC master user / secured self-hosted)."""
    return OpenSearch(
        hosts=[{"host": config.OPENSEARCH_HOST, "port": config.OPENSEARCH_PORT}],
        http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        pool_maxsize=20,
    )


def _local_client() -> OpenSearch:
    """Dockerized OpenSearch over http, security disabled. Dev only."""
    return OpenSearch(
        hosts=[{"host": config.OPENSEARCH_HOST, "port": config.OPENSEARCH_PORT}],
        http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
        use_ssl=config.OPENSEARCH_USE_SSL,
        verify_certs=False,
        ssl_show_warn=False,
    )


def get_client() -> OpenSearch:
    auth = config.OPENSEARCH_AUTH
    if auth == "iam":
        return _aws_client()
    if auth == "basic":
        return _basic_client()
    if auth == "local":
        return _local_client()
    raise ValueError(
        f"Unknown OPENSEARCH_AUTH: {auth!r} (expected 'iam', 'basic', or 'local')"
    )
