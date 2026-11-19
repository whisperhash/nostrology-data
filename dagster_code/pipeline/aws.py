"""boto3 client factory — the ONLY module that touches boto3, and it imports it LAZILY (inside
the call) so the pure-logic modules that import this one stay importable without boto3 installed
(local tests inject mock clients and never call client()). Runtime credentials come from the box
instance role via the standard boto3 chain — never passed in, never logged.
"""


def client(service: str, region: str):
    """Return a boto3 client for `service` in `region` (e.g. client("rds", "us-east-1"))."""
    import boto3

    return boto3.client(service, region_name=region)
