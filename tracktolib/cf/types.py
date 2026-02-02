from typing import NotRequired, TypedDict


class DnsRecord(TypedDict):
    """Cloudflare DNS record response."""

    id: str
    name: str
    type: str
    content: str
    ttl: int
    proxied: bool
    proxiable: NotRequired[bool]
    created_on: NotRequired[str]
    modified_on: NotRequired[str]
    comment: NotRequired[str]
    tags: NotRequired[list[str]]
