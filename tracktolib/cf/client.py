from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

try:
    import niquests
except ImportError:
    raise ImportError('Please install niquests or tracktolib with "cf" to use this module')

if TYPE_CHECKING:
    from urllib3.util.retry import Retry

    from tracktolib.cf.types import DnsRecord


class CloudflareError(Exception):
    """Error raised when a Cloudflare API call fails."""

    def __init__(self, message: str, status_code: int | None = None, errors: list | None = None):
        self.status_code = status_code
        self.errors = errors or []
        super().__init__(message)


@dataclass
class CloudflareDNSClient:
    """
    Async Cloudflare DNS API client for managing DNS records.

    Requires CLOUDFLARE_API_TOKEN and CLOUDFLARE_ZONE_ID environment variables,
    or pass them directly to the constructor.
    """

    zone_id: str | None = field(default_factory=lambda: os.environ.get("CLOUDFLARE_ZONE_ID"))
    token: str | None = field(default_factory=lambda: os.environ.get("CLOUDFLARE_API_TOKEN"))
    base_url: str = "https://api.cloudflare.com/client/v4"
    retries: int | Retry = 0
    hooks: Any = None
    session: niquests.AsyncSession = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.token:
            raise ValueError("CLOUDFLARE_API_TOKEN environment variable is required")
        if not self.zone_id:
            raise ValueError("CLOUDFLARE_ZONE_ID environment variable is required")

        self.session = niquests.AsyncSession(
            base_url=self.base_url,
            retries=self.retries,
            hooks=self.hooks,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )

    async def __aenter__(self) -> "CloudflareDNSClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying session."""
        await self.session.close()

    def _handle_response(self, response: niquests.Response) -> dict:
        """Handle Cloudflare API response and raise on errors."""
        data = response.json()
        if not data.get("success", False):
            errors = data.get("errors", [])
            error_messages = [e.get("message", str(e)) for e in errors]
            raise CloudflareError(
                f"Cloudflare API error: {', '.join(error_messages)}",
                status_code=response.status_code,
                errors=errors,
            )
        return data

    # DNS Records

    async def get_dns_record(self, name: str, record_type: str = "CNAME") -> DnsRecord | None:
        """
        Get a DNS record by name and type.

        Returns None if the record doesn't exist.
        """
        params = {"type": record_type, "name": name}
        response = await self.session.get(f"/zones/{self.zone_id}/dns_records", params=params)
        data = self._handle_response(response)

        results = data.get("result", [])
        if not results:
            return None

        return cast("DnsRecord", results[0])

    async def create_dns_record(
        self,
        name: str,
        content: str,
        record_type: str = "CNAME",
        *,
        ttl: int = 1,
        proxied: bool = False,
        comment: str | None = None,
    ) -> DnsRecord:
        """
        Create a DNS record.

        The ttl parameter defaults to 1 (automatic). Set to a value between 60-86400 for manual TTL.
        """
        payload: dict = {
            "type": record_type,
            "name": name,
            "content": content,
            "ttl": ttl,
            "proxied": proxied,
        }
        if comment:
            payload["comment"] = comment

        response = await self.session.post(f"/zones/{self.zone_id}/dns_records", json=payload)
        data = self._handle_response(response)
        return cast("DnsRecord", data["result"])

    async def update_dns_record(
        self,
        record_id: str,
        *,
        content: str | None = None,
        name: str | None = None,
        record_type: str | None = None,
        ttl: int | None = None,
        proxied: bool | None = None,
        comment: str | None = None,
    ) -> DnsRecord:
        """
        Update a DNS record by ID using a PATCH request.

        Only the provided fields will be updated; omitted fields remain unchanged.
        """
        payload: dict = {}
        if content is not None:
            payload["content"] = content
        if name is not None:
            payload["name"] = name
        if record_type is not None:
            payload["type"] = record_type
        if ttl is not None:
            payload["ttl"] = ttl
        if proxied is not None:
            payload["proxied"] = proxied
        if comment is not None:
            payload["comment"] = comment

        response = await self.session.patch(f"/zones/{self.zone_id}/dns_records/{record_id}", json=payload)
        data = self._handle_response(response)
        return cast("DnsRecord", data["result"])

    async def delete_dns_record(self, record_id: str) -> None:
        """Delete a DNS record by ID."""
        response = await self.session.delete(f"/zones/{self.zone_id}/dns_records/{record_id}")
        self._handle_response(response)

    async def delete_dns_record_by_name(self, name: str, record_type: str = "CNAME") -> bool:
        """
        Delete a DNS record by name and type.

        Returns True if deleted, False if the record didn't exist.
        """
        record = await self.get_dns_record(name, record_type)
        if record is None:
            return False

        await self.delete_dns_record(record["id"])
        return True

    async def dns_record_exists(self, name: str, record_type: str = "CNAME") -> bool:
        """Check if a DNS record exists."""
        return await self.get_dns_record(name, record_type) is not None
