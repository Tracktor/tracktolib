import os
from typing import Any, Literal

try:
    import niquests
except ImportError:
    raise ImportError('Please install niquests or tracktolib with "notion" to use this module')

from .models import (
    Block,
    BlockListResponse,
    Database,
    IntrospectTokenResponse,
    Page,
    PageListResponse,
    RevokeTokenResponse,
    SearchResponse,
    TokenResponse,
    User,
    UserListResponse,
)

# API version constants
API_VERSION_2022_06_28 = "2022-06-28"
API_VERSION_2025_09_03 = "2025-09-03"
DEFAULT_API_VERSION = API_VERSION_2025_09_03

ApiVersion = Literal["2022-06-28", "2025-09-03"]


def _use_data_source_api(api_version: str) -> bool:
    """Check if the API version uses data_source endpoints (2025-09-03+)."""
    return api_version >= "2025-09-03"


__all__ = (
    # Auth helpers
    "get_notion_headers",
    # OAuth
    "create_token",
    "introspect_token",
    "revoke_token",
    "refresh_token",
    # Users
    "fetch_users",
    "fetch_user",
    "fetch_me",
    # Pages
    "fetch_page",
    "create_page",
    "update_page",
    # Databases
    "fetch_database",
    "query_database",
    # Blocks
    "fetch_block",
    "fetch_block_children",
    "fetch_append_block_children",
    # Search
    "fetch_search",
)

NOTION_API_URL = "https://api.notion.com"


def _get_notion_token() -> str:
    """Get Notion token from config or environment."""
    token = os.environ.get("NOTION_TOKEN")
    if not token:
        raise ValueError("Notion token not found. Set NOTION_TOKEN env var.")
    return token


def get_notion_headers(api_version: str = "2025-09-03", token: str | None = None):
    """Get headers for Notion API requests."""
    _token = token or _get_notion_token()
    return {
        "Authorization": f"Bearer {_token}",
        "Notion-Version": api_version,
    }


# OAuth endpoints


async def create_token(
    session: niquests.AsyncSession,
    client_id: str,
    client_secret: str,
    code: str,
    redirect_uri: str | None = None,
) -> TokenResponse:
    """Create an access token from an OAuth authorization code."""
    payload: dict[str, str] = {
        "grant_type": "authorization_code",
        "code": code,
    }
    if redirect_uri:
        payload["redirect_uri"] = redirect_uri

    response = await session.post(f"{NOTION_API_URL}/v1/oauth/token", json=payload, auth=(client_id, client_secret))
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


async def introspect_token(
    session: niquests.AsyncSession,
    client_id: str,
    client_secret: str,
    token: str,
) -> IntrospectTokenResponse:
    """Get a token's active status, scope, and issued time."""
    payload = {"token": token}
    response = await session.post(
        f"{NOTION_API_URL}/v1/oauth/introspect", json=payload, auth=(client_id, client_secret)
    )
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


async def revoke_token(
    session: niquests.AsyncSession,
    client_id: str,
    client_secret: str,
    token: str,
) -> RevokeTokenResponse:
    """Revoke an access token."""
    payload = {"token": token}

    response = await session.post(f"{NOTION_API_URL}/v1/oauth/revoke", json=payload, auth=(client_id, client_secret))
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


async def refresh_token(
    session: niquests.AsyncSession,
    client_id: str,
    client_secret: str,
    refresh_token_value: str,
) -> TokenResponse:
    """Refresh an access token, generating new access and refresh tokens."""
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token_value,
    }

    response = await session.post(f"{NOTION_API_URL}/v1/oauth/token", json=payload, auth=(client_id, client_secret))
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


# Users endpoints


async def fetch_users(
    session: niquests.AsyncSession,
    *,
    start_cursor: str | None = None,
    page_size: int | None = None,
) -> UserListResponse:
    """List all users in the workspace."""
    params: dict[str, str] = {}
    if start_cursor:
        params["start_cursor"] = start_cursor
    if page_size:
        params["page_size"] = str(page_size)

    response = await session.get(f"{NOTION_API_URL}/v1/users", params=params or None)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


async def fetch_user(session: niquests.AsyncSession, user_id: str) -> User:
    """Retrieve a user by ID."""
    response = await session.get(f"{NOTION_API_URL}/v1/users/{user_id}")
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


async def fetch_me(session: niquests.AsyncSession) -> User:
    """Retrieve the bot user associated with the token."""
    response = await session.get(f"{NOTION_API_URL}/v1/users/me")
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


# Pages endpoints


async def fetch_page(session: niquests.AsyncSession, page_id: str) -> Page:
    """Retrieve a page by ID."""
    response = await session.get(f"{NOTION_API_URL}/v1/pages/{page_id}")
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


def _convert_parent_for_api_version(parent: dict[str, Any], api_version: str) -> dict[str, Any]:
    """Convert parent dict between database_id and data_source_id based on API version."""
    if _use_data_source_api(api_version):
        # Convert database_id to data_source_id for new API
        if "database_id" in parent:
            return {"data_source_id": parent["database_id"]}
    else:
        # Convert data_source_id to database_id for old API
        if "data_source_id" in parent:
            return {"database_id": parent["data_source_id"]}
    return parent


async def create_page(
    session: niquests.AsyncSession,
    *,
    parent: dict[str, Any],
    properties: dict[str, Any],
    children: list[dict[str, Any]] | None = None,
    icon: dict[str, Any] | None = None,
    cover: dict[str, Any] | None = None,
    api_version: ApiVersion | None = None,
) -> Page:
    """Create a new page.

    For API version 2025-09-03+, parent should use {"data_source_id": "..."}.
    For older API versions, parent should use {"database_id": "..."}.
    The function will automatically convert between the two formats.
    """
    _api_version = api_version or session.headers.get("Notion-Version", DEFAULT_API_VERSION)
    converted_parent = _convert_parent_for_api_version(parent, _api_version)
    payload: dict[str, Any] = {
        "parent": converted_parent,
        "properties": properties,
    }
    if children:
        payload["children"] = children
    if icon:
        payload["icon"] = icon
    if cover:
        payload["cover"] = cover

    response = await session.post(f"{NOTION_API_URL}/v1/pages", json=payload)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


async def update_page(
    session: niquests.AsyncSession,
    page_id: str,
    *,
    properties: dict[str, Any] | None = None,
    archived: bool | None = None,
    icon: dict[str, Any] | None = None,
    cover: dict[str, Any] | None = None,
) -> Page:
    """Update a page's properties."""
    payload: dict[str, Any] = {}
    if properties is not None:
        payload["properties"] = properties
    if archived is not None:
        payload["archived"] = archived
    if icon is not None:
        payload["icon"] = icon
    if cover is not None:
        payload["cover"] = cover

    response = await session.patch(f"{NOTION_API_URL}/v1/pages/{page_id}", json=payload)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


# Databases endpoints


async def fetch_database(
    session: niquests.AsyncSession,
    database_id: str,
    *,
    api_version: ApiVersion | None = None,
) -> Database:
    """Retrieve a database/data source by ID.

    For API version 2025-09-03+, uses /v1/data_sources/{id} endpoint.
    For older API versions, uses /v1/databases/{id} endpoint.
    """
    _api_version = api_version or session.headers.get("Notion-Version", DEFAULT_API_VERSION)
    if _use_data_source_api(_api_version):
        endpoint = f"{NOTION_API_URL}/v1/data_sources/{database_id}"
    else:
        endpoint = f"{NOTION_API_URL}/v1/databases/{database_id}"

    response = await session.get(endpoint)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


async def query_database(
    session: niquests.AsyncSession,
    database_id: str,
    *,
    filter: dict[str, Any] | None = None,
    sorts: list[dict[str, Any]] | None = None,
    start_cursor: str | None = None,
    page_size: int | None = None,
    api_version: ApiVersion | None = None,
) -> PageListResponse:
    """Query a database/data source.

    For API version 2025-09-03+, uses /v1/data_sources/{id}/query endpoint.
    For older API versions, uses /v1/databases/{id}/query endpoint.
    """
    _api_version = api_version or session.headers.get("Notion-Version", DEFAULT_API_VERSION)
    payload: dict[str, Any] = {}
    if filter:
        payload["filter"] = filter
    if sorts:
        payload["sorts"] = sorts
    if start_cursor:
        payload["start_cursor"] = start_cursor
    if page_size:
        payload["page_size"] = page_size

    if _use_data_source_api(_api_version):
        endpoint = f"{NOTION_API_URL}/v1/data_sources/{database_id}/query"
    else:
        endpoint = f"{NOTION_API_URL}/v1/databases/{database_id}/query"

    response = await session.post(endpoint, json=payload or None)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


# Blocks endpoints


async def fetch_block(session: niquests.AsyncSession, block_id: str) -> Block:
    """Retrieve a block by ID."""
    response = await session.get(f"{NOTION_API_URL}/v1/blocks/{block_id}")
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


async def fetch_block_children(
    session: niquests.AsyncSession,
    block_id: str,
    *,
    start_cursor: str | None = None,
    page_size: int | None = None,
) -> BlockListResponse:
    """Retrieve a block's children."""
    params: dict[str, str] = {}
    if start_cursor:
        params["start_cursor"] = start_cursor
    if page_size:
        params["page_size"] = str(page_size)

    response = await session.get(f"{NOTION_API_URL}/v1/blocks/{block_id}/children", params=params or None)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


async def fetch_append_block_children(
    session: niquests.AsyncSession,
    block_id: str,
    children: list[dict[str, Any]],
) -> BlockListResponse:
    """Append children blocks to a parent block."""
    payload = {"children": children}

    response = await session.patch(f"{NOTION_API_URL}/v1/blocks/{block_id}/children", json=payload)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


# Search endpoint


def _convert_search_filter_for_api_version(filter: dict[str, Any], api_version: str) -> dict[str, Any]:
    """Convert search filter value between 'database' and 'data_source' based on API version."""
    if "value" not in filter:
        return filter

    filter_copy = filter.copy()
    if _use_data_source_api(api_version):
        # Convert 'database' to 'data_source' for new API
        if filter_copy.get("value") == "database":
            filter_copy["value"] = "data_source"
    else:
        # Convert 'data_source' to 'database' for old API
        if filter_copy.get("value") == "data_source":
            filter_copy["value"] = "database"
    return filter_copy


async def fetch_search(
    session: niquests.AsyncSession,
    *,
    query: str | None = None,
    filter: dict[str, Any] | None = None,
    sort: dict[str, Any] | None = None,
    start_cursor: str | None = None,
    page_size: int | None = None,
    api_version: ApiVersion | None = None,
) -> SearchResponse:
    """Search pages and databases/data sources.

    For API version 2025-09-03+, filter value 'database' is automatically
    converted to 'data_source'. For older versions, 'data_source' is
    converted to 'database'.
    """
    _api_version = api_version or session.headers.get("Notion-Version", DEFAULT_API_VERSION)
    payload: dict[str, Any] = {}
    if query:
        payload["query"] = query
    if filter:
        payload["filter"] = _convert_search_filter_for_api_version(filter, _api_version)
    if sort:
        payload["sort"] = sort
    if start_cursor:
        payload["start_cursor"] = start_cursor
    if page_size:
        payload["page_size"] = page_size

    response = await session.post(f"{NOTION_API_URL}/v1/search", json=payload or None)
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


if __name__ == "__main__":
    import asyncio

    async def main():
        async with niquests.AsyncSession() as session:
            session.headers.update(get_notion_headers())
            me = await fetch_me(session)
            print("Me:", me)
            # print(await fetch_search(session, filter="aaa"))

    asyncio.run(main())
