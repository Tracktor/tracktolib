import os
from typing import Any

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


def _check_resp(response: niquests.Response) -> None:
    """Check response status and include JSON body in error if available."""
    try:
        response.raise_for_status()
    except niquests.HTTPError as e:
        # Try to parse JSON body for more info
        if response.reason is None:
            try:
                response.reason = response.json()
            except niquests.JSONDecodeError:
                pass
        raise e


def _get_notion_token() -> str:
    """Get Notion token from config or environment."""
    token = os.environ.get("NOTION_TOKEN")
    if not token:
        raise ValueError("Notion token not found. Set NOTION_TOKEN env var.")
    return token


def get_notion_headers(api_version: str = "2025-09-03", token: str | None = None) -> dict[str, str]:
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
    _check_resp(response)
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
    _check_resp(response)
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
    _check_resp(response)
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
    _check_resp(response)
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
    _check_resp(response)
    return response.json()  # type: ignore[return-value]


async def fetch_user(session: niquests.AsyncSession, user_id: str) -> User:
    """Retrieve a user by ID."""
    response = await session.get(f"{NOTION_API_URL}/v1/users/{user_id}")
    _check_resp(response)
    return response.json()  # type: ignore[return-value]


async def fetch_me(session: niquests.AsyncSession) -> User:
    """Rfetrieve the bot user associated with the token."""
    response = await session.get(f"{NOTION_API_URL}/v1/users/me")
    _check_resp(response)
    return response.json()  # type: ignore[return-value]


# Pages endpoints


async def fetch_page(session: niquests.AsyncSession, page_id: str) -> Page:
    """Retrieve a page by ID."""
    response = await session.get(f"{NOTION_API_URL}/v1/pages/{page_id}")
    _check_resp(response)
    return response.json()  # type: ignore[return-value]


async def create_page(
    session: niquests.AsyncSession,
    *,
    parent: dict[str, Any],
    properties: dict[str, Any],
    children: list[dict[str, Any]] | None = None,
    icon: dict[str, Any] | None = None,
    cover: dict[str, Any] | None = None,
) -> Page:
    """Create a new page."""
    payload: dict[str, Any] = {
        "parent": parent,
        "properties": properties,
    }
    if children:
        payload["children"] = children
    if icon:
        payload["icon"] = icon
    if cover:
        payload["cover"] = cover

    response = await session.post(f"{NOTION_API_URL}/v1/pages", json=payload)
    _check_resp(response)
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
    _check_resp(response)
    return response.json()  # type: ignore[return-value]


# Databases endpoints


async def fetch_database(session: niquests.AsyncSession, database_id: str) -> Database:
    """Retrieve a database by ID."""
    response = await session.get(f"{NOTION_API_URL}/v1/databases/{database_id}")
    _check_resp(response)
    return response.json()  # type: ignore[return-value]


async def query_database(
    session: niquests.AsyncSession,
    database_id: str,
    *,
    filter: dict[str, Any] | None = None,
    sorts: list[dict[str, Any]] | None = None,
    start_cursor: str | None = None,
    page_size: int | None = None,
) -> PageListResponse:
    """Query a database."""
    payload: dict[str, Any] = {}
    if filter:
        payload["filter"] = filter
    if sorts:
        payload["sorts"] = sorts
    if start_cursor:
        payload["start_cursor"] = start_cursor
    if page_size:
        payload["page_size"] = page_size

    response = await session.post(f"{NOTION_API_URL}/v1/databases/{database_id}/query", json=payload or None)
    _check_resp(response)
    return response.json()  # type: ignore[return-value]


# Blocks endpoints


async def fetch_block(session: niquests.AsyncSession, block_id: str) -> Block:
    """Retrieve a block by ID."""
    response = await session.get(f"{NOTION_API_URL}/v1/blocks/{block_id}")
    _check_resp(response)
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
    _check_resp(response)
    return response.json()  # type: ignore[return-value]


async def fetch_append_block_children(
    session: niquests.AsyncSession,
    block_id: str,
    children: list[dict[str, Any]],
) -> BlockListResponse:
    """Append children blocks to a parent block."""
    payload = {"children": children}

    response = await session.patch(f"{NOTION_API_URL}/v1/blocks/{block_id}/children", json=payload)
    _check_resp(response)
    return response.json()  # type: ignore[return-value]


# Search endpoint


async def fetch_search(
    session: niquests.AsyncSession,
    *,
    query: str | None = None,
    filter: dict[str, Any] | None = None,
    sort: dict[str, Any] | None = None,
    start_cursor: str | None = None,
    page_size: int | None = None,
) -> SearchResponse:
    """Search pages and databases."""
    payload: dict[str, Any] = {}
    if query:
        payload["query"] = query
    if filter:
        payload["filter"] = filter
    if sort:
        payload["sort"] = sort
    if start_cursor:
        payload["start_cursor"] = start_cursor
    if page_size:
        payload["page_size"] = page_size

    response = await session.post(f"{NOTION_API_URL}/v1/search", json=payload or None)
    _check_resp(response)
    return response.json()  # type: ignore[return-value]
