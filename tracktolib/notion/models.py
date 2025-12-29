"""Notion API models based on official notion-sdk-js types."""

from typing import Any, Literal, NotRequired, TypedDict

# Base types


class PartialUser(TypedDict):
    """Partial user object."""

    id: str
    object: Literal["user"]


class PersonDetails(TypedDict):
    """Person details."""

    email: NotRequired[str]


class PersonUserObjectResponse(TypedDict):
    """Person user type details."""

    type: Literal["person"]
    person: PersonDetails


class BotUserObjectResponse(TypedDict):
    """Bot user type details."""

    type: Literal["bot"]
    bot: dict[str, Any]


class UserObjectResponseCommon(TypedDict):
    """Common user object fields."""

    id: str
    object: Literal["user"]
    name: str | None
    avatar_url: str | None


# User object is common fields + either person or bot
class User(UserObjectResponseCommon):
    """Full user object."""

    type: Literal["person", "bot"]
    person: NotRequired[PersonDetails]
    bot: NotRequired[dict[str, Any]]


# OAuth types


class UserOwner(TypedDict):
    """User owner for OAuth."""

    type: Literal["user"]
    user: User | PartialUser


class WorkspaceOwner(TypedDict):
    """Workspace owner for OAuth."""

    type: Literal["workspace"]
    workspace: Literal[True]


Owner = UserOwner | WorkspaceOwner


class TokenResponse(TypedDict):
    """Response from creating or refreshing an access token."""

    access_token: str
    token_type: Literal["bearer"]
    refresh_token: str | None
    bot_id: str
    workspace_icon: str | None
    workspace_name: str | None
    workspace_id: str
    owner: Owner
    duplicated_template_id: str | None
    request_id: NotRequired[str]


class IntrospectTokenResponse(TypedDict):
    """Response from introspecting a token."""

    active: bool
    scope: NotRequired[str]
    iat: NotRequired[int]
    request_id: NotRequired[str]


class RevokeTokenResponse(TypedDict):
    """Response from revoking a token."""

    request_id: NotRequired[str]


# Rich text types


class RichTextItemResponse(TypedDict):
    """Rich text item."""

    type: str
    plain_text: str
    href: str | None
    annotations: dict[str, Any]
    text: NotRequired[dict[str, Any]]
    mention: NotRequired[dict[str, Any]]
    equation: NotRequired[dict[str, Any]]


# Parent types


class PageParent(TypedDict):
    """Page parent."""

    type: Literal["page_id"]
    page_id: str


class DatabaseParent(TypedDict):
    """Database parent (deprecated in API 2025-09-03, use DataSourceParent)."""

    type: Literal["database_id"]
    database_id: str


class DataSourceParent(TypedDict):
    """Data source parent (API 2025-09-03+)."""

    type: Literal["data_source_id"]
    data_source_id: str


class WorkspaceParent(TypedDict):
    """Workspace parent."""

    type: Literal["workspace"]
    workspace: Literal[True]


class BlockParent(TypedDict):
    """Block parent."""

    type: Literal["block_id"]
    block_id: str


Parent = PageParent | DatabaseParent | DataSourceParent | WorkspaceParent | BlockParent


# Page types


class Page(TypedDict):
    """Page object response."""

    object: Literal["page"]
    id: str
    created_time: str
    last_edited_time: str
    created_by: PartialUser
    last_edited_by: PartialUser
    archived: bool
    in_trash: bool
    is_locked: bool
    url: str
    public_url: str | None
    parent: Parent
    properties: dict[str, Any]
    icon: dict[str, Any] | None
    cover: dict[str, Any] | None


class PartialPage(TypedDict):
    """Partial page object."""

    object: Literal["page"]
    id: str


# Database types


class Database(TypedDict):
    """Database object response."""

    object: Literal["database"]
    id: str
    title: list[RichTextItemResponse]
    description: list[RichTextItemResponse]
    parent: Parent
    is_inline: bool
    in_trash: bool
    is_locked: bool
    created_time: str
    last_edited_time: str
    icon: dict[str, Any] | None
    cover: dict[str, Any] | None
    properties: dict[str, Any]
    url: str
    public_url: str | None
    archived: bool
    created_by: PartialUser
    last_edited_by: PartialUser


class PartialDatabase(TypedDict):
    """Partial database object."""

    object: Literal["database"]
    id: str


# Block types


class Block(TypedDict):
    """Block object response."""

    object: Literal["block"]
    id: str
    parent: Parent
    type: str
    created_time: str
    last_edited_time: str
    created_by: PartialUser
    last_edited_by: PartialUser
    has_children: bool
    archived: bool
    in_trash: bool


class PartialBlock(TypedDict):
    """Partial block object."""

    object: Literal["block"]
    id: str


# List response types


class UserListResponse(TypedDict):
    """Response from listing users."""

    object: Literal["list"]
    type: Literal["user"]
    results: list[User]
    next_cursor: str | None
    has_more: bool


class PageListResponse(TypedDict):
    """Response from querying a database."""

    object: Literal["list"]
    type: Literal["page_or_data_source"]
    results: list[Page | PartialPage]
    next_cursor: str | None
    has_more: bool


class BlockListResponse(TypedDict):
    """Response from listing block children."""

    object: Literal["list"]
    type: Literal["block"]
    results: list[Block | PartialBlock]
    next_cursor: str | None
    has_more: bool


class SearchResponse(TypedDict):
    """Response from search."""

    object: Literal["list"]
    type: Literal["page_or_data_source"]
    results: list[Page | PartialPage | Database | PartialDatabase]
    next_cursor: str | None
    has_more: bool
