"""Notion utility functions for exporting and importing content."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator, Protocol, cast

import niquests

if TYPE_CHECKING:
    from .models import Block, Comment, PartialBlock

from .blocks import ExportResult, blocks_to_markdown_with_comments, comments_to_markdown, markdown_to_blocks
from .fetch import NOTION_API_URL, create_comment, create_page, fetch_block_children


@asynccontextmanager
async def multiplex_session(session: niquests.AsyncSession) -> AsyncIterator[niquests.AsyncSession]:
    """Create a multiplexed session that copies headers from the original session.

    Automatically calls gather() on exit to execute all queued requests in parallel.

    Args:
        session: The original session to copy headers from.

    Yields:
        A new multiplexed session with copied headers.
    """
    async with niquests.AsyncSession(multiplexed=True) as mux_session:
        mux_session.headers.update(session.headers)
        yield mux_session
        await mux_session.gather()


class ProgressCallback(Protocol):
    """Protocol for progress callback functions."""

    def __call__(self, fetched: int, has_more: bool) -> None:
        """Called after each batch of blocks is fetched.

        Args:
            fetched: Total number of blocks fetched so far
            has_more: Whether there are more blocks to fetch
        """
        ...


__all__ = [
    "ProgressCallback",
    "export_markdown_to_page",
    "download_page_to_markdown",
]


async def export_markdown_to_page(
    session: niquests.AsyncSession,
    *,
    database_id: str,
    content: str,
    title: str,
    properties: dict[str, Any] | None = None,
    comments: list[str] | None = None,
) -> ExportResult:
    """Export markdown content to a Notion database as a new page.

    Args:
        session: Authenticated niquests session with Notion headers
        database_id: ID of the Notion database to create the page in
        content: Markdown content to convert to Notion blocks
        title: Page title (Name property)
        properties: Additional page properties (optional)
        comments: List of comment strings to add to the page (optional)

    Returns:
        ExportResult with count of blocks created and page URL
    """
    if not content.strip():
        return {"count": 0, "url": None}

    children = markdown_to_blocks(content)

    # Build properties with title
    page_properties: dict[str, Any] = {
        "Name": {"title": [{"text": {"content": title}}]},
    }
    if properties:
        page_properties.update(properties)

    page = await create_page(
        session,
        parent={"database_id": database_id},
        properties=page_properties,
        children=children,
    )

    url = page.get("url") if page else None
    page_id = page.get("id") if page else None

    # Add comments if provided
    if comments and page_id:
        for comment_text in comments:
            await create_comment(
                session,
                parent={"page_id": page_id},
                rich_text=[{"type": "text", "text": {"content": comment_text}}],
            )

    return {"count": len(children), "url": url}


async def download_page_to_markdown(
    session: niquests.AsyncSession,
    page_id: str,
    output_path: str | Path,
    *,
    include_comments: bool = False,
    on_progress: ProgressCallback | None = None,
) -> int:
    """Download a Notion page to a local markdown file.

    Uses HTTP/2 multiplexing for parallel fetching of comments.

    Args:
        session: Authenticated niquests session with Notion headers
        page_id: ID of the Notion page to download
        output_path: Path to save the markdown file
        include_comments: Whether to include comments (both page-level and inline block comments)
        on_progress: Optional callback called after each batch of blocks is fetched.
            Receives (fetched_count, has_more) arguments.

    Returns:
        Number of blocks converted
    """
    # Fetch all blocks from the page
    all_blocks: list[Block | PartialBlock] = []
    cursor: str | None = None

    while True:
        response = await fetch_block_children(session, page_id, start_cursor=cursor)
        all_blocks.extend(response.get("results", []))

        has_more = response.get("has_more", False)
        if on_progress:
            on_progress(len(all_blocks), has_more)

        if not has_more:
            break
        cursor = response.get("next_cursor")

    # Fetch comments if requested
    block_comments: dict[str, list[Comment]] = {}
    page_comments: list[Comment] = []

    if include_comments:
        # Collect all block IDs to fetch comments for (including page itself)
        block_ids = [page_id] + [b.get("id") for b in all_blocks if b.get("id")]

        # Use multiplexed session for parallel comment fetching
        async with multiplex_session(session) as mux_session:
            # Queue all comment requests
            comment_responses: list[niquests.Response] = []
            for block_id in block_ids:
                resp = await mux_session.get(
                    f"{NOTION_API_URL}/v1/comments",
                    params={"block_id": block_id},
                )
                comment_responses.append(resp)

        # Process responses and collect unique user IDs
        user_ids: set[str] = set()
        block_id_to_comments: dict[str, list[Comment]] = {}

        for block_id, resp in zip(block_ids, comment_responses):
            resp.raise_for_status()
            data = resp.json()
            comments = data.get("results", [])
            if comments:
                block_id_to_comments[block_id] = comments
                for comment in comments:
                    user_id = comment.get("created_by", {}).get("id")
                    if user_id:
                        user_ids.add(user_id)

        # Fetch all user names in parallel
        user_cache: dict[str, str] = {}
        if user_ids:
            async with multiplex_session(session) as mux_session:
                # Queue all user requests
                user_responses: list[tuple[str, niquests.Response]] = []
                for user_id in user_ids:
                    resp = await mux_session.get(f"{NOTION_API_URL}/v1/users/{user_id}")
                    user_responses.append((user_id, resp))

            # Process user responses
            for user_id, resp in user_responses:
                resp.raise_for_status()
                user = resp.json()
                user_cache[user_id] = user.get("name") or user_id

        # Apply user names to comments
        for comments in block_id_to_comments.values():
            for comment in comments:
                created_by = cast(dict[str, Any], comment.get("created_by", {}))
                user_id = created_by.get("id")
                if user_id and user_id in user_cache:
                    created_by["name"] = user_cache[user_id]

        # Separate page comments from block comments
        page_comments = block_id_to_comments.pop(page_id, [])
        block_comments = block_id_to_comments

    # Convert blocks to markdown with inline comments
    markdown_content = blocks_to_markdown_with_comments(all_blocks, block_comments)

    # Append page-level comments at the end
    if page_comments:
        comments_md = comments_to_markdown(page_comments)
        markdown_content = f"{markdown_content}\n\n{comments_md}"

    # Write to file
    output = Path(output_path)
    output.write_text(markdown_content, encoding="utf-8")

    return len(all_blocks)
