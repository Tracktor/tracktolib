---
title: "Notion"
---

# Notion

Notion API helpers using [niquests](https://github.com/jawah/niquests).

## Installation

```bash
uv add tracktolib[notion]
```

## Dependencies

- [niquests](https://github.com/jawah/niquests) - Modern HTTP client with HTTP/3 support

## Overview

This module provides utilities for interacting with the [Notion API](https://developers.notion.com/):

- Block creation helpers for building Notion content programmatically
- High-level utilities for exporting/importing pages as markdown
- Persistent caching for databases and page content

## Authentication

Set up a session with Notion headers:

```python
import niquests
from tracktolib.notion.fetch import get_notion_headers

# Using environment variable (NOTION_TOKEN)
headers = get_notion_headers()

# Or with explicit token
headers = get_notion_headers(token="secret_xxx")

async with niquests.AsyncSession() as session:
    session.headers.update(headers)
    # ... use session for API calls
```

## High-Level Utilities

### `export_markdown_to_page(...) -> ExportResult`

Export markdown content to a Notion database as a new page.

```python
result = await export_markdown_to_page(
    session,
    database_id="your-database-id",
    content="# My Document\n\nContent here...",
    title="Document Title",
    properties={"Tags": {"multi_select": [{"name": "docs"}]}},
    comments=["Initial review comment", "Another comment"],
)
print(f"Created {result['count']} blocks at {result['url']}")
```

**Comments handling**: The optional `comments` parameter accepts a list of strings. Each string is added as a page-level comment (not attached to any specific block) after the page is created.

### `download_page_to_markdown(...) -> int`

Download a Notion page to a local markdown file.

```python
block_count = await download_page_to_markdown(
    session,
    page_id="your-page-id",
    output_path="./output.md",
    include_comments=True,
    on_progress=lambda current, total: print(f"Fetched: {current}"),
)
```

**Comments handling**: When `include_comments=True`, the function fetches both:

- **Inline block comments**: Rendered as blockquotes immediately after their associated block
- **Page-level comments**: Appended at the end under a `## Comments` heading

Comments are formatted as:

```markdown
> 💬 **Author Name** - 2024-01-15 10:30: Comment text here
```

When re-uploading downloaded markdown with `update_page_content`, comment blockquotes (lines starting with `> 💬`) are automatically stripped to avoid converting them into regular quote blocks.

### `update_page_content(...) -> UpdateResult`

Update a page using smart prefix-preserving diff. Only deletes and recreates blocks that changed, preserving block IDs
and inline comments on unchanged content.

```python
result = await update_page_content(
    session,
    page_id="your-page-id",
    content="# Updated Content\n\nNew text here...",
)
print(f"Preserved: {result['preserved']}, Deleted: {result['deleted']}, Created: {result['created']}")
```

### `clear_page_blocks(...) -> ClearResult`

Delete all blocks from a Notion page.

```python
result = await clear_page_blocks(session, page_id="your-page-id")
print(f"Deleted {result['deleted']} blocks")
```

### `fetch_all_page_comments(...) -> list[PageComment]`

Fetch all comments from a page and its blocks.

```python
comments = await fetch_all_page_comments(
    session,
    page_id="your-page-id",
    concurrency=50,
)
for c in comments:
    print(f"{c['author_name']}: {c['text']}")
```

## Caching

### `NotionCache`

Persistent cache for Notion data. Use as a context manager to auto-load on entry and save on exit:

```python
from tracktolib.notion import NotionCache

with NotionCache() as cache:
    # Databases
    db = cache.get_database("db-id")  # Returns CachedDatabase | None
    cache.set_database(database_response)
    cache.delete_database("db-id")

    # Page blocks
    blocks = cache.get_page_blocks("page-id")
    cache.set_page_blocks("page-id", blocks)
    cache.delete_page_blocks("page-id")

    # Page comments
    comments = cache.get_page_comments("page-id")
    cache.set_page_comments("page-id", comments)
    cache.delete_page_comments("page-id")

    # Clear all
    cache.clear()
# Automatically saved on exit
```

Default cache location: `~/.cache/tracktolib/notion/cache.json`

Custom location:

```python
from pathlib import Path

cache = NotionCache(cache_dir=Path("/custom/cache/dir"))
```

### `CachedDatabase`

TypedDict with cached database info: `id`, `title`, `properties`, `cached_at`.

## Concurrency

High-level functions accept an optional `semaphore` parameter for rate limiting (default: 50 concurrent requests):

```python
import asyncio

semaphore = asyncio.Semaphore(10)
await clear_page_blocks(session, page_id, semaphore=semaphore)
```
