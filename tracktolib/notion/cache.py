from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, TypedDict, cast

if TYPE_CHECKING:
    from .utils import PageComment


class CachedDatabase(TypedDict):
    id: str
    title: str
    properties: dict[str, Any]
    cached_at: str


class CachedPageBlocks(TypedDict):
    """Cached blocks for a page."""

    page_id: str
    blocks: list[dict[str, Any]]
    cached_at: str


class CachedPageComments(TypedDict):
    """Cached comments for a page."""

    page_id: str
    comments: list[dict[str, Any]]
    cached_at: str


class CacheData(TypedDict, total=False):
    databases: dict[str, CachedDatabase]
    page_blocks: dict[str, CachedPageBlocks]
    page_comments: dict[str, CachedPageComments]


def _default_cache_dir() -> Path:
    """Default cache directory: $XDG_CACHE_HOME/tracktolib/notion or ~/.cache/tracktolib/notion."""
    xdg_cache = os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache"))
    return Path(xdg_cache) / "tracktolib" / "notion"


@dataclass
class NotionCache:
    """Persistent cache for Notion data.

    Use as a context manager to load on entry and save on exit:

        with NotionCache() as cache:
            db = cache.get_database("db-id")
            cache.set_database({"id": "new-db", ...})
        # Automatically saved on exit
    """

    # Directory for cache file.
    cache_dir: Path = field(default_factory=_default_cache_dir)
    _file_path: Path = field(init=False)
    _data: CacheData = field(init=False)
    _dirty: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self._file_path = self.cache_dir / "cache.json"
        self._data = {}

    def load(self) -> None:
        """Load cache from disk into memory."""
        if self._file_path.exists():
            self._data = json.loads(self._file_path.read_text())
        else:
            self._data = {}
        self._dirty = False

    def save(self) -> None:
        """Save in-memory cache to disk (only if modified)."""
        if not self._dirty:
            return
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._file_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, default=str))
        tmp.rename(self._file_path)
        self._dirty = False

    def __enter__(self) -> Self:
        self.load()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.save()

    def get_database(self, database_id: str) -> CachedDatabase | None:
        """Get a cached database by ID."""
        return self._data.get("databases", {}).get(database_id)

    def get_databases(self) -> dict[str, CachedDatabase]:
        """Get all cached databases."""
        return self._data.get("databases", {})

    def set_database(self, database: Mapping[str, Any]) -> CachedDatabase:
        """Cache a database from Notion API response."""
        title_prop = database.get("title", [])
        title = next(
            (el["plain_text"] for el in title_prop if isinstance(el, Mapping) and "plain_text" in el),
            "",
        )

        entry: CachedDatabase = {
            "id": database["id"],
            "title": title,
            "properties": database["properties"],
            "cached_at": datetime.now().isoformat(),
        }

        if "databases" not in self._data:
            self._data["databases"] = {}
        self._data["databases"][database["id"]] = entry
        self._dirty = True
        return entry

    def delete_database(self, database_id: str) -> None:
        """Remove a database from cache."""
        if "databases" in self._data:
            self._data["databases"].pop(database_id, None)
            self._dirty = True

    def clear(self) -> None:
        """Clear all cached data."""
        self._data = {}
        self._dirty = True

    def get_page_blocks(self, page_id: str) -> list[dict[str, Any]] | None:
        """Get cached blocks for the given page_id, or None if not cached."""
        cached = self._data.get("page_blocks", {}).get(page_id)
        if cached:
            return cached["blocks"]
        return None

    def set_page_blocks(
        self,
        page_id: str,
        blocks: Sequence[dict[str, Any]],
    ) -> CachedPageBlocks:
        """Cache the given blocks for page_id and return the cached entry."""
        entry: CachedPageBlocks = {
            "page_id": page_id,
            "blocks": list(blocks),
            "cached_at": datetime.now().isoformat(),
        }

        if "page_blocks" not in self._data:
            self._data["page_blocks"] = {}
        self._data["page_blocks"][page_id] = entry
        self._dirty = True
        return entry

    def delete_page_blocks(self, page_id: str) -> None:
        """Remove cached blocks for the given page_id."""
        if "page_blocks" in self._data:
            self._data["page_blocks"].pop(page_id, None)
            self._dirty = True

    def get_page_comments(self, page_id: str) -> list[PageComment] | None:
        """Get cached comments for the given page_id, or None if not cached."""
        cached = self._data.get("page_comments", {}).get(page_id)
        if cached:
            return cached["comments"]  # type: ignore[return-value]
        return None

    def set_page_comments(
        self,
        page_id: str,
        comments: Sequence[PageComment],
    ) -> CachedPageComments:
        """Cache the given comments for page_id and return the cached entry."""
        entry: CachedPageComments = {
            "page_id": page_id,
            "comments": cast(list[dict[str, Any]], list(comments)),
            "cached_at": datetime.now().isoformat(),
        }

        if "page_comments" not in self._data:
            self._data["page_comments"] = {}
        self._data["page_comments"][page_id] = entry
        self._dirty = True
        return entry

    def delete_page_comments(self, page_id: str) -> None:
        """Remove cached comments for the given page_id."""
        if "page_comments" in self._data:
            self._data["page_comments"].pop(page_id, None)
            self._dirty = True
