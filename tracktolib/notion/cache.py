from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict
import json
import os


class CachedDatabase(TypedDict):
    id: str
    title: str
    properties: dict[str, Any]
    cached_at: str


class CacheData(TypedDict, total=False):
    databases: dict[str, CachedDatabase]


@dataclass
class NotionCache:
    """Persistent cache for Notion data."""

    cache_dir: Path | None = None
    _file_path: Path = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.cache_dir is None:
            xdg_cache = os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache"))
            self.cache_dir = Path(xdg_cache) / "tracktolib" / "notion"
        self._file_path = self.cache_dir / "cache.json"

    def _load(self) -> CacheData:
        if not self._file_path.exists():
            return {}
        return json.loads(self._file_path.read_text())

    def _save(self, data: CacheData) -> None:
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._file_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, default=str))
        tmp.rename(self._file_path)

    def get_database(self, database_id: str) -> CachedDatabase | None:
        """Get a cached database by ID."""
        data = self._load()
        return data.get("databases", {}).get(database_id)

    def get_databases(self) -> dict[str, CachedDatabase]:
        """Get all cached databases."""
        return self._load().get("databases", {})

    def set_database(self, database: Mapping[str, Any]) -> CachedDatabase:
        """Cache a database from Notion API response."""
        title_prop = database.get("title", [])
        title = title_prop[0]["plain_text"] if title_prop else ""

        entry: CachedDatabase = {
            "id": database["id"],
            "title": title,
            "properties": database["properties"],
            "cached_at": datetime.now().isoformat(),
        }

        data = self._load()
        if "databases" not in data:
            data["databases"] = {}
        data["databases"][database["id"]] = entry
        self._save(data)
        return entry

    def delete_database(self, database_id: str) -> None:
        """Remove a database from cache."""
        data = self._load()
        if "databases" in data:
            data["databases"].pop(database_id, None)
            self._save(data)

    def clear(self) -> None:
        """Clear all cached data."""
        self._file_path.unlink(missing_ok=True)
