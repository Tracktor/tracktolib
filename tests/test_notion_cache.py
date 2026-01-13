import pytest
from pathlib import Path


@pytest.fixture
def temp_cache_dir(tmp_path: Path) -> Path:
    return tmp_path / "notion_cache"


@pytest.fixture
def cache(temp_cache_dir: Path):
    from tracktolib.notion.cache import NotionCache

    cache = NotionCache(cache_dir=temp_cache_dir)
    cache.load()
    return cache


@pytest.fixture
def sample_database() -> dict:
    return {
        "id": "db-123",
        "title": [{"plain_text": "Test Database"}],
        "properties": {
            "Name": {"id": "title", "type": "title", "title": {}},
            "Status": {"id": "xyz", "type": "select", "select": {"options": []}},
        },
    }


def test_set_and_get_database(cache, sample_database):
    cached = cache.set_database(sample_database)

    assert cached["id"] == "db-123"
    assert cached["title"] == "Test Database"
    assert cached["properties"] == sample_database["properties"]
    assert "cached_at" in cached

    retrieved = cache.get_database("db-123")
    assert retrieved == cached


def test_get_database_not_found(cache):
    result = cache.get_database("non-existent")
    assert result is None


def test_get_databases_empty(cache):
    result = cache.get_databases()
    assert result == {}


def test_get_databases_with_data(cache, sample_database):
    cache.set_database(sample_database)

    db2 = {
        "id": "db-456",
        "title": [{"plain_text": "Another DB"}],
        "properties": {},
    }
    cache.set_database(db2)

    all_dbs = cache.get_databases()
    assert len(all_dbs) == 2
    assert "db-123" in all_dbs
    assert "db-456" in all_dbs


def test_delete_database(cache, sample_database):
    cache.set_database(sample_database)
    assert cache.get_database("db-123") is not None

    cache.delete_database("db-123")
    assert cache.get_database("db-123") is None


def test_delete_database_not_found(cache):
    cache.delete_database("non-existent")


def test_clear(cache, sample_database):
    cache.set_database(sample_database)
    assert cache.get_database("db-123") is not None

    cache.clear()
    assert cache.get_database("db-123") is None


def test_database_with_empty_title(cache):
    db = {
        "id": "db-empty",
        "title": [],
        "properties": {},
    }
    cached = cache.set_database(db)
    assert cached["title"] == ""


def test_persistence_across_instances(temp_cache_dir, sample_database):
    from tracktolib.notion.cache import NotionCache

    with NotionCache(cache_dir=temp_cache_dir) as cache1:
        cache1.set_database(sample_database)

    with NotionCache(cache_dir=temp_cache_dir) as cache2:
        retrieved = cache2.get_database("db-123")

        assert retrieved is not None
        assert retrieved["id"] == "db-123"
        assert retrieved["title"] == "Test Database"


# Page blocks caching tests


@pytest.fixture
def sample_blocks() -> list:
    return [
        {
            "object": "block",
            "id": "block-1",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Hello"}}]},
        },
        {
            "object": "block",
            "id": "block-2",
            "type": "heading_1",
            "heading_1": {"rich_text": [{"type": "text", "text": {"content": "Title"}}]},
        },
    ]


def test_set_and_get_page_blocks(cache, sample_blocks):
    cached = cache.set_page_blocks("page-123", sample_blocks)

    assert cached["page_id"] == "page-123"
    assert cached["blocks"] == sample_blocks
    assert "cached_at" in cached

    retrieved = cache.get_page_blocks("page-123")
    assert retrieved == sample_blocks


def test_get_page_blocks_not_found(cache):
    result = cache.get_page_blocks("non-existent")
    assert result is None


def test_delete_page_blocks(cache, sample_blocks):
    cache.set_page_blocks("page-123", sample_blocks)
    assert cache.get_page_blocks("page-123") is not None

    cache.delete_page_blocks("page-123")
    assert cache.get_page_blocks("page-123") is None


def test_delete_page_blocks_not_found(cache):
    cache.delete_page_blocks("non-existent")


def test_page_blocks_persistence(temp_cache_dir, sample_blocks):
    from tracktolib.notion.cache import NotionCache

    with NotionCache(cache_dir=temp_cache_dir) as cache1:
        cache1.set_page_blocks("page-123", sample_blocks)

    with NotionCache(cache_dir=temp_cache_dir) as cache2:
        retrieved = cache2.get_page_blocks("page-123")

        assert retrieved is not None
        assert len(retrieved) == 2
        assert retrieved[0]["id"] == "block-1"


def test_clear_removes_page_blocks(cache, sample_blocks):
    cache.set_page_blocks("page-123", sample_blocks)
    assert cache.get_page_blocks("page-123") is not None

    cache.clear()
    assert cache.get_page_blocks("page-123") is None
