import pytest
from pathlib import Path

from tracktolib.notion.cache import NotionCache


@pytest.fixture
def temp_cache_dir(tmp_path: Path) -> Path:
    return tmp_path / "notion_cache"


@pytest.fixture
def cache(temp_cache_dir: Path):
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


class TestDatabaseCache:
    def test_set_and_get(self, cache, sample_database):
        cached = cache.set_database(sample_database)

        assert cached["id"] == "db-123"
        assert cached["title"] == "Test Database"
        assert cached["properties"] == sample_database["properties"]
        assert "cached_at" in cached

        retrieved = cache.get_database("db-123")
        assert retrieved == cached

    def test_get_not_found(self, cache):
        assert cache.get_database("non-existent") is None

    def test_get_all_empty(self, cache):
        assert cache.get_databases() == {}

    def test_get_all_with_data(self, cache, sample_database):
        cache.set_database(sample_database)
        cache.set_database({"id": "db-456", "title": [{"plain_text": "Another DB"}], "properties": {}})

        all_dbs = cache.get_databases()
        assert len(all_dbs) == 2
        assert "db-123" in all_dbs
        assert "db-456" in all_dbs

    def test_delete(self, cache, sample_database):
        cache.set_database(sample_database)
        assert cache.get_database("db-123") is not None

        cache.delete_database("db-123")
        assert cache.get_database("db-123") is None

    def test_delete_not_found(self, cache):
        cache.delete_database("non-existent")  # should not raise

    @pytest.mark.parametrize(
        ("title_field", "expected_title"),
        [
            pytest.param([{"plain_text": "My Title"}], "My Title", id="normal-title"),
            pytest.param([], "", id="empty-title"),
            pytest.param([{"other": "field"}], "", id="missing-plain-text"),
        ],
    )
    def test_title_extraction(self, cache, title_field, expected_title):
        db = {"id": "db-test", "title": title_field, "properties": {}}
        cached = cache.set_database(db)
        assert cached["title"] == expected_title

    def test_persistence_across_instances(self, temp_cache_dir, sample_database):
        with NotionCache(cache_dir=temp_cache_dir) as cache1:
            cache1.set_database(sample_database)

        with NotionCache(cache_dir=temp_cache_dir) as cache2:
            retrieved = cache2.get_database("db-123")
            assert retrieved is not None
            assert retrieved["id"] == "db-123"
            assert retrieved["title"] == "Test Database"


class TestPageBlocksCache:
    def test_set_and_get(self, cache, sample_blocks):
        cached = cache.set_page_blocks("page-123", sample_blocks)

        assert cached["page_id"] == "page-123"
        assert cached["blocks"] == sample_blocks
        assert "cached_at" in cached

        retrieved = cache.get_page_blocks("page-123")
        assert retrieved == sample_blocks

    def test_get_not_found(self, cache):
        assert cache.get_page_blocks("non-existent") is None

    def test_delete(self, cache, sample_blocks):
        cache.set_page_blocks("page-123", sample_blocks)
        assert cache.get_page_blocks("page-123") is not None

        cache.delete_page_blocks("page-123")
        assert cache.get_page_blocks("page-123") is None

    def test_delete_not_found(self, cache):
        cache.delete_page_blocks("non-existent")  # should not raise

    def test_persistence_across_instances(self, temp_cache_dir, sample_blocks):
        with NotionCache(cache_dir=temp_cache_dir) as cache1:
            cache1.set_page_blocks("page-123", sample_blocks)

        with NotionCache(cache_dir=temp_cache_dir) as cache2:
            retrieved = cache2.get_page_blocks("page-123")
            assert retrieved is not None
            assert len(retrieved) == 2
            assert retrieved[0]["id"] == "block-1"


class TestCacheClear:
    @pytest.mark.parametrize(
        ("setup_method", "setup_args", "check_method", "check_key"),
        [
            pytest.param(
                "set_database", ({"id": "db-1", "title": [], "properties": {}},), "get_database", "db-1", id="database"
            ),
            pytest.param("set_page_blocks", ("page-1", [{"id": "b1"}]), "get_page_blocks", "page-1", id="page-blocks"),
        ],
    )
    def test_clear_removes_data(self, cache, setup_method, setup_args, check_method, check_key):
        getattr(cache, setup_method)(*setup_args)
        assert getattr(cache, check_method)(check_key) is not None

        cache.clear()
        assert getattr(cache, check_method)(check_key) is None
