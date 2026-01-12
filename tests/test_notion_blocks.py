from tracktolib.notion.blocks import (
    blocks_content_equal,
    find_divergence_index,
    make_paragraph_block,
    make_heading_block,
    make_code_block,
    make_divider_block,
    make_todo_block,
)


class TestBlocksContentEqual:
    def test_equal_paragraphs(self):
        existing = {
            "object": "block",
            "id": "block-123",
            "type": "paragraph",
            "created_time": "2024-01-01T00:00:00.000Z",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Hello world"}}]},
        }
        new = make_paragraph_block("Hello world")

        assert blocks_content_equal(existing, new) is True

    def test_different_paragraphs(self):
        existing = {
            "object": "block",
            "id": "block-123",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Hello world"}}]},
        }
        new = make_paragraph_block("Goodbye world")

        assert blocks_content_equal(existing, new) is False

    def test_equal_headings(self):
        existing = {
            "object": "block",
            "id": "block-456",
            "type": "heading_1",
            "heading_1": {"rich_text": [{"type": "text", "text": {"content": "Title"}}]},
        }
        new = make_heading_block("Title", 1)

        assert blocks_content_equal(existing, new) is True

    def test_different_heading_levels(self):
        existing = {
            "object": "block",
            "id": "block-456",
            "type": "heading_1",
            "heading_1": {"rich_text": [{"type": "text", "text": {"content": "Title"}}]},
        }
        new = make_heading_block("Title", 2)

        assert blocks_content_equal(existing, new) is False

    def test_equal_code_blocks(self):
        existing = {
            "object": "block",
            "id": "block-789",
            "type": "code",
            "code": {
                "rich_text": [{"type": "text", "text": {"content": "print('hello')"}}],
                "language": "python",
            },
        }
        new = make_code_block("print('hello')", "python")[0]

        assert blocks_content_equal(existing, new) is True

    def test_different_code_language(self):
        existing = {
            "object": "block",
            "type": "code",
            "code": {
                "rich_text": [{"type": "text", "text": {"content": "console.log('hello')"}}],
                "language": "javascript",
            },
        }
        new = make_code_block("console.log('hello')", "python")[0]

        assert blocks_content_equal(existing, new) is False

    def test_equal_dividers(self):
        existing = {
            "object": "block",
            "id": "divider-1",
            "type": "divider",
            "divider": {},
        }
        new = make_divider_block()

        assert blocks_content_equal(existing, new) is True

    def test_equal_todo_blocks(self):
        existing = {
            "object": "block",
            "type": "to_do",
            "to_do": {
                "rich_text": [{"type": "text", "text": {"content": "Task 1"}}],
                "checked": True,
            },
        }
        new = make_todo_block("Task 1", checked=True)

        assert blocks_content_equal(existing, new) is True

    def test_different_todo_checked_state(self):
        existing = {
            "object": "block",
            "type": "to_do",
            "to_do": {
                "rich_text": [{"type": "text", "text": {"content": "Task 1"}}],
                "checked": True,
            },
        }
        new = make_todo_block("Task 1", checked=False)

        assert blocks_content_equal(existing, new) is False

    def test_different_block_types(self):
        existing = {
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Text"}}]},
        }
        new = make_heading_block("Text", 1)

        assert blocks_content_equal(existing, new) is False


class TestFindDivergenceIndex:
    def test_all_blocks_same(self):
        existing = [
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "C"}}]},
            },
        ]
        new = [
            make_paragraph_block("A"),
            make_paragraph_block("B"),
            make_paragraph_block("C"),
        ]

        assert find_divergence_index(existing, new) == 3

    def test_last_block_different(self):
        existing = [
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "C"}}]},
            },
        ]
        new = [
            make_paragraph_block("A"),
            make_paragraph_block("B"),
            make_paragraph_block("D"),  # Different
        ]

        assert find_divergence_index(existing, new) == 2

    def test_first_block_different(self):
        existing = [
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]},
            },
        ]
        new = [
            make_paragraph_block("X"),  # Different
            make_paragraph_block("B"),
        ]

        assert find_divergence_index(existing, new) == 0

    def test_new_has_more_blocks(self):
        existing = [
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]},
            },
        ]
        new = [
            make_paragraph_block("A"),
            make_paragraph_block("B"),
            make_paragraph_block("C"),  # New block
        ]

        assert find_divergence_index(existing, new) == 2

    def test_existing_has_more_blocks(self):
        existing = [
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "C"}}]},
            },
        ]
        new = [
            make_paragraph_block("A"),
            make_paragraph_block("B"),
        ]

        assert find_divergence_index(existing, new) == 2

    def test_empty_existing(self):
        existing = []
        new = [make_paragraph_block("A")]

        assert find_divergence_index(existing, new) == 0

    def test_empty_new(self):
        existing = [
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]},
            }
        ]
        new = []

        assert find_divergence_index(existing, new) == 0

    def test_both_empty(self):
        assert find_divergence_index([], []) == 0

    def test_mixed_block_types(self):
        existing = [
            {
                "type": "heading_1",
                "heading_1": {"rich_text": [{"type": "text", "text": {"content": "Title"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Para"}}]},
            },
            {"type": "divider", "divider": {}},
        ]
        new = [
            make_heading_block("Title", 1),
            make_paragraph_block("Para"),
            make_divider_block(),
        ]

        assert find_divergence_index(existing, new) == 3

    def test_middle_block_different(self):
        existing = [
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]},
            },
            {
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "C"}}]},
            },
        ]
        new = [
            make_paragraph_block("A"),
            make_paragraph_block("CHANGED"),  # Different
            make_paragraph_block("C"),
        ]

        assert find_divergence_index(existing, new) == 1
