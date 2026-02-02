import pytest

from tracktolib.notion.blocks import (
    blocks_content_equal,
    find_divergence_index,
    make_code_block,
    make_divider_block,
    make_heading_block,
    make_paragraph_block,
    make_todo_block,
)


class TestBlocksContentEqual:
    @pytest.mark.parametrize(
        ("existing", "new", "expected"),
        [
            pytest.param(
                {
                    "object": "block",
                    "id": "block-123",
                    "type": "paragraph",
                    "created_time": "2024-01-01T00:00:00.000Z",
                    "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Hello world"}}]},
                },
                make_paragraph_block("Hello world"),
                True,
                id="equal-paragraphs",
            ),
            pytest.param(
                {
                    "object": "block",
                    "id": "block-123",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Hello world"}}]},
                },
                make_paragraph_block("Goodbye world"),
                False,
                id="different-paragraphs",
            ),
            pytest.param(
                {
                    "object": "block",
                    "id": "block-456",
                    "type": "heading_1",
                    "heading_1": {"rich_text": [{"type": "text", "text": {"content": "Title"}}]},
                },
                make_heading_block("Title", 1),
                True,
                id="equal-headings",
            ),
            pytest.param(
                {
                    "object": "block",
                    "id": "block-456",
                    "type": "heading_1",
                    "heading_1": {"rich_text": [{"type": "text", "text": {"content": "Title"}}]},
                },
                make_heading_block("Title", 2),
                False,
                id="different-heading-levels",
            ),
            pytest.param(
                {
                    "object": "block",
                    "id": "block-789",
                    "type": "code",
                    "code": {
                        "rich_text": [{"type": "text", "text": {"content": "print('hello')"}}],
                        "language": "python",
                    },
                },
                make_code_block("print('hello')", "python")[0],
                True,
                id="equal-code-blocks",
            ),
            pytest.param(
                {
                    "object": "block",
                    "type": "code",
                    "code": {
                        "rich_text": [{"type": "text", "text": {"content": "console.log('hello')"}}],
                        "language": "javascript",
                    },
                },
                make_code_block("console.log('hello')", "python")[0],
                False,
                id="different-code-language",
            ),
            pytest.param(
                {
                    "object": "block",
                    "id": "divider-1",
                    "type": "divider",
                    "divider": {},
                },
                make_divider_block(),
                True,
                id="equal-dividers",
            ),
            pytest.param(
                {
                    "object": "block",
                    "type": "to_do",
                    "to_do": {
                        "rich_text": [{"type": "text", "text": {"content": "Task 1"}}],
                        "checked": True,
                    },
                },
                make_todo_block("Task 1", checked=True),
                True,
                id="equal-todo-blocks",
            ),
            pytest.param(
                {
                    "object": "block",
                    "type": "to_do",
                    "to_do": {
                        "rich_text": [{"type": "text", "text": {"content": "Task 1"}}],
                        "checked": True,
                    },
                },
                make_todo_block("Task 1", checked=False),
                False,
                id="different-todo-checked-state",
            ),
            pytest.param(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Text"}}]},
                },
                make_heading_block("Text", 1),
                False,
                id="different-block-types",
            ),
        ],
    )
    def test_blocks_content_equal(self, existing, new, expected):
        assert blocks_content_equal(existing, new) is expected


class TestFindDivergenceIndex:
    @pytest.mark.parametrize(
        ("existing", "new", "expected"),
        [
            pytest.param(
                [
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "C"}}]}},
                ],
                [make_paragraph_block("A"), make_paragraph_block("B"), make_paragraph_block("C")],
                3,
                id="all-blocks-same",
            ),
            pytest.param(
                [
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "C"}}]}},
                ],
                [make_paragraph_block("A"), make_paragraph_block("B"), make_paragraph_block("D")],
                2,
                id="last-block-different",
            ),
            pytest.param(
                [
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]}},
                ],
                [make_paragraph_block("X"), make_paragraph_block("B")],
                0,
                id="first-block-different",
            ),
            pytest.param(
                [
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]}},
                ],
                [make_paragraph_block("A"), make_paragraph_block("B"), make_paragraph_block("C")],
                2,
                id="new-has-more-blocks",
            ),
            pytest.param(
                [
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "C"}}]}},
                ],
                [make_paragraph_block("A"), make_paragraph_block("B")],
                2,
                id="existing-has-more-blocks",
            ),
            pytest.param(
                [],
                [make_paragraph_block("A")],
                0,
                id="empty-existing",
            ),
            pytest.param(
                [{"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]}}],
                [],
                0,
                id="empty-new",
            ),
            pytest.param(
                [],
                [],
                0,
                id="both-empty",
            ),
            pytest.param(
                [
                    {"type": "heading_1", "heading_1": {"rich_text": [{"type": "text", "text": {"content": "Title"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Para"}}]}},
                    {"type": "divider", "divider": {}},
                ],
                [make_heading_block("Title", 1), make_paragraph_block("Para"), make_divider_block()],
                3,
                id="mixed-block-types",
            ),
            pytest.param(
                [
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "A"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "B"}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "C"}}]}},
                ],
                [make_paragraph_block("A"), make_paragraph_block("CHANGED"), make_paragraph_block("C")],
                1,
                id="middle-block-different",
            ),
        ],
    )
    def test_find_divergence_index(self, existing, new, expected):
        assert find_divergence_index(existing, new) == expected
