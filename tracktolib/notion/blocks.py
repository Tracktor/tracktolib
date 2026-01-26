"""Notion block creation utilities."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Sequence, TypedDict

if TYPE_CHECKING:
    from .models import Block, PartialBlock

__all__ = [
    # Types
    "ParagraphBlock",
    "HeadingBlock",
    "CodeBlock",
    "BulletedListBlock",
    "NumberedListBlock",
    "TodoBlock",
    "DividerBlock",
    "QuoteBlock",
    "NotionBlock",
    "ExportResult",
    # Rich text parsing
    "parse_rich_text",
    # Block creators
    "make_paragraph_block",
    "make_heading_block",
    "make_code_block",
    "make_bulleted_list_block",
    "make_numbered_list_block",
    "make_todo_block",
    "make_divider_block",
    "make_quote_block",
    # Block comparison
    "blocks_content_equal",
    "find_divergence_index",
]


class ExportResult(TypedDict):
    """Result of exporting markdown to Notion."""

    count: int
    """Number of blocks created in the page."""
    url: str | None
    """URL of the created Notion page, or None if creation failed."""


class _TextContent(TypedDict):
    content: str


class _Annotations(TypedDict, total=False):
    bold: bool
    italic: bool
    code: bool
    strikethrough: bool
    underline: bool
    color: str


class _TextItem(TypedDict, total=False):
    type: str
    text: _TextContent
    annotations: _Annotations


class _RichTextContent(TypedDict):
    rich_text: list[_TextItem]


class _CodeContent(TypedDict):
    rich_text: list[_TextItem]
    language: str


class _TodoContent(TypedDict):
    rich_text: list[_TextItem]
    checked: bool


class _DividerContent(TypedDict):
    pass


class ParagraphBlock(TypedDict):
    """Notion paragraph block structure."""

    object: str
    type: str
    paragraph: _RichTextContent


class HeadingBlock(TypedDict):
    """Notion heading block structure."""

    object: str
    type: str
    heading_1: _RichTextContent
    heading_2: _RichTextContent
    heading_3: _RichTextContent


class CodeBlock(TypedDict):
    """Notion code block structure."""

    object: str
    type: str
    code: _CodeContent


class BulletedListBlock(TypedDict):
    """Notion bulleted list item block structure."""

    object: str
    type: str
    bulleted_list_item: _RichTextContent


class NumberedListBlock(TypedDict):
    """Notion numbered list item block structure."""

    object: str
    type: str
    numbered_list_item: _RichTextContent


class TodoBlock(TypedDict):
    """Notion to_do block structure."""

    object: str
    type: str
    to_do: _TodoContent


class DividerBlock(TypedDict):
    """Notion divider block structure."""

    object: str
    type: str
    divider: _DividerContent


class QuoteBlock(TypedDict):
    """Notion quote block structure."""

    object: str
    type: str
    quote: _RichTextContent


# Union type for all block types
NotionBlock = (
    ParagraphBlock
    | HeadingBlock
    | CodeBlock
    | BulletedListBlock
    | NumberedListBlock
    | TodoBlock
    | DividerBlock
    | QuoteBlock
)

# Language aliases for code blocks
LANGUAGE_ALIASES = {
    "js": "javascript",
    "ts": "typescript",
    "py": "python",
    "rb": "ruby",
    "sh": "shell",
    "bash": "shell",
    "zsh": "shell",
    "yml": "yaml",
    "": "plain text",
}

# Pattern to match bold, code, or italic (in order of priority)
# Note: underscore italics use lookahead/lookbehind to avoid matching underscores
# inside identifiers like my_var_name
_INLINE_FORMAT_PATTERN = re.compile(
    r"(\*\*(.+?)\*\*|__(.+?)__|`([^`]+)`|\*([^*]+)\*|(?<![a-zA-Z0-9])_([^_]+)_(?![a-zA-Z0-9]))"
)


def parse_rich_text(text: str) -> list[_TextItem]:
    """Parse markdown inline formatting to Notion rich_text array.

    Handles:
    - **bold** or __bold__
    - `inline code`
    - *italic* or _italic_
    """
    rich_text: list[_TextItem] = []
    pos = 0
    for match in _INLINE_FORMAT_PATTERN.finditer(text):
        # Add plain text before the match
        if match.start() > pos:
            plain = text[pos : match.start()]
            if plain:
                rich_text.append({"type": "text", "text": {"content": plain}})

        full_match = match.group(0)
        if full_match.startswith("**") or full_match.startswith("__"):
            # Bold
            content = match.group(2) or match.group(3)
            rich_text.append(
                {
                    "type": "text",
                    "text": {"content": content},
                    "annotations": {"bold": True},
                }
            )
        elif full_match.startswith("`"):
            # Inline code
            content = match.group(4)
            rich_text.append(
                {
                    "type": "text",
                    "text": {"content": content},
                    "annotations": {"code": True},
                }
            )
        else:
            # Italic
            content = match.group(5) or match.group(6)
            rich_text.append(
                {
                    "type": "text",
                    "text": {"content": content},
                    "annotations": {"italic": True},
                }
            )

        pos = match.end()

    # Add remaining plain text
    if pos < len(text):
        remaining = text[pos:]
        if remaining:
            rich_text.append({"type": "text", "text": {"content": remaining}})

    # If no formatting found, return plain text
    if not rich_text:
        rich_text.append({"type": "text", "text": {"content": text}})

    return rich_text


def make_paragraph_block(text: str) -> ParagraphBlock:
    """Create a Notion paragraph block with rich text formatting.

    Args:
        text: The paragraph text (max 2000 characters)

    Raises:
        ValueError: If text exceeds 2000 characters (Notion's limit)
    """
    if len(text) > 2000:
        raise ValueError(f"Text exceeds Notion limit of 2000 characters ({len(text)} chars). Pre-chunk the text.")
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": parse_rich_text(text),
        },
    }


def make_heading_block(text: str, level: int) -> dict[str, Any]:
    """Create a Notion heading block (h1, h2, h3).

    Args:
        text: The heading text
        level: Heading level (1-6). Levels 4-6 are mapped to h3.
    """
    # Notion only supports h1, h2, h3 - map others to h3
    heading_type = f"heading_{min(level, 3)}"
    return {
        "object": "block",
        "type": heading_type,
        heading_type: {
            "rich_text": parse_rich_text(text),
        },
    }


def make_code_block(code: str, language: str = "plain text", *, chunk_size: int = 2000) -> list[dict[str, Any]]:
    """Create Notion code block(s).

    If code exceeds chunk_size characters, it is split into multiple blocks
    to preserve the full content.

    Args:
        code: The code content
        language: Programming language (supports aliases like 'py', 'js', 'ts')
        chunk_size: Maximum characters per block (default 2000, Notion's limit)

    Returns:
        List of code block dicts (usually one, multiple if code > chunk_size chars)
    """
    notion_lang = LANGUAGE_ALIASES.get(language.lower(), language.lower())

    blocks: list[dict[str, Any]] = []
    for i in range(0, len(code), chunk_size):
        chunk = code[i : i + chunk_size]
        blocks.append(
            {
                "object": "block",
                "type": "code",
                "code": {
                    "rich_text": [{"type": "text", "text": {"content": chunk}}],
                    "language": notion_lang,
                },
            }
        )

    return blocks


def make_bulleted_list_block(text: str) -> BulletedListBlock:
    """Create a Notion bulleted list item block."""
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {
            "rich_text": parse_rich_text(text),
        },
    }


def make_numbered_list_block(text: str) -> NumberedListBlock:
    """Create a Notion numbered list item block."""
    return {
        "object": "block",
        "type": "numbered_list_item",
        "numbered_list_item": {
            "rich_text": parse_rich_text(text),
        },
    }


def make_todo_block(text: str, checked: bool = False) -> TodoBlock:
    """Create a Notion to_do block (checkbox item)."""
    return {
        "object": "block",
        "type": "to_do",
        "to_do": {
            "rich_text": parse_rich_text(text),
            "checked": checked,
        },
    }


def make_divider_block() -> DividerBlock:
    """Create a Notion divider block (horizontal rule)."""
    return {
        "object": "block",
        "type": "divider",
        "divider": {},
    }


def make_quote_block(text: str) -> QuoteBlock:
    """Create a Notion quote block with rich text formatting."""
    return {
        "object": "block",
        "type": "quote",
        "quote": {
            "rich_text": parse_rich_text(text),
        },
    }


def _extract_block_content(block: Block | PartialBlock | NotionBlock | dict[str, Any]) -> dict[str, Any]:
    """Extract only the content-relevant parts of a block for comparison.

    Ignores metadata like id, created_time, last_edited_time, etc.
    """
    from .markdown import rich_text_to_markdown

    block_type = block.get("type")
    if not block_type:
        return {}

    block_data = block.get(block_type, {})

    if block_type == "divider":
        return {"type": "divider"}

    if block_type in ("paragraph", "heading_1", "heading_2", "heading_3", "bulleted_list_item", "numbered_list_item"):
        rich_text = block_data.get("rich_text", [])
        return {"type": block_type, "text": rich_text_to_markdown(rich_text)}

    if block_type == "to_do":
        rich_text = block_data.get("rich_text", [])
        return {
            "type": block_type,
            "text": rich_text_to_markdown(rich_text),
            "checked": block_data.get("checked", False),
        }

    if block_type == "code":
        rich_text = block_data.get("rich_text", [])
        code = "".join(item.get("text", {}).get("content", "") for item in rich_text)
        return {"type": block_type, "code": code, "language": block_data.get("language", "")}

    if block_type in ("quote", "callout"):
        rich_text = block_data.get("rich_text", [])
        result: dict[str, Any] = {"type": block_type, "text": rich_text_to_markdown(rich_text)}
        if block_type == "callout":
            icon = block_data.get("icon", {})
            result["emoji"] = icon.get("emoji", "")
        return result

    return {"type": block_type}


def blocks_content_equal(
    existing: Block | PartialBlock | dict[str, Any],
    new: NotionBlock | dict[str, Any],
) -> bool:
    """Check if two blocks have equivalent content.

    Compares only content-relevant fields, ignoring metadata like IDs and timestamps.
    This allows comparing an existing Notion block (with full metadata) against
    a newly created block (without IDs).

    Args:
        existing: An existing block from Notion (has id, timestamps, etc.)
        new: A new block to compare (may not have id/timestamps)

    Returns:
        True if the blocks have equivalent content
    """
    return _extract_block_content(existing) == _extract_block_content(new)


def find_divergence_index(
    existing_blocks: Sequence[Block | PartialBlock] | Sequence[dict[str, Any]],
    new_blocks: Sequence[NotionBlock | dict[str, Any]],
) -> int:
    """Find the index where existing blocks start to differ from new blocks.

    Compares blocks from the start until a difference is found.
    Blocks that match are preserved (keeping their IDs and comments).

    Args:
        existing_blocks: Current blocks from Notion
        new_blocks: New blocks to replace the content

    Returns:
        Index of first differing block. Returns min(len(existing), len(new))
        if all compared blocks match.
    """
    min_len = min(len(existing_blocks), len(new_blocks))
    for i in range(min_len):
        if not blocks_content_equal(existing_blocks[i], new_blocks[i]):
            return i
    return min_len
