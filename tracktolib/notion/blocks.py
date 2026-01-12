"""Notion block creation utilities for markdown conversion."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from .models import Block, Comment, PartialBlock, RichTextItemResponse

__all__ = [
    # Types
    "ExportResult",
    # Rich text parsing
    "parse_rich_text",
    "rich_text_to_markdown",
    # Block creators
    "make_paragraph_block",
    "make_heading_block",
    "make_code_block",
    "make_bulleted_list_block",
    "make_numbered_list_block",
    "make_todo_block",
    "make_divider_block",
    # Markdown conversion
    "markdown_to_blocks",
    "blocks_to_markdown",
    "blocks_to_markdown_with_comments",
    "comments_to_markdown",
]


class ExportResult(TypedDict):
    """Result of exporting markdown to Notion."""

    count: int
    """Number of blocks created in the page."""
    url: str | None
    """URL of the created Notion page, or None if creation failed."""


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


def parse_rich_text(text: str) -> list[dict[str, Any]]:
    """Parse markdown inline formatting to Notion rich_text array.

    Handles:
    - **bold** or __bold__
    - `inline code`
    - *italic* or _italic_
    """
    rich_text: list[dict[str, Any]] = []
    # Pattern to match bold, code, or italic (in order of priority)
    pattern = re.compile(r"(\*\*(.+?)\*\*|__(.+?)__|`([^`]+)`|\*([^*]+)\*|_([^_]+)_)")

    pos = 0
    for match in pattern.finditer(text):
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


def make_paragraph_block(text: str) -> dict[str, Any]:
    """Create a Notion paragraph block with rich text formatting."""
    # Truncate to Notion's 2000 char limit per rich_text element
    if len(text) > 2000:
        text = text[:2000]
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


def make_code_block(code: str, language: str = "plain text") -> dict[str, Any]:
    """Create a Notion code block.

    Args:
        code: The code content
        language: Programming language (supports aliases like 'py', 'js', 'ts')
    """
    notion_lang = LANGUAGE_ALIASES.get(language.lower(), language.lower())

    return {
        "object": "block",
        "type": "code",
        "code": {
            "rich_text": [{"type": "text", "text": {"content": code[:2000]}}],
            "language": notion_lang,
        },
    }


def make_bulleted_list_block(text: str) -> dict[str, Any]:
    """Create a Notion bulleted list item block."""
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {
            "rich_text": parse_rich_text(text),
        },
    }


def make_numbered_list_block(text: str) -> dict[str, Any]:
    """Create a Notion numbered list item block."""
    return {
        "object": "block",
        "type": "numbered_list_item",
        "numbered_list_item": {
            "rich_text": parse_rich_text(text),
        },
    }


def make_todo_block(text: str, checked: bool = False) -> dict[str, Any]:
    """Create a Notion to_do block (checkbox item)."""
    return {
        "object": "block",
        "type": "to_do",
        "to_do": {
            "rich_text": parse_rich_text(text),
            "checked": checked,
        },
    }


def make_divider_block() -> dict[str, Any]:
    """Create a Notion divider block (horizontal rule)."""
    return {
        "object": "block",
        "type": "divider",
        "divider": {},
    }


def markdown_to_blocks(content: str, *, max_blocks: int = 100) -> list[dict[str, Any]]:
    """Convert markdown content to Notion blocks with proper formatting.

    Handles:
    - Code blocks (```)
    - Headings (# ## ### etc)
    - Bold (**text**)
    - Inline code (`code`)
    - Italic (*text*)
    - Todo items (- [ ] or - [x])
    - Bulleted lists (- or *)
    - Numbered lists (1. 2. etc)
    - Horizontal rules (---)

    Args:
        content: Markdown content to convert
        max_blocks: Maximum number of blocks to return (Notion API limit is 100)

    Returns:
        List of Notion block objects
    """
    blocks: list[dict[str, Any]] = []
    lines = content.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check for fenced code block
        code_match = re.match(r"^```(\w*)$", line)
        if code_match:
            language = code_match.group(1) or "plain text"
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                code_lines.append(lines[i])
                i += 1
            code_content = "\n".join(code_lines)
            if code_content:
                blocks.append(make_code_block(code_content, language))
            i += 1  # Skip closing ```
            continue

        # Check for horizontal rule (---, ***, ___)
        if re.match(r"^[-*_]{3,}\s*$", line):
            blocks.append(make_divider_block())
            i += 1
            continue

        # Check for heading
        heading_match = re.match(r"^(#{1,6})\s+(.+)$", line)
        if heading_match:
            level = len(heading_match.group(1))
            text = heading_match.group(2).strip()
            blocks.append(make_heading_block(text, level))
            i += 1
            continue

        # Check for todo item (- [ ] or - [x]) - must be before bullet list
        todo_match = re.match(r"^\s*[-*]\s*\[([xX ])\]\s*(.*)$", line)
        if todo_match:
            checked = todo_match.group(1).lower() == "x"
            text = todo_match.group(2).strip()
            blocks.append(make_todo_block(text, checked))
            i += 1
            continue

        # Check for bulleted list
        bullet_match = re.match(r"^\s*[-*]\s+(.+)$", line)
        if bullet_match:
            text = bullet_match.group(1).strip()
            blocks.append(make_bulleted_list_block(text))
            i += 1
            continue

        # Check for numbered list
        numbered_match = re.match(r"^\s*\d+\.\s+(.+)$", line)
        if numbered_match:
            text = numbered_match.group(1).strip()
            blocks.append(make_numbered_list_block(text))
            i += 1
            continue

        # Skip empty lines
        if not line.strip():
            i += 1
            continue

        # Regular paragraph - collect consecutive non-empty lines
        para_lines = [line]
        i += 1
        while i < len(lines):
            next_line = lines[i]
            # Stop at special lines
            if (
                not next_line.strip()
                or next_line.startswith("#")
                or next_line.startswith("```")
                or re.match(r"^[-*_]{3,}\s*$", next_line)
                or re.match(r"^\s*[-*]\s*\[", next_line)
                or re.match(r"^\s*[-*]\s+", next_line)
                or re.match(r"^\s*\d+\.\s+", next_line)
            ):
                break
            para_lines.append(next_line)
            i += 1

        para_text = " ".join(ln.strip() for ln in para_lines)
        if para_text:
            # Split long paragraphs
            if len(para_text) > 2000:
                for j in range(0, len(para_text), 2000):
                    blocks.append(make_paragraph_block(para_text[j : j + 2000]))
            else:
                blocks.append(make_paragraph_block(para_text))

    # Notion API limit: max 100 children per request
    return blocks[:max_blocks]


def rich_text_to_markdown(rich_text: Sequence[RichTextItemResponse] | Sequence[dict[str, Any]]) -> str:
    """Convert Notion rich_text array to markdown string.

    Handles:
    - Bold (annotations.bold)
    - Italic (annotations.italic)
    - Inline code (annotations.code)
    - Links (text.link.url)
    """
    result = []
    for item in rich_text:
        text_obj = item.get("text", {})
        content = text_obj.get("content", "")

        if not content:
            continue

        annotations = item.get("annotations", {})
        link = text_obj.get("link")

        # Apply formatting in order: code, bold, italic
        if annotations.get("code"):
            content = f"`{content}`"
        if annotations.get("bold"):
            content = f"**{content}**"
        if annotations.get("italic"):
            content = f"*{content}*"
        if link:
            content = f"[{content}]({link['url']})"

        result.append(content)

    return "".join(result)


def _block_to_markdown(block: Block | PartialBlock | dict[str, Any]) -> str | None:
    """Convert a single Notion block to markdown.

    Returns None for unsupported block types.
    """
    block_type = block.get("type")
    if not block_type:
        return None

    block_data = block.get(block_type, {})

    if block_type == "paragraph":
        rich_text = block_data.get("rich_text", [])
        text = rich_text_to_markdown(rich_text)
        return text if text else ""

    if block_type in ("heading_1", "heading_2", "heading_3"):
        level = int(block_type[-1])
        rich_text = block_data.get("rich_text", [])
        text = rich_text_to_markdown(rich_text)
        return f"{'#' * level} {text}"

    if block_type == "code":
        rich_text = block_data.get("rich_text", [])
        code = "".join(item.get("text", {}).get("content", "") for item in rich_text)
        language = block_data.get("language", "")
        # Map Notion language back to common alias
        if language == "plain text":
            language = ""
        return f"```{language}\n{code}\n```"

    if block_type == "bulleted_list_item":
        rich_text = block_data.get("rich_text", [])
        text = rich_text_to_markdown(rich_text)
        return f"- {text}"

    if block_type == "numbered_list_item":
        rich_text = block_data.get("rich_text", [])
        text = rich_text_to_markdown(rich_text)
        return f"1. {text}"

    if block_type == "to_do":
        rich_text = block_data.get("rich_text", [])
        text = rich_text_to_markdown(rich_text)
        checked = block_data.get("checked", False)
        checkbox = "[x]" if checked else "[ ]"
        return f"- {checkbox} {text}"

    if block_type == "divider":
        return "---"

    if block_type == "quote":
        rich_text = block_data.get("rich_text", [])
        text = rich_text_to_markdown(rich_text)
        return f"> {text}"

    if block_type == "callout":
        rich_text = block_data.get("rich_text", [])
        text = rich_text_to_markdown(rich_text)
        icon = block_data.get("icon", {})
        emoji = icon.get("emoji", "")
        prefix = f"{emoji} " if emoji else ""
        return f"> {prefix}{text}"

    # Unsupported block type
    return None


def blocks_to_markdown(blocks: list[Block | PartialBlock] | list[dict[str, Any]]) -> str:
    """Convert a list of Notion blocks to markdown content.

    Handles:
    - Paragraphs
    - Headings (h1, h2, h3)
    - Code blocks
    - Bulleted lists
    - Numbered lists
    - Todo items
    - Dividers
    - Quotes
    - Callouts

    Args:
        blocks: List of Notion block objects

    Returns:
        Markdown string
    """
    lines: list[str] = []

    for block in blocks:
        md_line = _block_to_markdown(block)
        if md_line is not None:
            lines.append(md_line)

    return "\n\n".join(lines)


def _inline_comment_to_markdown(comment: Comment | dict[str, Any]) -> str:
    """Convert a single inline comment to markdown format."""
    rich_text = comment.get("rich_text", [])
    text = rich_text_to_markdown(rich_text)

    created_by = comment.get("created_by", {})
    author = created_by.get("name") or created_by.get("id", "Unknown")

    created_time = comment.get("created_time", "")
    if created_time:
        timestamp = created_time[:16].replace("T", " ")
    else:
        timestamp = ""

    header = f"**{author}**"
    if timestamp:
        header += f" - {timestamp}"

    return f"> 💬 {header}: {text}"


def blocks_to_markdown_with_comments(
    blocks: list[Block | PartialBlock] | list[dict[str, Any]],
    block_comments: dict[str, list[Comment]] | dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    """Convert a list of Notion blocks to markdown content with inline comments.

    Args:
        blocks: List of Notion block objects
        block_comments: Dictionary mapping block IDs to their comments

    Returns:
        Markdown string with inline comments after their respective blocks
    """
    if block_comments is None:
        block_comments = {}

    lines: list[str] = []

    for block in blocks:
        md_line = _block_to_markdown(block)
        if md_line is not None:
            lines.append(md_line)

            # Add inline comments for this block
            block_id = block.get("id")
            if block_id and block_id in block_comments:
                for comment in block_comments[block_id]:
                    comment_md = _inline_comment_to_markdown(comment)
                    lines.append(comment_md)

    return "\n\n".join(lines)


def comments_to_markdown(comments: list[Comment] | list[dict[str, Any]]) -> str:
    """Convert a list of Notion comments to markdown.

    Each comment is formatted as a blockquote with author and timestamp.

    Args:
        comments: List of Notion comment objects

    Returns:
        Markdown string with comments section
    """
    if not comments:
        return ""

    lines: list[str] = ["## Comments", ""]

    for comment in comments:
        rich_text = comment.get("rich_text", [])
        text = rich_text_to_markdown(rich_text)

        # Get author info
        created_by = comment.get("created_by", {})
        author = created_by.get("name") or created_by.get("id", "Unknown")

        # Get timestamp
        created_time = comment.get("created_time", "")
        if created_time:
            # Format: 2024-01-15T10:30:00.000Z -> 2024-01-15 10:30
            timestamp = created_time[:16].replace("T", " ")
        else:
            timestamp = ""

        # Format as blockquote with metadata
        header = f"**{author}**"
        if timestamp:
            header += f" - {timestamp}"

        lines.append(f"> {header}")
        lines.append(f"> {text}")
        lines.append("")

    return "\n".join(lines)
