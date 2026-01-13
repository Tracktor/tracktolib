"""Markdown conversion utilities for Notion blocks."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from tracktolib.notion.blocks import (
    BulletedListBlock,
    DividerBlock,
    NumberedListBlock,
    ParagraphBlock,
    QuoteBlock,
    TodoBlock,
    make_bulleted_list_block,
    make_code_block,
    make_divider_block,
    make_heading_block,
    make_numbered_list_block,
    make_paragraph_block,
    make_quote_block,
    make_todo_block,
)

from tracktolib.utils import get_chunks

# Union type for all Notion blocks used in markdown conversion
NotionBlock = ParagraphBlock | DividerBlock | BulletedListBlock | NumberedListBlock | TodoBlock | QuoteBlock

if TYPE_CHECKING:
    from tracktolib.notion.models import Block, Comment, PartialBlock, RichTextItemResponse

__all__ = [
    "NOTION_CHAR_LIMIT",
    "NotionBlock",
    "rich_text_to_markdown",
    "markdown_to_blocks",
    "blocks_to_markdown",
    "blocks_to_markdown_with_comments",
    "comments_to_markdown",
    "strip_comments_from_markdown",
]

# Notion's character limit per rich_text element
NOTION_CHAR_LIMIT = 2000

# Markdown block patterns (pre-compiled for performance)
_CODE_FENCE_PATTERN = re.compile(r"^```(\w*)$")
_HORIZONTAL_RULE_PATTERN = re.compile(r"^[-*_]{3,}\s*$")
_HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.+)$")
_TODO_PATTERN = re.compile(r"^\s*[-*]\s*\[([xX ])\]\s*(.*)$")
_BULLET_PATTERN = re.compile(r"^\s*[-*]\s+(.+)$")
_NUMBERED_PATTERN = re.compile(r"^\s*\d+\.\s+(.+)$")
_QUOTE_PATTERN = re.compile(r"^>\s*(.*)$")


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


def markdown_to_blocks(content: str) -> list[NotionBlock | dict[str, Any]]:
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

    Returns:
        List of Notion block objects (caller handles chunking for API limits)
    """
    blocks: list[NotionBlock | dict[str, Any]] = []
    lines = content.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check for fenced code block
        code_match = _CODE_FENCE_PATTERN.match(line)
        if code_match:
            language = code_match.group(1) or "plain text"
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                code_lines.append(lines[i])
                i += 1
            code_content = "\n".join(code_lines)
            if code_content:
                blocks.extend(make_code_block(code_content, language))
            i += 1  # Skip closing ```
            continue

        # Check for horizontal rule (---, ***, ___)
        if _HORIZONTAL_RULE_PATTERN.match(line):
            blocks.append(make_divider_block())
            i += 1
            continue

        # Check for heading
        heading_match = _HEADING_PATTERN.match(line)
        if heading_match:
            level = len(heading_match.group(1))
            text = heading_match.group(2).strip()
            blocks.append(make_heading_block(text, level))
            i += 1
            continue

        # Check for todo item (- [ ] or - [x]) - must be before bullet list
        todo_match = _TODO_PATTERN.match(line)
        if todo_match:
            checked = todo_match.group(1).lower() == "x"
            text = todo_match.group(2).strip()
            blocks.append(make_todo_block(text, checked))
            i += 1
            continue

        # Check for bulleted list
        bullet_match = _BULLET_PATTERN.match(line)
        if bullet_match:
            text = bullet_match.group(1).strip()
            blocks.append(make_bulleted_list_block(text))
            i += 1
            continue

        # Check for numbered list
        numbered_match = _NUMBERED_PATTERN.match(line)
        if numbered_match:
            text = numbered_match.group(1).strip()
            blocks.append(make_numbered_list_block(text))
            i += 1
            continue

        # Check for blockquote
        quote_match = _QUOTE_PATTERN.match(line)
        if quote_match:
            text = quote_match.group(1)
            blocks.append(make_quote_block(text))
            i += 1
            continue

        # Empty line - check if it separates quote blocks
        if not line.strip():
            # Look ahead to see if next non-empty line is a quote
            # and previous block was also a quote
            if blocks and blocks[-1].get("type") == "quote":
                j = i + 1
                while j < len(lines) and not lines[j].strip():
                    j += 1
                if j < len(lines) and lines[j].startswith(">"):
                    # Insert empty paragraph to preserve blank line between quotes
                    blocks.append(make_paragraph_block(""))
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
                or next_line.startswith(">")
                or _HORIZONTAL_RULE_PATTERN.match(next_line)
                or _TODO_PATTERN.match(next_line)
                or _BULLET_PATTERN.match(next_line)
                or _NUMBERED_PATTERN.match(next_line)
            ):
                break
            para_lines.append(next_line)
            i += 1

        para_text = " ".join(ln.strip() for ln in para_lines)
        if para_text:
            # Split long paragraphs into chunks
            if len(para_text) > NOTION_CHAR_LIMIT:
                for chunk in get_chunks(para_text, NOTION_CHAR_LIMIT):
                    blocks.append(make_paragraph_block("".join(chunk)))
            else:
                blocks.append(make_paragraph_block(para_text))

    return blocks


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
    result: list[str] = []
    prev_type: str | None = None

    for block in blocks:
        block_type = block.get("type")
        md_line = _block_to_markdown(block)
        if md_line is not None:
            # Empty paragraph acts as separator (resets consecutive quote joining)
            if block_type == "paragraph" and md_line == "":
                prev_type = None
                continue
            # Join consecutive quotes with single newline
            if prev_type == "quote" and block_type == "quote":
                result.append(f"\n{md_line}")
            elif result:
                result.append(f"\n\n{md_line}")
            else:
                result.append(md_line)
            prev_type = block_type

    return "".join(result)


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

    result: list[str] = []
    prev_type: str | None = None

    for block in blocks:
        block_type = block.get("type")
        md_line = _block_to_markdown(block)
        if md_line is not None:
            # Empty paragraph acts as separator (resets consecutive quote joining)
            if block_type == "paragraph" and md_line == "":
                prev_type = None
                continue
            # Join consecutive quotes with single newline
            if prev_type == "quote" and block_type == "quote":
                result.append(f"\n{md_line}")
            elif result:
                result.append(f"\n\n{md_line}")
            else:
                result.append(md_line)
            prev_type = block_type

            # Add inline comments for this block
            block_id = block.get("id")
            if block_id and block_id in block_comments:
                for comment in block_comments[block_id]:
                    comment_md = _inline_comment_to_markdown(comment)
                    result.append(f"\n\n{comment_md}")
                    prev_type = None  # Reset after comment

    return "".join(result)


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


def strip_comments_from_markdown(content: str) -> str:
    """Remove comment blockquotes (> 💬) from markdown content.

    This is useful when re-uploading markdown that was downloaded with comments,
    to avoid converting comments into regular quote blocks.

    Args:
        content: Markdown content potentially containing comment blockquotes

    Returns:
        Markdown content with comment lines removed
    """
    lines = content.splitlines()
    result = []
    for line in lines:
        if line.startswith("> 💬"):
            continue
        result.append(line)
    return "\n".join(result)
