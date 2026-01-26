---
title: "GitHub"
---

# GitHub

GitHub API helpers using [niquests](https://github.com/jawah/niquests).

## Installation

```bash
uv add tracktolib[gh]
```

## Dependencies

- [niquests](https://github.com/jawah/niquests) - Modern HTTP client with HTTP/3 support

## Overview

This module provides an async client for the [GitHub REST API](https://docs.github.com/en/rest):

- Issue and PR comment management (create, delete, idempotent operations)
- Label management (add, remove, list)
- Deployment status management (list, mark inactive)

## Authentication

The client uses the `GITHUB_TOKEN` environment variable by default:

```python
from tracktolib.gh import GitHubClient

async with GitHubClient() as gh:
    # ... use client
```

Or pass a token explicitly:

```python
async with GitHubClient(token="ghp_xxx") as gh:
    # ... use client
```

## Issue Comments

### `get_issue_comments(repository, issue_number) -> list[IssueComment]`

Get all comments on an issue or PR.

```python
comments = await gh.get_issue_comments("owner/repo", 123)
for c in comments:
    print(f"{c['user']['login']}: {c['body']}")
```

### `create_issue_comment(repository, issue_number, body) -> IssueComment`

Create a comment on an issue or PR.

```python
comment = await gh.create_issue_comment("owner/repo", 123, "Hello from bot!")
print(f"Created comment {comment['id']}")
```

### `delete_issue_comment(repository, comment_id) -> None`

Delete a comment by ID.

```python
await gh.delete_issue_comment("owner/repo", 12345678)
```

### `find_comments_with_marker(repository, issue_number, marker) -> list[int]`

Find comment IDs containing a specific marker string.

```python
# Find all bot comments
ids = await gh.find_comments_with_marker("owner/repo", 123, "<!-- my-bot -->")
```

### `delete_comments_with_marker(repository, issue_number, marker, *, on_progress) -> int`

Delete all comments containing a specific marker. Returns the count of deleted comments.

```python
deleted = await gh.delete_comments_with_marker(
    "owner/repo", 123, "<!-- preview-bot -->",
    on_progress=lambda i, total: print(f"Deleted {i}/{total}")
)
print(f"Removed {deleted} old bot comments")
```

### `create_idempotent_comment(repository, issue_number, body, marker) -> IssueComment | None`

Create a comment only if one with the marker doesn't already exist. Returns the created comment, or `None` if skipped.

```python
# Only post once per PR
body = "<!-- ci-status -->\n## Build Status\n..."
comment = await gh.create_idempotent_comment("owner/repo", 123, body, "<!-- ci-status -->")
if comment:
    print("Posted new status comment")
else:
    print("Status comment already exists")
```

## Labels

### `get_issue_labels(repository, issue_number) -> list[Label]`

Get all labels on an issue or PR.

```python
labels = await gh.get_issue_labels("owner/repo", 123)
print([l["name"] for l in labels])
```

### `add_labels(repository, issue_number, labels) -> list[Label]`

Add labels to an issue or PR.

```python
await gh.add_labels("owner/repo", 123, ["bug", "priority:high"])
```

### `remove_label(repository, issue_number, label) -> bool`

Remove a label from an issue or PR. Returns `True` if removed, `False` if not found.

```python
if await gh.remove_label("owner/repo", 123, "needs-review"):
    print("Label removed")
```

## Deployments

### `get_deployments(repository, *, environment) -> list[Deployment]`

Get deployments, optionally filtered by environment.

```python
# All deployments
deploys = await gh.get_deployments("owner/repo")

# Filter by environment
preview_deploys = await gh.get_deployments("owner/repo", environment="preview-123")
```

### `create_deployment_status(repository, deployment_id, state, *, description, environment_url) -> DeploymentStatus`

Create a deployment status. State can be: `error`, `failure`, `inactive`, `in_progress`, `queued`, `pending`, `success`.

```python
status = await gh.create_deployment_status(
    "owner/repo",
    deployment_id=123456,
    state="success",
    description="Deployed successfully",
    environment_url="https://preview-123.example.com",
)
```

### `mark_deployment_inactive(repository, environment, *, description, on_progress) -> int`

Mark all deployments for an environment as inactive. Returns the count of updated deployments.

```python
# Clean up preview environment when PR is closed
count = await gh.mark_deployment_inactive(
    "owner/repo",
    "preview-pr-42",
    description="PR closed",
    on_progress=lambda i, total: print(f"Deactivated {i}/{total}"),
)
print(f"Marked {count} deployments as inactive")
```

## Configuration

### Custom Base URL

For GitHub Enterprise:

```python
async with GitHubClient(base_url="https://github.mycompany.com/api/v3") as gh:
    # ...
```

### Retries

Configure automatic retries:

```python
from urllib3.util.retry import Retry

retry = Retry(total=3, backoff_factor=0.5)
async with GitHubClient(retries=retry) as gh:
    # ...
```

### Request Hooks

Add custom hooks for logging or metrics:

```python
def log_response(response, **kwargs):
    print(f"{response.request.method} {response.url} -> {response.status_code}")

async with GitHubClient(hooks={"response": [log_response]}) as gh:
    # ...
```

## Error Handling

The client raises `GitHubError` on API failures:

```python
from tracktolib.gh import GitHubClient, GitHubError

try:
    await gh.create_issue_comment("owner/repo", 999999, "test")
except GitHubError as e:
    print(f"GitHub API error: {e} (status: {e.status_code})")
```

## Types

The module exports TypedDict types generated from GitHub's OpenAPI spec:

- `IssueComment` - Issue/PR comment data
- `Label` - Label data
- `Deployment` - Deployment data
- `DeploymentStatus` - Deployment status data

```python
from tracktolib.gh import IssueComment, Label, Deployment, DeploymentStatus
```