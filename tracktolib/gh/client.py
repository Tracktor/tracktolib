from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, cast
from urllib.parse import quote

try:
    import niquests
except ImportError:
    raise ImportError('Please install niquests or tracktolib with "gh" to use this module')

if TYPE_CHECKING:
    from urllib3.util.retry import Retry

    from tracktolib.gh.types import Deployment, DeploymentStatus, IssueComment, Label


ProgressCallback = Callable[[int, int], None]


@dataclass
class GitHubClient:
    """
    Async GitHub API client for issues, labels, and deployments.
    """

    token: str | None = field(default_factory=lambda: os.environ.get("GITHUB_TOKEN"))
    base_url: str = "https://api.github.com"
    retries: int | Retry = 0
    hooks: Any = None
    session: niquests.AsyncSession = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.token:
            raise ValueError("GITHUB_TOKEN environment variable is required")

        self.session = niquests.AsyncSession(
            base_url=self.base_url,
            retries=self.retries,
            hooks=self.hooks,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )

    async def __aenter__(self) -> GitHubClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying session."""
        await self.session.close()

    # Issue Comments

    async def get_issue_comments(self, repository: str, issue_number: int) -> list[IssueComment]:
        """Get all comments on an issue or PR."""
        response = await self.session.get(f"/repos/{repository}/issues/{issue_number}/comments")
        response.raise_for_status()
        return cast("list[IssueComment]", response.json())

    async def create_issue_comment(self, repository: str, issue_number: int, body: str) -> IssueComment:
        """Create a comment on an issue or PR."""
        response = await self.session.post(f"/repos/{repository}/issues/{issue_number}/comments", json={"body": body})
        response.raise_for_status()
        return cast("IssueComment", response.json())

    async def delete_issue_comment(self, repository: str, comment_id: int) -> None:
        """Delete a comment by ID."""
        response = await self.session.delete(f"/repos/{repository}/issues/comments/{comment_id}")
        response.raise_for_status()

    async def find_comments_with_marker(self, repository: str, issue_number: int, marker: str) -> list[int]:
        """Find comment IDs containing a specific marker string."""
        comments = await self.get_issue_comments(repository, issue_number)
        return [c["id"] for c in comments if marker in c.get("body", "")]

    async def delete_comments_with_marker(
        self,
        repository: str,
        issue_number: int,
        marker: str,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> int:
        """Delete all comments containing a specific marker. Returns count deleted."""
        comment_ids = await self.find_comments_with_marker(repository, issue_number, marker)
        total = len(comment_ids)
        for i, comment_id in enumerate(comment_ids):
            await self.delete_issue_comment(repository, comment_id)
            if on_progress:
                on_progress(i + 1, total)
        return total

    async def create_idempotent_comment(
        self, repository: str, issue_number: int, body: str, marker: str
    ) -> IssueComment | None:
        """
        Create a comment only if one with the marker doesn't already exist.

        The marker should be included in the body (e.g., an HTML comment like
        '<!-- my-marker -->'). Returns the created comment, or None if skipped.
        """
        if await self.find_comments_with_marker(repository, issue_number, marker):
            return None
        return await self.create_issue_comment(repository, issue_number, body)

    # Labels

    async def get_issue_labels(self, repository: str, issue_number: int) -> list[Label]:
        """Get all labels on an issue or PR."""
        response = await self.session.get(f"/repos/{repository}/issues/{issue_number}/labels")
        response.raise_for_status()
        return cast("list[Label]", response.json())

    async def add_labels(self, repository: str, issue_number: int, labels: list[str]) -> list[Label]:
        """Add labels to an issue or PR."""
        response = await self.session.post(f"/repos/{repository}/issues/{issue_number}/labels", json={"labels": labels})
        response.raise_for_status()
        return cast("list[Label]", response.json())

    async def remove_label(self, repository: str, issue_number: int, label: str) -> bool:
        """Remove a label from an issue/PR. Returns True if removed, False if not found."""
        response = await self.session.delete(
            f"/repos/{repository}/issues/{issue_number}/labels/{quote(label, safe='')}"
        )
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    # Deployments

    async def get_deployments(self, repository: str, *, environment: str | None = None) -> list[Deployment]:
        """Get deployments, optionally filtered by environment."""
        params = {"environment": environment} if environment else {}
        response = await self.session.get(f"/repos/{repository}/deployments", params=params)
        response.raise_for_status()
        return cast("list[Deployment]", response.json())

    async def create_deployment_status(
        self,
        repository: str,
        deployment_id: int,
        state: str,
        *,
        description: str | None = None,
        environment_url: str | None = None,
    ) -> DeploymentStatus:
        """
        Create a deployment status.

        State can be: error, failure, inactive, in_progress, queued, pending, success.
        """
        payload: dict = {"state": state}
        if description:
            payload["description"] = description
        if environment_url:
            payload["environment_url"] = environment_url
        response = await self.session.post(f"/repos/{repository}/deployments/{deployment_id}/statuses", json=payload)
        response.raise_for_status()
        return cast("DeploymentStatus", response.json())

    async def get_deployment_statuses(
        self,
        repository: str,
        deployment_id: int,
    ) -> list[DeploymentStatus]:
        """Get all statuses for a deployment, most recent first."""
        response = await self.session.get(f"/repos/{repository}/deployments/{deployment_id}/statuses")
        response.raise_for_status()
        return cast("list[DeploymentStatus]", response.json())

    async def get_latest_deployment_status(
        self,
        repository: str,
        environment: str,
    ) -> DeploymentStatus | None:
        """Get the latest deployment status for an environment."""
        deployments = await self.get_deployments(repository, environment=environment)
        if not deployments:
            return None
        statuses = await self.get_deployment_statuses(repository, deployments[0]["id"])
        return statuses[0] if statuses else None

    async def mark_deployment_inactive(
        self,
        repository: str,
        environment: str,
        *,
        description: str = "Environment removed",
        on_progress: ProgressCallback | None = None,
    ) -> int:
        """Mark all deployments for an environment as inactive. Returns count updated."""
        deployments = await self.get_deployments(repository, environment=environment)
        total = len(deployments)
        for i, deployment in enumerate(deployments):
            await self.create_deployment_status(repository, deployment["id"], "inactive", description=description)
            if on_progress:
                on_progress(i + 1, total)
        return total
