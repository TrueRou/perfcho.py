"""Adapt osu!lazer comment endpoints onto the content service."""

from typing import Annotated

from fastapi import APIRouter, Form, Query
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalAccountDependency, CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error
from perfcho.modules.content import CommentView, ContentInputRejected

router = APIRouter()


@router.get("/comments", response_model=None, tags=["Comments"])
async def list_comments(
    services: CanonicalServicesDependency,
    commentable_id: Annotated[int, Query(gt=0)],
    commentable_type: Annotated[str, Query()] = "beatmapset",
) -> dict[str, object] | JSONResponse:
    """List comments for one content target."""
    if services.content_query is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    target = _target(commentable_type)
    if target is None:
        return error(422, "invalid_request", "Unsupported commentable type.")
    comments = await services.content_query.list_comments(target, commentable_id)
    return {"comments": [_comment(comment) for comment in comments], "has_more": False, "total": len(comments)}


@router.post("/comments", response_model=None, tags=["Comments"])
async def post_comment(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    commentable_type: Annotated[str, Form(alias="comment[commentable_type]")] = "beatmapset",
    commentable_id: Annotated[int, Form(alias="comment[commentable_id]", gt=0)] = 0,
    message: Annotated[str, Form(alias="comment[message]")] = "",
    parent_id: Annotated[int | None, Form(alias="comment[parent_id]")] = None,
) -> dict[str, object] | JSONResponse:
    """Post one comment (parent_id ignored; no threaded comments yet)."""
    del parent_id
    if services.content is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    target = _target(commentable_type)
    if target is None:
        return error(422, "invalid_request", "Unsupported commentable type.")
    try:
        comment = await services.content.create_comment(account.account_id, target, commentable_id, 0, message)
    except ContentInputRejected as exc:
        return error(422, "invalid_request", str(exc))
    return {"comments": [_comment(comment)]}


@router.delete("/comments/{comment_id}", response_model=None, tags=["Comments"])
async def delete_comment(
    comment_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> JSONResponse:
    """Return a valid empty result for an unsupported comment deletion."""
    del comment_id, services, account
    return JSONResponse(status_code=204, content=None)


def _comment(comment: CommentView) -> dict[str, object]:
    commentable_type = {"song": "beatmapset", "map": "beatmap", "replay": "score"}.get(comment.target, comment.target)
    return {
        "id": comment.comment_id,
        "user_id": comment.author_account_id,
        "message": comment.body,
        "commentable_type": commentable_type,
        "commentable_id": 0,
        "created_at": comment.created_at.isoformat(),
        "pinned": False,
        "votes_count": 0,
        "parent_id": None,
        "user": {"id": comment.author_account_id, "username": ""},
    }


def _target(commentable_type: str) -> str | None:
    # Canonical comments use stable target codes: map / song / replay.
    return {"beatmapset": "song", "beatmap": "map"}.get(commentable_type)
