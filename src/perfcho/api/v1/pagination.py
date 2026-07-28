"""Provide bounded pagination contracts for administrative HTTP APIs."""

from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

MAX_RESULTS_PER_PAGE = 50


class PaginationParams(BaseModel):
    """Validate page-number pagination supplied by an API caller."""

    page_number: int = Field(default=1, ge=1)
    page_size: int = Field(default=10, ge=1, le=MAX_RESULTS_PER_PAGE)


class Page[T](BaseModel):
    """Return one page together with total row and page metadata."""

    records: list[T] = Field()
    total_row: int = Field(ge=0)
    total_page: int = Field(ge=0)
    page_number: int = Field(ge=0)
    page_size: int = Field(ge=0)


async def paginate[T](
    session: AsyncSession,
    query: Select[T],
    params: PaginationParams,
) -> Page[T]:
    """Execute a bounded count and page query in the caller transaction."""
    total_row = await session.scalar(select(func.count()).select_from(query.subquery()))
    if not isinstance(total_row, int):
        raise RuntimeError("Database error occurred while fetching `total_row`.")

    total_pages = (total_row + params.page_size - 1) // params.page_size
    total_pages = max(total_pages, 1)
    current_page = min(params.page_number, total_pages)
    offset = (current_page - 1) * params.page_size

    result = await session.scalars(query.offset(offset).limit(params.page_size))
    items = list(result.all())

    return Page[T](
        records=items,
        total_row=total_row,
        total_page=total_pages,
        page_size=len(items),
        page_number=current_page,
    )
