"""Data access for datasets."""

from sqlalchemy.ext.asyncio import AsyncSession


async def list_datasets(session: AsyncSession) -> list[dict]:
    result = await session.execute("SELECT id, name, layer FROM datasets")
    return [dict(r) for r in result.fetchall()]
