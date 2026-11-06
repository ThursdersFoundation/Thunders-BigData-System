"""Business logic for datasets."""


async def list_datasets() -> list[dict]:
    return []


async def create_dataset(name: str, layer: str) -> dict:
    return {"id": "TODO", "name": name, "layer": layer}
