from pydantic import BaseModel

class ListResult(BaseModel):
    total: int
    skip: int | None
    limit: int | None
