from typing import Optional
from pydantic import BaseModel

class FileRef(BaseModel):
  name: str
  path: str
  size: int
  alt_name: Optional[str] = None
  alt_path: Optional[str] = None
  alt_size: Optional[int] = None