from typing import Optional, List
from pydantic import BaseModel, Field

class FileRef(BaseModel):
  name: str
  path: str
  size: int
  alt_name: Optional[str] = None
  alt_path: Optional[str] = None
  alt_size: Optional[int] = None
  
class FileNode(BaseModel):
  name: str
  path: Optional[str] = None
  size: Optional[int] = None
  alt_name: Optional[str] = None
  alt_path: Optional[str] = None
  alt_size: Optional[int] = None
  is_file: bool
  children: Optional[List["FileNode"]] = Field(default_factory=list)
  
# We need to update self references.
FileNode.model_rebuild()