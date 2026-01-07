from typing import List, Tuple, Any
from fastapi.datastructures import UploadFile
from ..models.files import FileNode
import logging
import shutil
import mimetypes
from pathlib import Path

class FilesService:
  """
  This service provides file-related operations. It is an abstraction layer
  over different storage backends such as S3 or local file system.
  """
  
  async def upload_file(self, upload_file: UploadFile, folder: str = "") -> FileNode:
    """Upload a file to the specified folder.

    Args:
        upload_file (UploadFile): The file to upload.
        folder (str, optional): The folder to upload the file to. Defaults to "".
    
    Returns:
        FileNode: The uploaded file node.
    """
    pass

  async def upload_local_file(self, file_path: str, folder: str = "") -> FileNode:
    """Upload a local file to the specified folder.

    Args:
        file_path (str): The path to the local file.
        folder (str, optional): The folder to upload the file to. Defaults to "".
        
    Returns:
        FileNode: The uploaded file node.
    """
    pass

  async def get_file(self, file_path: str) -> Tuple[Any, Any]:
      """Extract file content and mimetype from S3 storage

      Args:
          file_path (str): Path of the file in S3

      Returns:
          Tuple: File content and mimetype
      """
      pass

  async def list_files(self, folder: str) -> List[FileNode]:
    """List the files in the specified folder.

    Args:
        folder (str): The folder to list the files from.

    Returns:
        List[FileNode]: The list of file nodes in the folder.
    """
    pass

  async def path_exists(self, path: str) -> bool:
    """Check a file exists at the specified path.

    Args:
        path (str): The path to check.

    Returns:
        bool: True if the path exists, False otherwise.
    """
    pass

  async def copy_file(self, source_path: str, destination_path: str) -> bool:
    """Copy file from one location to another.

    Args:
        source_path (str): The source file path.
        destination_path (str): The destination file path.

    Returns:
        bool: True if the copy was successful, False otherwise.
    """
    pass

  async def move_file(self, source_path: str, destination_path: str) -> bool:
    """Move file from one location to another.

    Args:
        source_path (str): The source file path.
        destination_path (str): The destination file path.
    
    Returns:
        bool: True if the move was successful, False otherwise.
    """
    pass

  async def delete_file(self, file_path: str) -> bool:
    """Delete the file at the specified path.
    
    Args:
        file_path (str): The path to the file to delete.
    
    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    pass
  
class LocalFilesService(FilesService):
  """
  This service provides file-related operations on the local file system.
  """
  
  def __init__(self, base_path: str = "."):
    """Initialize the local files service with a base path.
    
    Args:
        base_path (str): The base path for file operations. Defaults to current directory.
    """
    self.base_path = Path(base_path).resolve()
    self.base_path.mkdir(parents=True, exist_ok=True)
  
  def _get_full_path(self, path: str) -> Path:
    """Get the full path by joining with base path.
    
    Args:
        path (str): The relative path.
        
    Returns:
        Path: The full resolved path.
    """
    full_path = (self.base_path / path).resolve()
    # Ensure the path is within base_path (security check)
    if not str(full_path).startswith(str(self.base_path)):
      raise ValueError(f"Path {path} is outside the base path")
    return full_path
  
  async def upload_file(self, upload_file: UploadFile, folder: str = "") -> FileNode:
    """Upload a file to the specified folder.

    Args:
        upload_file (UploadFile): The file to upload.
        folder (str, optional): The folder to upload the file to. Defaults to "".
    
    Returns:
        FileNode: The uploaded file node.
    """
    # Create the target directory
    target_dir = self._get_full_path(folder)
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # Create the full file path
    file_path = target_dir / upload_file.filename
    
    # Write the file
    content = await upload_file.read()
    with open(file_path, "wb") as f:
      f.write(content)
    
    # Get file stats
    stat = file_path.stat()
    mime_type, _ = mimetypes.guess_type(str(file_path))
    
    # Create relative path for return
    rel_path = str(file_path.relative_to(self.base_path))
    
    return FileNode(
      name=file_path.name,
      path=rel_path,
      size=stat.st_size,
      mime_type=mime_type,
      is_file=True
    )
  
  async def upload_local_file(self, file_path: str, folder: str = "") -> FileNode:
    """Upload a local file to the specified folder.

    Args:
        file_path (str): The path to the local file.
        folder (str, optional): The folder to upload the file to. Defaults to "".
        
    Returns:
        FileNode: The uploaded file node.
    """
    source_path = Path(file_path)
    if not source_path.exists():
      raise FileNotFoundError(f"Source file {file_path} does not exist")
    
    # Create the target directory
    target_dir = self._get_full_path(folder)
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # Create the full file path
    destination_path = target_dir / source_path.name
    
    # Copy the file
    shutil.copy2(source_path, destination_path)
    
    # Get file stats
    stat = destination_path.stat()
    mime_type, _ = mimetypes.guess_type(str(destination_path))
    
    # Create relative path for return
    rel_path = str(destination_path.relative_to(self.base_path))
    
    return FileNode(
      name=destination_path.name,
      path=rel_path,
      size=stat.st_size,
      mime_type=mime_type,
      is_file=True
    )
  
  async def get_file(self, file_path: str) -> Tuple[Any, Any]:
    """Extract file content and mimetype from local storage

    Args:
        file_path (str): Path of the file

    Returns:
        Tuple: File content and mimetype
    """
    full_path = self._get_full_path(file_path)
    
    if not full_path.exists() or not full_path.is_file():
      raise FileNotFoundError(f"File {file_path} does not exist")
    
    # Read file content
    with open(full_path, "rb") as f:
      content = f.read()
    
    # Get mimetype
    mime_type, _ = mimetypes.guess_type(str(full_path))
    
    return content, mime_type
  
  async def list_files(self, folder: str) -> List[FileNode]:
    """List the files in the specified folder.

    Args:
        folder (str): The folder to list the files from.

    Returns:
        List[FileNode]: The list of file nodes in the folder.
    """
    target_dir = self._get_full_path(folder)
    
    if not target_dir.exists():
      return []
    
    if not target_dir.is_dir():
      raise ValueError(f"Path {folder} is not a directory")
    
    file_nodes = []
    
    for item in target_dir.iterdir():
      rel_path = str(item.relative_to(self.base_path))
      
      if item.is_file():
        stat = item.stat()
        mime_type, _ = mimetypes.guess_type(str(item))
        
        file_nodes.append(FileNode(
          name=item.name,
          path=rel_path,
          size=stat.st_size,
          mime_type=mime_type,
          is_file=True
        ))
      elif item.is_dir():
        file_nodes.append(FileNode(
          name=item.name,
          path=rel_path,
          is_file=False
        ))
    
    return file_nodes
  
  async def path_exists(self, path: str) -> bool:
    """Check a file exists at the specified path.

    Args:
        path (str): The path to check.

    Returns:
        bool: True if the path exists, False otherwise.
    """
    try:
      full_path = self._get_full_path(path)
      return full_path.exists()
    except (ValueError, Exception):
      logging.error(f"Error checking path existence for {path}")
      return False
  
  async def copy_file(self, source_path: str, destination_path: str) -> bool:
    """Copy file from one location to another.

    Args:
        source_path (str): The source file path.
        destination_path (str): The destination file path.

    Returns:
        bool: True if the copy was successful, False otherwise.
    """
    try:
      source = self._get_full_path(source_path)
      destination = self._get_full_path(destination_path)
      
      if not source.exists():
        raise FileNotFoundError(f"Source file {source_path} does not exist")
      
      # Create parent directory if it doesn't exist
      destination.parent.mkdir(parents=True, exist_ok=True)
      
      shutil.copy2(source, destination)
      return True
    except Exception as e:
      logging.error(f"Error copying file from {source_path} to {destination_path}: {e}")
      return False
  
  async def move_file(self, source_path: str, destination_path: str) -> bool:
    """Move file from one location to another.

    Args:
        source_path (str): The source file path.
        destination_path (str): The destination file path.
    
    Returns:
        bool: True if the move was successful, False otherwise.
    """
    try:
      source = self._get_full_path(source_path)
      destination = self._get_full_path(destination_path)
      
      if not source.exists():
        raise FileNotFoundError(f"Source file {source_path} does not exist")
      
      # Create parent directory if it doesn't exist
      destination.parent.mkdir(parents=True, exist_ok=True)
      
      shutil.move(source, destination)
      return True
    except Exception as e:
      logging.error(f"Error moving file from {source_path} to {destination_path}: {e}")
      return False
  
  async def delete_file(self, file_path: str) -> bool:
    """Delete the file at the specified path.
    
    Args:
        file_path (str): The path to the file to delete.
    
    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
      full_path = self._get_full_path(file_path)
      
      if not full_path.exists():
        return False
      
      if full_path.is_file():
        full_path.unlink()
      elif full_path.is_dir():
        shutil.rmtree(full_path)
      
      return True
    except Exception as e:
      logging.error(f"Error deleting file at {file_path}: {e}")
      return False