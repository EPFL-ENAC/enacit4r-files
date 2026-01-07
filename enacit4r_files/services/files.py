from typing import List, Tuple, Any
from fastapi.datastructures import UploadFile
from ..models.files import FileNode
from .s3 import S3Service
import logging
import shutil
import mimetypes
import os
import tempfile
import urllib.parse
from pathlib import Path
from cryptography.fernet import Fernet
from io import BytesIO

class FilesStore:
  """
  This service provides file-related operations. It is an abstraction layer
  over different storage backends such as S3 or local file system.
  """
  
  def __init__(self, key: bytes = None):
    """Initialize the files service."""
    self.fernet = Fernet(key) if key else None
  
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
    """Extract file content and mimetype from storage.

    Args:
        file_path (str): Path of the file in the storage backend.

    Returns:
        Tuple: File content and mimetype.
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
  
  def encrypt_content(self, content: bytes) -> bytes:
    """Encrypt file content, if encryption is enabled.

    Args:
        content (bytes): The file content to encrypt.
    Returns:
        bytes: The encrypted content.
    """
    if not self.fernet:
      return content
    encrypted_content = self.fernet.encrypt(content)
    return encrypted_content
  
  def decrypt_content(self, encrypted_content: bytes) -> bytes:
    """Decrypt file content, if encryption is enabled.

    Args:
        encrypted_content (bytes): The encrypted file content.

    Returns:
        bytes: The decrypted content.
    """
    if not self.fernet:
      return encrypted_content
    decrypted_content = self.fernet.decrypt(encrypted_content)
    return decrypted_content
  
class LocalFilesStore(FilesStore):
  """
  This service provides file-related operations on the local file system.
  """
  
  def __init__(self, base_path: str = ".", key: bytes = None):
    """Initialize the local files service with a base path.
    
    Args:
        base_path (str): The base path for file operations. Defaults to current directory.
        key (bytes, optional): The encryption key. Defaults to None.
    """
    super().__init__(key=key)
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
    if hasattr(full_path, "is_relative_to"):
      if not full_path.is_relative_to(self.base_path):
        raise ValueError(f"Path {path} is outside the base path")
    else:
      try:
        full_path.relative_to(self.base_path)
      except ValueError:
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
    
    # Read the file content
    content = await upload_file.read()
    
    # Get file stats from the original content (before encryption)
    size = len(content)
    mime_type, _ = mimetypes.guess_type(str(file_path))
    
    # Encrypt content if needed
    content_to_write = self.encrypt_content(content)
    
    # Write the file (only once)
    with open(file_path, "wb") as f:
      f.write(content_to_write)
    
    # Create relative path for return
    rel_path = file_path.relative_to(self.base_path).as_posix()
    
    return FileNode(
      name=file_path.name,
      path=rel_path,
      size=size,
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
    
    # Get file stats from source
    stat = source_path.stat()
    mime_type, _ = mimetypes.guess_type(str(source_path))
    
    # Copy the file
    if self.fernet:
      with open(source_path, "rb") as f:
        content = f.read()
      encrypted_content = self.encrypt_content(content)
      with open(destination_path, "wb") as f:
        f.write(encrypted_content)
    else:
      shutil.copy2(source_path, destination_path)
    
    # Create relative path for return
    rel_path = destination_path.relative_to(self.base_path).as_posix()
    
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
      content = self.decrypt_content(f.read())
    
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
      rel_path = item.relative_to(self.base_path).as_posix()
      
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
    except (ValueError, Exception) as e:
      logging.error(f"Error checking path existence for {path}: {e}")
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
    
class S3FilesStore(FilesStore):
  """
  This service provides file-related operations on a S3 storage backend.
  """
  
  def __init__(self, s3_service: S3Service, key: bytes = None):
    """Initialize the files service."""
    super().__init__(key=key)
    self.s3_service = s3_service
  
  async def upload_file(self, upload_file: UploadFile, folder: str = "") -> FileNode:
    """Upload a file to the specified folder.

    Args:
        upload_file (UploadFile): The file to upload.
        folder (str, optional): The folder to upload the file to. Defaults to "".
    
    Returns:
        FileNode: The uploaded file node.
    """
    # Read and optionally encrypt the content
    content = await upload_file.read()
    size = len(content)
    encrypted_content = self.encrypt_content(content)
    
    # Create a new UploadFile with encrypted content
    encrypted_file = UploadFile(
      filename=upload_file.filename,
      file=BytesIO(encrypted_content),
      content_type=upload_file.content_type
    )
    
    # Upload to S3
    file_ref = await self.s3_service.upload_file(encrypted_file, folder)
    
    # Convert FileRef to FileNode
    return FileNode(
      name=file_ref.name,
      path=file_ref.path,
      size=size,  # Use original size before encryption
      mime_type=file_ref.mime_type,
      alt_name=file_ref.alt_name,
      alt_path=file_ref.alt_path,
      alt_size=file_ref.alt_size,
      alt_mime_type=file_ref.alt_mime_type,
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
    
    parent_path = str(source_path.parent)
    relative_path = source_path.name
    
    # Get original file size before encryption
    stat = source_path.stat()
    size = stat.st_size
    
    # If encryption is enabled, we need to encrypt the file first
    if self.fernet:
      with open(source_path, "rb") as f:
        content = f.read()
      encrypted_content = self.encrypt_content(content)
      
      # Write encrypted content to a temporary file
      with tempfile.NamedTemporaryFile(delete=False, suffix=source_path.suffix) as temp_file:
        temp_file.write(encrypted_content)
        temp_path = temp_file.name
      
      try:
        # Upload the temporary encrypted file
        file_ref = await self.s3_service.upload_local_file(os.path.dirname(temp_path), os.path.basename(temp_path), folder)
      finally:
        # Clean up temporary file
        os.unlink(temp_path)
    else:
      # Upload directly without encryption
      file_ref = await self.s3_service.upload_local_file(parent_path, relative_path, folder)
    
    # Convert FileRef to FileNode
    return FileNode(
      name=file_ref.name,
      path=file_ref.path,
      size=size,  # Use original size before encryption
      mime_type=file_ref.mime_type,
      alt_name=file_ref.alt_name,
      alt_path=file_ref.alt_path,
      alt_size=file_ref.alt_size,
      alt_mime_type=file_ref.alt_mime_type,
      is_file=True
    )

  async def get_file(self, file_path: str) -> Tuple[Any, Any]:
    """Extract file content and mimetype from storage.

    Args:
        file_path (str): Path of the file in the storage backend.

    Returns:
        Tuple: File content and mimetype.
    """
    content, mime_type = await self.s3_service.get_file(file_path)
    
    if content is False:
      raise FileNotFoundError(f"File {file_path} does not exist")
    
    # Decrypt content if encryption is enabled
    decrypted_content = self.decrypt_content(content)
    
    return decrypted_content, mime_type

  async def list_files(self, folder: str) -> List[FileNode]:
    """List the files in the specified folder.

    Args:
        folder (str): The folder to list the files from.

    Returns:
        List[FileNode]: The list of file nodes in the folder.
    """
    
    # List all keys in the folder
    keys = await self.s3_service.list_files(folder)
    
    file_nodes = []
    folder_key = self.s3_service.to_s3_key(folder)
    if not folder_key.endswith("/"):
      folder_key += "/"
    
    # Track unique immediate children
    seen_items = set()
    
    for key in keys:
      # Get relative path from folder
      if key.startswith(folder_key):
        relative_path = key[len(folder_key):]
      else:
        relative_path = key
      
      # Check if this is a direct child or nested
      path_parts = relative_path.split("/")
      
      if len(path_parts) == 1 and path_parts[0]:  # Direct file
        item_name = path_parts[0]
        if item_name not in seen_items:
          seen_items.add(item_name)
          # Get file details
          try:
            content, mime_type = await self.s3_service.get_file(key)
            size = len(content) if content else 0
            file_nodes.append(FileNode(
              name=item_name,
              path=urllib.parse.unquote(key),
              size=size,
              mime_type=mime_type,
              is_file=True
            ))
          except Exception as e:
            logging.error(f"Error getting file details for {key}: {e}")
      elif len(path_parts) > 1 and path_parts[0]:  # Folder
        folder_name = path_parts[0]
        if folder_name not in seen_items:
          seen_items.add(folder_name)
          folder_path = f"{folder}/{folder_name}" if folder else folder_name
          file_nodes.append(FileNode(
            name=folder_name,
            path=folder_path,
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
    return await self.s3_service.path_exists(path)

  async def copy_file(self, source_path: str, destination_path: str) -> bool:
    """Copy file from one location to another.

    Args:
        source_path (str): The source file path.
        destination_path (str): The destination file path.

    Returns:
        bool: True if the copy was successful, False otherwise.
    """
    try:
      result = await self.s3_service.copy_file(source_path, destination_path)
      return result is not False
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
      result = await self.s3_service.move_file(source_path, destination_path)
      return result is not False
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
      result = await self.s3_service.delete_file(file_path)
      return result is not False
    except Exception as e:
      logging.error(f"Error deleting file at {file_path}: {e}")
      return False