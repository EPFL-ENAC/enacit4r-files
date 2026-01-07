from typing import List, Tuple, Any
from fastapi.datastructures import UploadFile
from ..models.files import FileNode
from ..utils.files import FileNodeBuilder
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
  
  def _dump_file_node(self, file_node: FileNode, file_path: Path):
    """Dump a FileNode to a JSON file.

    Args:
        file_node (FileNode): The file node to dump.
        file_path (Path): The path to the reference file.
    """
    json_path = file_path.with_suffix(file_path.suffix + ".meta")
    with open(json_path, "w") as f:
      f.write(file_node.model_dump_json())
  
  def _read_file_node(self, file_path: Path) -> FileNode:
    """Read a FileNode from a JSON file.

    Args:
        file_path (Path): The path to the reference file.
    Returns:
        FileNode: The loaded file node.
    """
    json_path = file_path.with_suffix(file_path.suffix + ".meta")
    with open(json_path, "r") as f:
      json_content = f.read()
    file_node = FileNode.model_validate_json(json_content)
    return file_node
  
  def _delete_file_node(self, file_path: Path):
    """Delete the metadata file associated with a file.

    Args:
        file_path (Path): The path to the reference file.
    """
    json_path = file_path.with_suffix(file_path.suffix + ".meta")
    if json_path.exists():
      json_path.unlink()
  
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
    
    node = FileNode(
      name=file_path.name,
      path=rel_path,
      size=size,
      mime_type=mime_type,
      is_file=True
    )
    
    # Dump node in metadata file
    self._dump_file_node(node, file_path)
    
    return node
  
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
    
    node = FileNode(
      name=destination_path.name,
      path=rel_path,
      size=stat.st_size,
      mime_type=mime_type,
      is_file=True
    )
    
    # Dump node in metadata file
    self._dump_file_node(node, destination_path)
    
    return node
  
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
      if item.suffix == ".meta":
        continue  # Skip metadata files
      
      rel_path = item.relative_to(self.base_path).as_posix()
      
      # List meta files only as part of the associated file
      if item.is_file():
        # Read associated file node 
        node = self._read_file_node(item)
        file_nodes.append(node)
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
      
      # Read metadata from source and write to destination
      try:
        node = self._read_file_node(source)
        # Create new node for destination
        node.path = destination_path
        self._dump_file_node(node, destination)
      except Exception as e:
        logging.warning(f"Could not copy metadata for {source_path} to {destination_path}: {e}")
      
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
      
      # Move metadata from source to destination
      try:
        node = self._read_file_node(source)
        # Update path in node
        node.path = destination_path
        self._dump_file_node(node, destination)
        # Remove old metadata file
        self._delete_file_node(source)
      except Exception as e:
        logging.warning(f"Could not move metadata for {source_path} to {destination_path}: {e}")
      
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
        self._delete_file_node(full_path)
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
  
  async def _dump_file_node(self, file_node: FileNode, folder: str):
    """Dump a FileNode to a JSON file in S3.
    Args:
        file_node (FileNode): The file node to dump.
        folder (str): The folder in S3 to dump the file node to.
    """
    json_name = f"{file_node.name}.meta"
    # Make temp directory and and dump json file
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir) / json_name
      with open(temp_path, "w") as f:
        f.write(file_node.model_dump_json())
      # Upload to S3
      s3_folder = folder.rstrip("/") if folder else ""
      with open(temp_path, "rb") as f:
        await self.s3_service.upload_local_file(str(temp_path.parent), json_name, s3_folder)
  
  async def _read_file_node(self, file_key: str) -> FileNode:
    """Read a FileNode from a JSON file in S3.
    Args:
        file_key (str): The S3 key of the reference file.
    Returns:
        FileNode: The loaded file node if available, otherwise None.
    """
    json_key = f"{file_key}.meta" if not file_key.endswith(".meta") else file_key
    json_content = await self.s3_service.get_file(json_key)
    if json_content is not False:
      file_node = FileNode.model_validate_json(json_content.decode("utf-8"))
      return file_node
    return None
  
  async def _delete_file_node(self, file_key: str):
    """Delete the metadata file associated with a file in S3.
    Args:
        file_key (str): The S3 key of the reference file.
    """
    json_key = f"{file_key}.meta" if not file_key.endswith(".meta") else file_key
    await self.s3_service.delete_file(json_key)
  
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
    node = FileNodeBuilder.from_ref(file_ref).build()
    node.size = size  # Use original size before encryption
    
    # Dump file metadata in S3
    await self._dump_file_node(node, folder)
    
    return node

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
    node = FileNodeBuilder.from_ref(file_ref).build()
    node.size = size  # Use original size before encryption
    
    # Dump file metadata in S3
    await self._dump_file_node(node, folder)
    
    return node

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
        if item_name.endswith(".meta"):
          continue  # Skip metadata files
        if item_name not in seen_items:
          seen_items.add(item_name)
          # Get file details from associate metadata file
          try:
            node = await self._read_file_node(key)
            if node:
              file_nodes.append(node)
          except Exception as e:
            logging.warning(f"Could not read metadata for {key}: {e}")
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
      if result is not False:
        # Copy metadata file as well
        try:
          node = await self._read_file_node(source_path)
          if node:
            node.path = destination_path
            await self._dump_file_node(node, os.path.dirname(destination_path))
        except Exception as e:
          logging.warning(f"Could not copy metadata for {source_path} to {destination_path}: {e}")
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
      if result is not False:
        # Move metadata file as well
        try:
          node = await self._read_file_node(source_path)
          if node:
            node.path = destination_path
            await self._dump_file_node(node, os.path.dirname(destination_path))
            # Delete old metadata file
            await self._delete_file_node(source_path)
        except Exception as e:
          logging.warning(f"Could not move metadata for {source_path} to {destination_path}: {e}")
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
      if result is not False:
        await self._delete_file_node(file_path)
      return result is not False
    except Exception as e:
      logging.error(f"Error deleting file at {file_path}: {e}")
      return False