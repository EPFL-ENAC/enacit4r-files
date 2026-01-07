from typing import List, Tuple, Any
from fastapi.datastructures import UploadFile
from ..models.files import FileNode
from cryptography.fernet import Fernet

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
