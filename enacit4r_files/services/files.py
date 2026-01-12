import re
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
    self.sanitization_regex = re.compile(r'^[\w/ .()\[\]:\-\'<>?]+$')
    self.meta_extension = ".meta.json"
  
  async def write_file(self, upload_file: UploadFile, folder: str = "") -> FileNode:
    """Write an uploaded file to the specified folder.

    Args:
        upload_file (UploadFile): The uploaded file to write.
        folder (str, optional): The folder to write the file to. Defaults to "".
    
    Returns:
        FileNode: The uploaded file node.
    """
    pass

  async def write_local_file(self, file_path: str, folder: str = "") -> FileNode:
    """Write a local file to the specified folder.

    Args:
        file_path (str): The path to the local file.
        folder (str, optional): The folder to write the file to. Defaults to "".
        
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

  async def list_files(self, folder: str, recursive: bool = False) -> List[FileNode]:
    """List the files in the specified folder.

    Args:
        folder (str): The folder to list the files from.
        recursive (bool, optional): Whether to list files recursively. Defaults to False.

    Returns:
        List[FileNode]: The list of file nodes in the folder.
    """
    pass

  async def file_exists(self, path: str) -> bool:
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

  def set_sanitization_regex(self, pattern: str):
    """Set a custom regex pattern for path sanitization.

    Args:
        pattern (str): The regex pattern to use for sanitization.
    
    Raises:
        ValueError: If the provided pattern is not a valid regular expression.
    """
    try:
      self.sanitization_regex = re.compile(pattern)
    except re.error as exc:
      raise ValueError(f"Invalid sanitization regex pattern: {pattern!r}") from exc

  def sanitize_path(self, path: str) -> str:
    """Sanitize a file path string to prevent directory traversal and reject unsafe characters.
    This method performs the following steps:
    * Removes all leading forward slashes (``/``) from the path.
    * Removes all newline (``\\n``) and carriage-return (``\\r``) characters from the path.
    * Rejects paths containing the substring ``".."`` to avoid directory traversal.
    * Validates the resulting (possibly empty) path against ``self.sanitization_regex`` when it is
      non-empty. By default, the allowed characters are those matched by the pattern
      ``^[a-zA-Z0-9/ _.()\\[\\]:-]+$`` (letters, digits, forward slashes, spaces, underscores,
      periods, parentheses, square brackets, colons, and hyphens), but this pattern can be
      customized via :meth:`set_sanitization_regex`.
    Args:
        path (str): The raw path string to sanitize.
    Raises:
        ValueError: If path is None.
        ValueError: If the path contains ``".."``.
        ValueError: If the non-empty sanitized path contains characters not allowed by
            ``self.sanitization_regex``.
    Returns:
        str: The sanitized path string (which may be empty) after all transformations
        have been applied.
    """
    # Check for None input
    if path is None:
        raise ValueError("Invalid path: path cannot be None")
    # Remove leading/trailing slashes
    path = path.strip("/")
    # Remove \n and \r characters
    path = path.replace("\n", "").replace("\r", "")
    # Check for '..' as a path component (directory traversal)
    # Split by '/' and check if any component is exactly '..'
    if any(component == ".." for component in path.split("/")):
        raise ValueError("Invalid path: '..' not allowed")
    if path and not self.sanitization_regex.match(path):
        raise ValueError("Invalid path: contains forbidden characters")
    return path

  def sanitize_file_name(self, file_name: str) -> str:
    """Sanitize only the file name part of a path.

    Args:
        file_name (str): The raw file name string to sanitize.

    Raises:
        ValueError: If file_name is None.
        ValueError: If the sanitized file name contains forbidden characters.

    Returns:
        str: The sanitized file name.
    """
    # Check for None input
    if file_name is None:
        raise ValueError("Invalid file name: file name cannot be None")
    # Remove \n and \r characters
    file_name = file_name.replace("\n", "").replace("\r", "")
    # Allow empty file names
    if file_name and not self.sanitization_regex.match(file_name):
        raise ValueError("Invalid file name: contains forbidden characters")
    # Do not allow path separators in file names
    if "/" in file_name:
        raise ValueError("Invalid file name: path separators not allowed")
    return file_name