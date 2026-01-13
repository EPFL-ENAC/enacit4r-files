from typing import List, Tuple, Any
from aiobotocore.session import get_session
from botocore.config import Config
from io import BytesIO
from fastapi.datastructures import UploadFile
from starlette.datastructures import Headers
from PIL import Image
from ..utils.files import FileNodeBuilder, image_mimetypes
from ..models.files import FileRef, FileNode
from .files import FilesStore
import logging
import os
import urllib.parse
import mimetypes
import tempfile
from pathlib import Path

class S3Error(Exception):
    """Exception raised when managing S3 files."""
    pass

class S3Service(object):

    def __init__(self, s3_endpoint_url: str, s3_access_key_id: str, s3_secret_access_key: str, region: str, bucket: str, path_prefix: str, with_checksums: bool = False):
        """Initiate the S3 service.

        Args:
            s3_endpoint_url (str): The endpoint URL of the S3 service.
            s3_access_key_id (str): The access key ID for S3 authentication.
            s3_secret_access_key (str): The secret access key for S3 authentication.
            region (str): The AWS region where the S3 bucket is located.
            bucket (str): The name of the S3 bucket.
            path_prefix (str): The prefix path within the S3 bucket.
            with_checksums (bool, optional): Whether to enable checksum handling. When False (default), 
            checksum use is disabled for compatibility with S3-compatible services that do not support checksums.
        """
        self.s3_endpoint_url = s3_endpoint_url
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.region = region
        self.path_prefix = path_prefix if path_prefix.endswith("/") else f"{path_prefix}/"
        self.bucket = bucket
        self.with_checksums = with_checksums

    def to_s3_path(self, file_path: str) -> str:
        """Ensure that file path starts with path prefix.

        Args:
            file_path (str): The path to the file in the S3 bucket

        Returns:
            str: The full file path.
        """
        if not file_path.startswith(self.path_prefix):
            return f"{self.path_prefix}{file_path}"
        return file_path

    def to_s3_key(self, file_path: str) -> str:
        """Make sure file path is a valid S3 key.

        Args:
            file_path (str): The path to the file in the S3 bucket

        Returns:
            str: The full file path, ready to be used in S3 queries.
        """
        return urllib.parse.unquote(self.to_s3_path(file_path))

    async def path_exists(self, file_path: str) -> bool:
        """Check if file exists in S3 storage

        Args:
            file_path (str): Path of the file in S3

        Returns:
            bool: True if file exists, False otherwise
        """
        key = self.to_s3_key(file_path)

        # check if file_path exists
        async with self._create_client() as client:
            try:
                response = await client.head_object(
                    Bucket=self.bucket, Key=key)
                if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    return True
            except Exception as e:
                return False
        return False

    async def list_files(self, folder_path: str) -> List[str]:
        """List files in a folder in S3 storage

        Args:
            folder_path (str): Path of the folder in S3
            
        Returns:
            List[str]: An array of S3 file keys.
        """
        key = self.to_s3_key(folder_path)

        keys = []
        # list files in folder_path
        async with self._create_client() as client:
            paginator = client.get_paginator('list_objects_v2')
            async for page in paginator.paginate(Bucket=self.bucket, Prefix=key):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        keys.append(obj['Key'])
        return keys

    async def get_file(self, file_path: str) -> Tuple[Any, Any]:
        """Extract file content and mimetype from S3 storage

        Args:
            file_path (str): Path of the file in S3

        Returns:
            Tuple[Any, Any]: File content and mimetype
        """
        key = self.to_s3_key(file_path)

        # get file from file path
        async with self._create_client() as client:
            try:
                response = await client.get_object(
                    Bucket=self.bucket, Key=key)
                if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    # Read the content of the S3 object
                    file_content = await response['Body'].read()
                    return file_content, response["ContentType"]
            except Exception as e:
                return False, False
        return False, False

    async def upload_local_file(self, parent_path, file_path: str, s3_folder: str = "", mime_type: str = None) -> FileRef:
        """Upload local file to S3 storage

        Args:
            parent_path (str): Parent path of the file
            file_path (str): Path to local file relative to parent path
            s3_folder (str, optional): Relative parent folder in S3. Defaults to "".
            mime_type (str, optional): MIME type of the file. Defaults to None.

        Returns:
            FileRef: S3 upload reference
        """

        content_type =  mime_type if mime_type is not None else self._get_mime_type(file_path)
        if content_type in image_mimetypes:
            return await self._upload_local_image(parent_path, file_path, s3_folder)
        return await self._upload_local_file(parent_path, file_path, s3_folder, mime_type=content_type)

    async def upload_file(self, upload_file: UploadFile, s3_folder: str = "") -> FileRef:
        """Upload file to S3 storage

        Args:
            upload_file (UploadFile): UploadFile object
            s3_folder (str, optional): Relative parent folder in S3. Defaults to "".

        Returns:
            FileRef: S3 upload reference
        """
        # if mimetype is image upload image
        if upload_file.content_type in image_mimetypes:
            return await self._upload_image(upload_file, s3_folder)
        return await self._upload_file(upload_file, s3_folder)

    async def move_file(self, file_path: str, destination_path: str) -> Any:
        """Move a file from one location to another in the same S3 storage

        Args:
            file_path (str): Path of the file in S3
            destination_path (str): Destination path in S3

        Returns:
            Any: File S3 key if deleted, False otherwise
        """
        source_key = self.to_s3_key(file_path)

        destination_key = self.to_s3_key(destination_path)

        # copy to new location and delete source
        res = await self.copy_file(source_key, destination_key)
        if res is not False:
            await self.delete_file(source_key)
        return res

    async def copy_file(self, file_path: str, destination_path: str) -> Any:
        """Copy a file from one location to another in the same S3 storage

        Args:
            file_path (str): Path of the file in S3
            destination_path (str): Destination path in S3

        Returns:
            Any: File S3 key if deleted, False otherwise
        """
        source_key = self.to_s3_key(file_path)

        destination_key = self.to_s3_key(destination_path)

        # copy file_path to new location
        async with self._create_client() as client:
            response = await client.copy_object(
                Bucket=self.bucket,
                CopySource={'Bucket': self.bucket, 'Key': source_key},
                ACL="public-read",
                Key=destination_key)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logging.info(
                    f"File copied path : {self.s3_endpoint_url}/{self.bucket}/{destination_key}")
                return destination_key
        return False

    async def delete_file(self, file_path: str) -> Any:
        """Delete file from S3 storage

        Args:
            file_path (str): Path of the file in S3

        Returns:
            Any: File path if deleted, False otherwise
        """
        key = self.to_s3_key(file_path)

        # delete file_path
        async with self._create_client() as client:
            response = await client.delete_object(
                Bucket=self.bucket, Key=key)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                logging.info(
                    f"File deleted path : {self.s3_endpoint_url}/{self.bucket}/{key}")
                return key
        return False

    async def delete_files(self, file_path: str) -> Any:
        """Delete files recursively from S3 storage

        Args:
            file_path (str): Path of the file in S3

        Returns:
            Any: File path if deleted, False otherwise
        """
        folder_key = self.to_s3_key(file_path)

        # delete file_path
        async with self._create_client() as client:
            # delete content, if any
            paginator = client.get_paginator('list_objects_v2')
            async for result in paginator.paginate(Bucket=self.bucket, Prefix=folder_key):
                for content in result.get('Contents', []):
                    object_key = content['Key']
                    response = await client.delete_object(Bucket=self.bucket, Key=object_key)
                    if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                        logging.info(
                            f"File deleted path : {self.s3_endpoint_url}/{self.bucket}/{object_key}")

            # delete object
            response = await client.delete_object(Bucket=self.bucket, Key=folder_key)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                logging.info(
                    f"File deleted path : {self.s3_endpoint_url}/{self.bucket}/{folder_key}")
                return folder_key
        return False

    #
    # Private methods
    #
    
    def _create_client(self):
        """Create an S3 client using the provided credentials and endpoint URL.

        Returns:
            Any: The S3 client.
        """
        settings = {
            'payload_signing_enabled': False,
            'use_accelerate_endpoint': False,
            'addressing_style': 'path'
        }
        if not self.with_checksums:
            # Completely disable checksums for S3-compatible services that don't support them
            settings['checksum_mode'] = 'DISABLED'
            settings['request_checksum_calculation'] = 'when_required'
            settings['response_checksum_validation'] = 'when_required'
        config = Config(
            s3=settings,
            signature_version='s3v4',
            disable_request_compression=True
        )
            
        session = get_session()
        return session.create_client(
            's3',
            region_name=self.region,
            endpoint_url=self.s3_endpoint_url,
            aws_secret_access_key=self.s3_secret_access_key,
            aws_access_key_id=self.s3_access_key_id,
            config=config)

    async def _convert_image(self, upload_file: UploadFile) -> Tuple[BytesIO, BytesIO]:
        """Convert an image to webp format

        Args:
            upload_file (UploadFile): UploadFile object

        Returns:
            Tuple[BytesIO, BytesIO]: Data of webp image and data of original image
        """
        request_object_content = await upload_file.read()
        origin_BytesIo = BytesIO(request_object_content)
        image = Image.open(origin_BytesIo)
        data = BytesIO()
        image.save(data, format="webp", quality=60)
        return (data, origin_BytesIo)

    async def _get_unique_filename(self,
                                   filename: str,
                                   ext: str = "",
                                   s3_folder: str = "") -> Tuple[str, str]:
        """Get unique file path in S3 and file name, change extension if one is provided

        Args:
            filename (str): File name
            ext (str, optional): New file extension. Defaults to "".
            s3_folder (str, optional): Relative parent folder in S3. Defaults to "".

        Returns:
            Tuple[str, str]: File path in S3 and new or original file name
        """
        split_file_name = os.path.splitext(filename)
        ext = ext if ext != "" else split_file_name[1]
        name = split_file_name[0]
        return (f"{s3_folder}/{name}{ext}", f"{name}{ext}")

    async def _upload_image(self, upload_file: UploadFile, s3_folder: str = "") -> FileRef:
        """Upload image to S3, convert to webp if necessary

        Args:
            upload_file (UploadFile): UploadFile object
            s3_folder (str, optional): Relative parent folder in S3. Defaults to "".

        Raises:
            S3Error: When S3 upload fails

        Returns:
            FileRef: S3 upload reference
        """
        if upload_file.content_type == "image/webp":
            # no need to convert to webp
            return await self._upload_file(upload_file, s3_folder)
        else:
            # convert to bytes
            (data, origin_data) = await self._convert_image(upload_file)

            # Webp converted image
            mimetype = "image/webp"
            (unique_file_name, name) = await self._get_unique_filename(upload_file.filename, ".webp", s3_folder=s3_folder)

            key = f"{self.path_prefix}{unique_file_name}"
            uploads3 = await self._upload_fileobj(bucket=self.bucket,
                                                  key=key,
                                                  data=data.getvalue(),
                                                  mimetype=mimetype)

            if not uploads3:
                raise S3Error("Failed to upload image to S3")

            # Original image
            (unique_alt_file_name, alt_name) = await self._get_unique_filename(upload_file.filename, s3_folder=s3_folder)
            alt_key = f"{self.path_prefix}{unique_alt_file_name}"
            alt_uploads3 = await self._upload_fileobj(
                bucket=self.bucket,
                key=alt_key,
                data=origin_data.getvalue(),
                mimetype=upload_file.content_type)

            if not alt_uploads3:
                raise S3Error("Failed to upload image to S3")

            # response http to be used by the frontend
            return FileRef(
                    name=name,
                    path=urllib.parse.quote(key),
                    size=uploads3,
                    mime_type=mimetype,
                    alt_name=alt_name,
                    alt_path=urllib.parse.quote(alt_key),
                    alt_size=alt_uploads3,
                    alt_mime_type=upload_file.content_type
                )

    async def _upload_file(self, upload_file: UploadFile, s3_folder: str = "") -> FileRef:
        """Upload file to S3, as is

        Args:
            upload_file (UploadFile): UploadFile object
            s3_folder (str, optional): Relative parent folder in S3. Defaults to "".

        Raises:
            S3Error: When S3 upload fails

        Returns:
            FileRef: S3 upload reference
        """
        (filename, name) = await self._get_unique_filename(upload_file.filename, s3_folder=s3_folder)
        key = f"{self.path_prefix}{filename}"
        uploads3 = await self._upload_fileobj(bucket=self.bucket,
                                              key=key,
                                              data=getattr(upload_file.file, '_file', upload_file.file),
                                              mimetype=upload_file.content_type)
        if uploads3:
            # response http to be used by the frontend
            return FileRef(
                name=name,
                path=urllib.parse.quote(key),
                size=uploads3,
                mime_type=upload_file.content_type
            )
        else:
            raise S3Error("Failed to upload file to S3")

    async def _upload_local_image(self, parent_path, file_path: str, s3_folder: str = "") -> FileRef:
        """Upload local image to S3, convert to webp if necessary

        Args:
            parent_path (str): Parent path of the file
            file_path (str): Path to local file relative to parent path
            s3_folder (str, optional): Relative parent folder in S3. Defaults to "".

        Raises:
            S3Error: When S3 upload fails

        Returns:
            FileRef: S3 upload reference
        """
        if file_path.endswith(".webp"):
            # no need to convert to webp
            return await self._upload_local_file(parent_path, file_path, s3_folder)
        else:
            alt_info = None
            try:
                # convert to webp
                file_path_alt = self._convert_image_file(
                    parent_path, file_path)

                # upload converted file
                alt_info = await self._upload_local_file(
                    parent_path, file_path_alt, s3_folder)
            except Exception as e:
                logging.error(e)

            # Original file
            orig_info = await self._upload_local_file(
                parent_path, file_path, s3_folder)

            # response http to be used by the frontend
            return FileRef(
                name=orig_info["name"],
                path=orig_info["path"],
                size=orig_info["size"],
                mime_type=orig_info["mime_type"],
                alt_name=alt_info["name"],
                alt_path=alt_info["path"],
                alt_size=alt_info["size"],
                alt_mime_type=alt_info["mime_type"]
             ) if alt_info else orig_info

    def _convert_image_file(self, parent_path: str, file_path: str) -> str:
        split_file_path = os.path.splitext(file_path)
        file_path_webp = f"{split_file_path[0]}.webp"
        input_file = os.path.join(parent_path, file_path)
        output_file = os.path.join(parent_path, file_path_webp)

        Image.open(input_file).save(output_file, format="webp", quality=60)

        return file_path_webp

    async def _upload_local_file(self, parent_path, file_path: str, s3_folder: str = "", mime_type: str = None) -> FileRef:
        """Upload file to S3, as is

        Args:
            parent_path (str): Parent path of the file
            file_path (str): Path to local file relative to parent path
            s3_folder (str, optional): Relative parent folder in S3. Defaults to "".
            mime_type (str, optional): MIME type of the file. Defaults to None.

        Raises:
            S3Error: When S3 upload fails

        Returns:
            FileRef: S3 upload reference
        """

        (filename, name) = await self._get_unique_filename(file_path, s3_folder=s3_folder)
        key = f"{self.path_prefix}{filename}"
        if mime_type is None:
            mime_type = self._get_mime_type(file_path)
        with open(os.path.join(parent_path, file_path), 'rb') as file:
            uploads3 = await self._upload_fileobj(bucket=self.bucket,
                                                  key=key,
                                                  data=file,
                                                  mimetype=mime_type)
        if uploads3:
            # response http to be used by the frontend
            return FileRef(
                name=name,
                path=urllib.parse.quote(key),
                size=uploads3,
                mime_type=mime_type
            )
        else:
            raise S3Error("Failed to upload file to S3")

    async def _upload_fileobj(self, data: BytesIO, bucket: str, key: str, mimetype: str) -> bool:
        """Perform the data upload to S3

        Args:
            data (BytesIO): Data to be uploaded
            bucket (str): Destination bucket
            key (str): Path of the obejct in the bucket
            mimetype (str): Object mimetype

        Returns:
            bool: True if upload was successful, the object size in bytes otherwise
        """
        async with self._create_client() as client:
            # Disable checksums for S3-compatible services that don't support them
            put_kwargs = {
                'Bucket': bucket,
                'Key': key,
                'Body': data,
                'ACL': 'public-read',
                'ContentType': mimetype
            }

            resp = await client.put_object(**put_kwargs)

            if resp["ResponseMetadata"][
                    "HTTPStatusCode"] == 200:
                logging.info(
                    f"File uploaded path : {self.s3_endpoint_url}/{bucket}/{key}")
                resp = await client.head_object(Bucket=bucket, Key=key)
                object_size = resp.get("ContentLength", 0)
                return object_size
        return False

    def _get_mime_type(self, file_name: str) -> str:
        """Guess the mime type from file name.

        Args:
            file_name (str): The file name.

        Returns:
            str: A standard mime type string.
        """
        mime_type, encoding = mimetypes.guess_type(file_name)
        if mime_type is None:
            if file_name.endswith('.webp'):
                mime_type = 'image/webp'
            else:
                mime_type = 'application/octet-stream'
        return mime_type


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
    json_name = f"{file_node.name}{self.meta_extension}"
    # Make temp directory and dump json file
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
    json_key = f"{file_key}{self.meta_extension}" if not file_key.endswith(self.meta_extension) else file_key
    json_content, _ = await self.s3_service.get_file(json_key)
    if json_content is not False:
      file_node = FileNode.model_validate_json(json_content.decode("utf-8"))
      return file_node
    return None
  
  async def _delete_file_node(self, file_key: str):
    """Delete the metadata file associated with a file in S3.
    Args:
        file_key (str): The S3 key of the reference file.
    """
    json_key = f"{file_key}{self.meta_extension}" if not file_key.endswith(self.meta_extension) else file_key
    await self.s3_service.delete_file(json_key)
  
  async def write_file(self, upload_file: UploadFile, folder: str = "") -> FileNode:
    """Write an uploaded file to the specified folder.

    Args:
        upload_file (UploadFile): The uploaded file to write.
        folder (str, optional): The folder to write the file to. Defaults to "".
    
    Returns:
        FileNode: The uploaded file node.
    """
    folder = self.sanitize_path(folder)
    # Read and optionally encrypt the content
    content = await upload_file.read()
    size = len(content)
    encrypted_content = self.encrypt_content(content)
    
    # Create a new UploadFile with encrypted content, preserving content type via headers
    headers = Headers({'content-type': upload_file.content_type or 'application/octet-stream'})
    encrypted_file = UploadFile(
      filename=self.sanitize_file_name(upload_file.filename),
      file=BytesIO(encrypted_content),
      headers=headers
    )
    
    # Upload to S3
    file_ref = await self.s3_service.upload_file(encrypted_file, folder)
    
    # Convert FileRef to FileNode
    node = FileNodeBuilder.from_ref(file_ref, self.s3_service.path_prefix).build()
    node.size = size  # Use original size before encryption
    
    # Dump file metadata in S3
    await self._dump_file_node(node, folder)
    
    return node

  async def write_local_file(self, file_path: str, folder: str = "") -> FileNode:
    """Write a local file to the specified folder.

    Args:
        file_path (str): The path to the local file.
        folder (str, optional): The folder to write the file to. Defaults to "".
        
    Returns:
        FileNode: The written file node.
    """
    folder = self.sanitize_path(folder)
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
    node = FileNodeBuilder.from_ref(file_ref, self.s3_service.path_prefix).build()
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

  async def list_files(self, folder: str, recursive: bool = False) -> List[FileNode]:
    """List the files in the specified folder.

    Args:
        folder (str): The folder to list the files from.
        recursive (bool, optional): Whether to list files recursively. Defaults to False.
    
    Returns:
        List[FileNode]: The list of file nodes in the folder.
    """
    folder = self.sanitize_path(folder)
    # List all keys in the folder
    keys = await self.s3_service.list_files(folder)
    
    file_nodes = []
    folder_key = self.s3_service.to_s3_key(folder)
    if not folder_key.endswith("/"):
      folder_key += "/"
    
    # Track unique immediate children
    seen_items = set()
    # Track directories for building hierarchy
    dir_nodes = {}  # path -> FileNode
    
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
        if item_name.endswith(self.meta_extension):
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
      elif len(path_parts) > 1 and path_parts[0]:  # Nested content
        if recursive:
          # Include all nested files recursively
          item_name = path_parts[-1]
          if item_name.endswith(self.meta_extension):
            continue  # Skip metadata files
          # Check if this is a file (not a folder marker ending with /)
          if item_name and not key.endswith("/"):
            if key not in seen_items:
              seen_items.add(key)
              # Get file details from associated metadata file
              try:
                node = await self._read_file_node(key)
                if node:
                  # Create all intermediate directories and build hierarchy
                  for i in range(len(path_parts) - 1):
                    dir_parts = path_parts[:i+1]
                    dir_name = dir_parts[-1]
                    dir_relative_path = "/".join(dir_parts)
                    dir_full_path = f"{folder}/{dir_relative_path}" if folder else dir_relative_path
                    
                    if dir_relative_path not in dir_nodes:
                      # Create new directory node
                      folder_node = FileNode(
                        name=dir_name,
                        path=dir_full_path,
                        is_file=False,
                        children=[]
                      )
                      dir_nodes[dir_relative_path] = folder_node
                      
                      # Add to parent or root
                      if i == 0:
                        # Top-level directory
                        file_nodes.append(folder_node)
                      else:
                        # Nested directory - add to parent
                        parent_path = "/".join(path_parts[:i])
                        if parent_path in dir_nodes:
                          dir_nodes[parent_path].children.append(folder_node)
                  
                  # Add file to its parent directory
                  if len(path_parts) > 1:
                    parent_dir_path = "/".join(path_parts[:-1])
                    if parent_dir_path in dir_nodes:
                      dir_nodes[parent_dir_path].children.append(node)
                  else:
                    file_nodes.append(node)
              except Exception as e:
                logging.warning(f"Could not read metadata for {key}: {e}")
        else:
          # Non-recursive: only add immediate subfolder
          folder_name = path_parts[0]
          if folder_name not in seen_items:
            seen_items.add(folder_name)
            folder_path = f"{folder}/{folder_name}" if folder else folder_name
            folder_node = FileNode(
              name=folder_name,
              path=folder_path,
              is_file=False
            )
            file_nodes.append(folder_node)
    
    return file_nodes

  async def file_exists(self, path: str) -> bool:
    """Check a file exists at the specified path.

    Args:
        path (str): The path to check.

    Returns:
        bool: True if the path exists, False otherwise.
    """
    path = self.sanitize_path(path)
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
      source_path = self.sanitize_path(source_path)
      destination_path = self.sanitize_path(destination_path)
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
      source_path = self.sanitize_path(source_path)
      destination_path = self.sanitize_path(destination_path)
      result = await self.s3_service.move_file(source_path, destination_path)
      if result is not False:
        # Move metadata file as well
        try:
          node = await self._read_file_node(source_path)
          if node:
            node.name = os.path.basename(destination_path)
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
    file_path = self.sanitize_path(file_path)
    try:
      result = await self.s3_service.delete_file(file_path)
      if result is not False:
        await self._delete_file_node(file_path)
      return result is not False
    except Exception as e:
      logging.error(f"Error deleting file at {file_path}: {e}")
      return False