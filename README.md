# ENAC-IT4R Python Files Utils

A Python library of files utils that are commonly used in the EPFL ENAC IT infrastructure:
 
 * `LocalFilesStore`: a service to upload, get, list, check, copy, move and delete files in a local file storage, with optional encryption support.
 * `S3FilesStore`: a service to upload, get, list, check, copy, move and delete files in a S3 file storage, with optional encryption support.
 * `S3Service`: a low-level service to upload, get, list, check, copy, move and delete files in a S3 file storage.
 * `FileChecker`: a class for checking the size of uploaded files.
 * `FileNodeBuilder`: a class to represent file references from S3 (`FileRef` class) a tree of file nodes (`FileNode` class) to facilitate the display of a folder in a web UI.
 
## Usage

To include the files library in your project:

```shell
# Using Poetry
poetry add git+https://github.com/EPFL-ENAC/enacit4r-files#someref
# Using uv
uv add git+https://github.com/EPFL-ENAC/enacit4r-files --tag tagname
```

Note: For Poetry, `someref` should be replaced by the commit hash, tag or branch name you want to use. For uv, see [UV Git dependencies documentation](https://docs.astral.sh/uv/concepts/projects/dependencies/#git).

## Development

See the Makefile for available commands.

## Services

The files management API is defined by the FilesStore interface, which is implemented by LocalFilesStore and S3FilesStore. Files can be optionally encrypted using Fernet symmetric encryption from the cryptography library.

Available methods:

* `write_file`: Write an uploaded file provided by FastAPI to file storage.
* `write_local_file`: Write a local file to file storage.
* `get_file`: Get a file content from file storage.
* `list_files`: List files from a "folder" in file storage.
* `file_exists`: Check if a file exists in file storage.
* `copy_file`: Copy a file in file storage.
* `move_file`: Move a file in file storage.
* `delete_file`: Delete a file in file storage.

### LocalFilesStore

Basic usage:

```python
from enacit4r_files.services import LocalFilesStore, FileNode
local_service = LocalFilesStore("/tmp/enacit4r_files")
# do something with local_service
```

Encryption usage:

```python
from enacit4r_files.services import LocalFilesStore, FileNode
from cryptography.fernet import Fernet
key = Fernet.generate_key()
local_service = LocalFilesStore("/tmp/enacit4r_files", key=key)
# do something with local_service
```

### S3FilesStore

Basic usage:

```python
from enacit4r_files.services import S3FilesStore, S3Service, S3Error, FileNode
s3_service = S3Service(config.S3_ENDPOINT_PROTOCOL + config.S3_ENDPOINT_HOSTNAME,
                           config.S3_ACCESS_KEY_ID,
                           config.S3_SECRET_ACCESS_KEY, 
                           config.S3_REGION,
                           config.S3_BUCKET,
                           config.S3_PATH_PREFIX)
s3_files_service = S3FilesStore(s3_service)
# do something with s3_files_service
```

Encryption usage:

```python
from enacit4r_files.services import S3FilesStore, S3Service, S3Error, FileNode
from cryptography.fernet import Fernet
key = Fernet.generate_key()
s3_service = S3Service(config.S3_ENDPOINT_PROTOCOL + config.S3_ENDPOINT_HOSTNAME,
                           config.S3_ACCESS_KEY_ID,
                           config.S3_SECRET_ACCESS_KEY, 
                           config.S3_REGION,
                           config.S3_BUCKET,
                           config.S3_PATH_PREFIX)
s3_files_service = S3FilesStore(s3_service, key=key)
# do something with s3_files_service
```

### S3Service

This is a low-level service to interact with S3 file storage. It is recommended to use `S3FilesStore` instead, which provides higher-level methods.

```python
from enacit4r_files.services import S3Service, S3Error, FileRef

s3_service = S3Service(config.S3_ENDPOINT_PROTOCOL + config.S3_ENDPOINT_HOSTNAME,
                     config.S3_ACCESS_KEY_ID,
                     config.S3_SECRET_ACCESS_KEY, 
                     config.S3_REGION,
                     config.S3_BUCKET,
                     config.S3_PATH_PREFIX)

try:
  # do something with s3_service
  pass
except S3Error as e:
    print(e)
```

## Tools

### FileChecker

```python
from enacit4r_files.tools.files import FileChecker

# Example using the default max file size
file_checker = FileChecker()

# Example usage with FastAPI
@router.post("/tmp",
             status_code=200,
             description="Upload any assets to S3 to a temporary folder",
             dependencies=[Depends(file_checker.check_size)])
async def upload_temp_files(
        files: list[UploadFile] = File(description="multiple file upload")):
        pass
```

### FileNodeBuilder

```python
from enacit4r_files.tools.files import FileNodeBuilder
from enacit4r_files.models.files import FileNode

builder = FileNodeBuilder.from_name("root")
# include a list of FileRef from S3
builder.add_files(file_refs)
root = builder.build()
```