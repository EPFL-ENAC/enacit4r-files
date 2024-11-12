# ENAC-IT4R Python Utils

A Python library of file utils that are commomly used in the EPFL ENAC IT infrastructure:
 
 * `S3Service`: a service to upload, get, list, check, copy, move and delete files in a S3 file storage.
 * `FileChecker`: a class for checking the size of uploaded files.
 * `FileNodeBuilder`: a class to represent file references from S3 (`FileRef` class) a tree of file nodes (`FileNode` class) to facilitate the display of a folder in a web UI.
 
## Usage

To include the files library in your project:

```shell
poetry add git+https://github.com/EPFL-ENAC/enacit4r-files#someref
```

Note: `someref` should be replaced by the commit hash, tag or branch name you want to use.

### S3Service

```python
from enacit4r_files.services.s3 import S3Service, S3Error
from enacit4r_files.models.files import FileRef

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

### FileChecker

```python
from enacit4r_files.tools.files import FileChecker

# Example using the default max file size
file_checker = FileChecker()

# Example usage with FastAPI
@router.post("/tmp",
             status_code=200,
             description="Upload any assets to S3",
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