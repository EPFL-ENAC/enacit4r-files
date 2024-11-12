# ENAC-IT4R Python Utils

A Python library of utils that are commomly used in the EPFL ENAC IT infrastructure:
 
 * `S3Service`: a service to upload, get, list, check, copy, move and delete files in a S3 file storage.
 * `FileChecker`: a class for checking the size of uploaded files.
 * `FileNodeBuilder`: a class to represent file references from S3 (`FileRef` class) a tree of file nodes (`FileNode` class) to facilitate the display of a folder in a web UI.
 * `KeycloakService`: a service to authenticate users with Keycloak and check their roles.

## Usage

To include the files library in your project:

```
poetry add git+https://github.com/EPFL-ENAC/enacit4r-pyutils@someref#subdirectory=files
```
To include the authentication library in your project:

```
poetry add git+https://github.com/EPFL-ENAC/enacit4r-pyutils@someref#subdirectory=auth
```

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

### KeycloakService
  
```python
from enacit4r_auth.services.keycloak import KeycloakService, User

kc_service = KeycloakService(config.KEYCLOAK_URL, config.KEYCLOAK_REALM, 
    config.KEYCLOAK_CLIENT_ID, config.KEYCLOAK_CLIENT_SECRET, "myapp-admin-role")


# Example usage with FastAPI
@router.delete("/{file_path:path}",
               status_code=204,
               description="Delete asset present in S3, requires administrator role",
               )
async def delete_file(file_path: str, user: User = Depends(kc_service.require_admin())):
    # delete path if it contains /tmp/
    pass

```
