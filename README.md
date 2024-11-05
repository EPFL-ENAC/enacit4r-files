# ENAC-IT4R Python Utils

A Python library of utils that are commomly used in the EPFL ENAC IT infrastructure:
 
 * `S3Service`: a convenient service to upload, get, list, check, copy, move and delete files in a S3 file storage.

## Usage

To include the library in your project:

```
poetry add git+https://github.com/EPFL-ENAC/enacit4r-pyutils
```

For refering to a specific git tag/branch/commit:

```
poetry add git+https://github.com/EPFL-ENAC/enacit4r-pyutils#someref
```

Then in the python code:

```
from enacit4r.services.s3 import S3Service, S3Error
from enacit4r.models.files import FileRef

s3_service = S3Service(config.S3_ENDPOINT_PROTOCOL + config.S3_ENDPOINT_HOSTNAME,
                     config.S3_ACCESS_KEY_ID,
                     config.S3_SECRET_ACCESS_KEY, 
                     config.S3_REGION,
                     config.S3_BUCKET,
                     config.S3_PATH_PREFIX)

try:
  # do something with s3_service
except S3Error as e:
    print(e)
```
