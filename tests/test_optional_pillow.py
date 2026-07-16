import subprocess
import sys
import textwrap
from io import BytesIO
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.datastructures import UploadFile
from starlette.datastructures import Headers

from enacit4r_files.models.files import FileRef
from enacit4r_files.services.s3 import S3Service


def test_import_services_without_pillow():
    """enacit4r_files.services must import cleanly even when PIL is unavailable."""
    script = textwrap.dedent("""
        import builtins

        real_import = builtins.__import__

        def blocking_import(name, *args, **kwargs):
            if name == "PIL" or name.startswith("PIL."):
                raise ImportError("No module named 'PIL'")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = blocking_import

        from enacit4r_files.services import (
            FilesStore,
            LocalFilesStore,
            S3Service,
            S3FilesStore,
        )

        print("OK")
    """)

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )

    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_convert_image_fails_without_pillow():
    """Image conversion should fail directly (no silent fallback) if PIL is unavailable."""
    script = textwrap.dedent("""
        import asyncio
        import builtins
        from io import BytesIO

        real_import = builtins.__import__

        def blocking_import(name, *args, **kwargs):
            if name == "PIL" or name.startswith("PIL."):
                raise ImportError("No module named 'PIL'")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = blocking_import

        from fastapi.datastructures import UploadFile
        from enacit4r_files.services.s3 import S3Service

        service = S3Service(
            "http://localhost:9000", "key", "secret", "us-east-1", "bucket", "prefix"
        )
        upload_file = UploadFile(filename="test.jpg", file=BytesIO(b"not really an image"))

        try:
            asyncio.run(service._convert_image(upload_file))
            print("NO_ERROR")
        except ImportError:
            print("IMPORT_ERROR")
    """)

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )

    assert result.returncode == 0, result.stderr
    assert "IMPORT_ERROR" in result.stdout


@pytest.mark.asyncio
async def test_upload_local_image_falls_back_to_unconverted_upload_without_pillow(tmp_path):
    """Without Pillow, uploading a local image must not raise: the image is
    uploaded as-is, unconverted, just like an already-webp file."""
    service = S3Service(
        "http://localhost:9000", "key", "secret", "us-east-1", "bucket", "prefix"
    )

    source = tmp_path / "test.jpg"
    source.write_bytes(b"not really an image")

    orig_ref = FileRef(name="test.jpg", path="test.jpg", size=20, mime_type="image/jpeg")

    with patch.object(
        service, "_convert_image_file", side_effect=ImportError("No module named 'PIL'")
    ):
        with patch.object(
            service, "_upload_local_file", new_callable=AsyncMock, return_value=orig_ref
        ) as mock_upload_local_file:
            result = await service._upload_local_image(str(tmp_path), "test.jpg")

    # The original (unconverted) file is uploaded, no webp alt version.
    mock_upload_local_file.assert_awaited_once_with(str(tmp_path), "test.jpg", "")
    assert result is orig_ref


@pytest.mark.asyncio
async def test_upload_image_falls_back_to_unconverted_upload_without_pillow():
    """Without Pillow, uploading an image via UploadFile must not raise: the
    image is uploaded as-is, unconverted, just like an already-webp file."""
    service = S3Service(
        "http://localhost:9000", "key", "secret", "us-east-1", "bucket", "prefix"
    )

    upload_file = UploadFile(
        filename="test.jpg",
        file=BytesIO(b"not really an image"),
        headers=Headers({"content-type": "image/jpeg"}),
    )

    orig_ref = FileRef(name="test.jpg", path="test.jpg", size=20, mime_type="image/jpeg")

    with patch.object(
        service, "_convert_image", side_effect=ImportError("No module named 'PIL'")
    ):
        with patch.object(
            service, "_upload_file", new_callable=AsyncMock, return_value=orig_ref
        ) as mock_upload_file:
            result = await service._upload_image(upload_file)

    mock_upload_file.assert_awaited_once_with(upload_file, "")
    assert result is orig_ref
