import pytest
from io import BytesIO
from pathlib import Path
from fastapi.datastructures import UploadFile
from starlette.datastructures import Headers
from enacit4r_files.services.s3 import S3Service

PIL = pytest.importorskip("PIL")
from PIL import Image  # noqa: E402


def _make_image_bytes(fmt: str) -> bytes:
    image = Image.new("RGB", (4, 4), color="red")
    buffer = BytesIO()
    image.save(buffer, format=fmt)
    return buffer.getvalue()


@pytest.fixture
def s3_service():
    return S3Service(
        "http://localhost:9000", "key", "secret", "us-east-1", "bucket", "prefix"
    )


class TestConvertImage:
    """Test suite for S3Service image conversion (requires the `images` extra)."""

    @pytest.mark.asyncio
    async def test_convert_jpeg_to_webp(self, s3_service):
        content = _make_image_bytes("JPEG")
        upload_file = UploadFile(
            filename="test.jpg",
            file=BytesIO(content),
            headers=Headers({"content-type": "image/jpeg"}),
        )

        converted, original = await s3_service._convert_image(upload_file)

        assert original.getvalue() == content
        converted.seek(0)
        assert Image.open(converted).format == "WEBP"

    @pytest.mark.asyncio
    async def test_convert_png_to_webp(self, s3_service):
        content = _make_image_bytes("PNG")
        upload_file = UploadFile(
            filename="test.png",
            file=BytesIO(content),
            headers=Headers({"content-type": "image/png"}),
        )

        converted, original = await s3_service._convert_image(upload_file)

        assert original.getvalue() == content
        converted.seek(0)
        assert Image.open(converted).format == "WEBP"

    def test_convert_image_file(self, s3_service, tmp_path):
        source = tmp_path / "test.jpg"
        source.write_bytes(_make_image_bytes("JPEG"))

        webp_relative_path = s3_service._convert_image_file(str(tmp_path), "test.jpg")

        assert webp_relative_path == "test.webp"
        webp_path = Path(tmp_path) / webp_relative_path
        assert webp_path.exists()
        assert Image.open(webp_path).format == "WEBP"
