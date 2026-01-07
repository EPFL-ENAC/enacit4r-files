import pytest
import tempfile
import shutil
from pathlib import Path
from io import BytesIO
from fastapi.datastructures import UploadFile
from enacit4r_files.services.files import LocalFilesService
from enacit4r_files.models.files import FileNode


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    # Cleanup after test
    shutil.rmtree(temp_path)


@pytest.fixture
def local_service(temp_dir):
    """Create a LocalFilesService instance with a temporary base path."""
    return LocalFilesService(base_path=temp_dir)


@pytest.fixture
def sample_file(temp_dir):
    """Create a sample file for testing."""
    file_path = Path(temp_dir) / "sample.txt"
    file_path.write_text("Sample content")
    return str(file_path)


class TestLocalFilesService:
    """Test suite for LocalFilesService."""

    @pytest.mark.asyncio
    async def test_init_creates_base_path(self):
        """Test that initialization creates the base path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            non_existent = Path(temp_dir) / "new_dir"
            service = LocalFilesService(base_path=str(non_existent))
            assert non_existent.exists()
            assert service.base_path == non_existent.resolve()

    @pytest.mark.asyncio
    async def test_upload_file(self, local_service):
        """Test uploading a file via UploadFile."""
        content = b"Test file content"
        upload_file = UploadFile(
            filename="test.txt",
            file=BytesIO(content)
        )
        
        result = await local_service.upload_file(upload_file, folder="uploads")
        
        assert isinstance(result, FileNode)
        assert result.name == "test.txt"
        assert Path(result.path) == Path("uploads") / "test.txt"
        assert result.size == len(content)
        assert result.is_file is True
        assert result.mime_type == "text/plain"
        
        # Verify file was actually written
        file_path = local_service.base_path / "uploads" / "test.txt"
        assert file_path.exists()
        assert file_path.read_bytes() == content

    @pytest.mark.asyncio
    async def test_upload_file_to_root(self, local_service):
        """Test uploading a file to the root folder."""
        content = b"Root file content"
        upload_file = UploadFile(
            filename="root.txt",
            file=BytesIO(content)
        )
        
        result = await local_service.upload_file(upload_file)
        
        assert result.name == "root.txt"
        assert result.path == "root.txt"
        assert result.is_file is True

    @pytest.mark.asyncio
    async def test_upload_local_file(self, local_service, sample_file):
        """Test uploading a local file."""
        result = await local_service.upload_local_file(sample_file, folder="docs")
        
        assert isinstance(result, FileNode)
        assert result.name == "sample.txt"
        assert Path(result.path) == Path("docs") / "sample.txt"
        assert result.size > 0
        assert result.is_file is True
        
        # Verify file was copied
        dest_path = local_service.base_path / "docs" / "sample.txt"
        assert dest_path.exists()
        assert dest_path.read_text() == "Sample content"

    @pytest.mark.asyncio
    async def test_upload_local_file_not_found(self, local_service):
        """Test uploading a non-existent local file."""
        with pytest.raises(FileNotFoundError):
            await local_service.upload_local_file("/non/existent/file.txt")

    @pytest.mark.asyncio
    async def test_get_file(self, local_service):
        """Test retrieving a file."""
        # Create a test file
        test_path = local_service.base_path / "test.txt"
        test_content = b"Test content"
        test_path.write_bytes(test_content)
        
        content, mime_type = await local_service.get_file("test.txt")
        
        assert content == test_content
        assert mime_type == "text/plain"

    @pytest.mark.asyncio
    async def test_get_file_not_found(self, local_service):
        """Test retrieving a non-existent file."""
        with pytest.raises(FileNotFoundError):
            await local_service.get_file("nonexistent.txt")

    @pytest.mark.asyncio
    async def test_list_files_empty(self, local_service):
        """Test listing files in an empty directory."""
        result = await local_service.list_files("")
        assert result == []

    @pytest.mark.asyncio
    async def test_list_files(self, local_service):
        """Test listing files in a directory."""
        # Create some test files and directories
        (local_service.base_path / "file1.txt").write_text("content1")
        (local_service.base_path / "file2.txt").write_text("content2")
        (local_service.base_path / "subdir").mkdir()
        (local_service.base_path / "subdir" / "file3.txt").write_text("content3")
        
        result = await local_service.list_files("")
        
        assert len(result) == 3
        
        # Check files
        files = [node for node in result if node.is_file]
        assert len(files) == 2
        file_names = {f.name for f in files}
        assert "file1.txt" in file_names
        assert "file2.txt" in file_names
        
        # Check directories
        dirs = [node for node in result if not node.is_file]
        assert len(dirs) == 1
        assert dirs[0].name == "subdir"

    @pytest.mark.asyncio
    async def test_list_files_in_subfolder(self, local_service):
        """Test listing files in a subfolder."""
        subdir = local_service.base_path / "subdir"
        subdir.mkdir()
        (subdir / "file1.txt").write_text("content1")
        (subdir / "file2.txt").write_text("content2")
        
        result = await local_service.list_files("subdir")
        
        assert len(result) == 2
        assert all(node.is_file for node in result)
        file_names = {node.name for node in result}
        assert file_names == {"file1.txt", "file2.txt"}

    @pytest.mark.asyncio
    async def test_list_files_nonexistent_folder(self, local_service):
        """Test listing files in a non-existent folder."""
        result = await local_service.list_files("nonexistent")
        assert result == []

    @pytest.mark.asyncio
    async def test_path_exists_file(self, local_service):
        """Test checking if a file exists."""
        # Create a test file
        test_path = local_service.base_path / "exists.txt"
        test_path.write_text("content")
        
        assert await local_service.path_exists("exists.txt") is True
        assert await local_service.path_exists("notexists.txt") is False

    @pytest.mark.asyncio
    async def test_path_exists_directory(self, local_service):
        """Test checking if a directory exists."""
        test_dir = local_service.base_path / "testdir"
        test_dir.mkdir()
        
        assert await local_service.path_exists("testdir") is True

    @pytest.mark.asyncio
    async def test_path_exists_invalid_path(self, local_service):
        """Test checking an invalid path."""
        # Path traversal attempt should return False
        assert await local_service.path_exists("../../etc/passwd") is False

    @pytest.mark.asyncio
    async def test_copy_file(self, local_service):
        """Test copying a file."""
        # Create source file
        source_path = local_service.base_path / "source.txt"
        source_path.write_text("Source content")
        
        result = await local_service.copy_file("source.txt", "destination.txt")
        
        assert result is True
        
        # Verify both files exist
        assert source_path.exists()
        dest_path = local_service.base_path / "destination.txt"
        assert dest_path.exists()
        assert dest_path.read_text() == "Source content"

    @pytest.mark.asyncio
    async def test_copy_file_to_subfolder(self, local_service):
        """Test copying a file to a subfolder."""
        # Create source file
        source_path = local_service.base_path / "source.txt"
        source_path.write_text("Source content")
        
        result = await local_service.copy_file("source.txt", "subfolder/destination.txt")
        
        assert result is True
        
        # Verify destination exists in subfolder
        dest_path = local_service.base_path / "subfolder" / "destination.txt"
        assert dest_path.exists()
        assert dest_path.read_text() == "Source content"

    @pytest.mark.asyncio
    async def test_copy_file_not_found(self, local_service):
        """Test copying a non-existent file."""
        result = await local_service.copy_file("nonexistent.txt", "destination.txt")
        assert result is False

    @pytest.mark.asyncio
    async def test_move_file(self, local_service):
        """Test moving a file."""
        # Create source file
        source_path = local_service.base_path / "source.txt"
        source_path.write_text("Source content")
        
        result = await local_service.move_file("source.txt", "destination.txt")
        
        assert result is True
        
        # Verify source no longer exists
        assert not source_path.exists()
        
        # Verify destination exists
        dest_path = local_service.base_path / "destination.txt"
        assert dest_path.exists()
        assert dest_path.read_text() == "Source content"

    @pytest.mark.asyncio
    async def test_move_file_to_subfolder(self, local_service):
        """Test moving a file to a subfolder."""
        # Create source file
        source_path = local_service.base_path / "source.txt"
        source_path.write_text("Source content")
        
        result = await local_service.move_file("source.txt", "subfolder/destination.txt")
        
        assert result is True
        assert not source_path.exists()
        
        dest_path = local_service.base_path / "subfolder" / "destination.txt"
        assert dest_path.exists()

    @pytest.mark.asyncio
    async def test_move_file_not_found(self, local_service):
        """Test moving a non-existent file."""
        result = await local_service.move_file("nonexistent.txt", "destination.txt")
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_file(self, local_service):
        """Test deleting a file."""
        # Create test file
        test_path = local_service.base_path / "delete_me.txt"
        test_path.write_text("Delete this")
        
        result = await local_service.delete_file("delete_me.txt")
        
        assert result is True
        assert not test_path.exists()

    @pytest.mark.asyncio
    async def test_delete_directory(self, local_service):
        """Test deleting a directory."""
        # Create test directory with file
        test_dir = local_service.base_path / "delete_dir"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content")
        
        result = await local_service.delete_file("delete_dir")
        
        assert result is True
        assert not test_dir.exists()

    @pytest.mark.asyncio
    async def test_delete_file_not_found(self, local_service):
        """Test deleting a non-existent file."""
        result = await local_service.delete_file("nonexistent.txt")
        assert result is False

    @pytest.mark.asyncio
    async def test_security_path_traversal(self, local_service):
        """Test that path traversal attempts are blocked."""
        with pytest.raises(ValueError, match="outside the base path"):
            local_service._get_full_path("../../etc/passwd")

    @pytest.mark.asyncio
    async def test_mime_type_detection(self, local_service):
        """Test MIME type detection for various file types."""
        # Create files with different extensions
        test_files = {
            "test.txt": "text/plain",
            "test.html": "text/html",
            "test.json": "application/json",
            "test.pdf": "application/pdf",
        }
        
        for filename, expected_mime in test_files.items():
            test_path = local_service.base_path / filename
            test_path.write_bytes(b"content")
            
            content, mime_type = await local_service.get_file(filename)
            assert mime_type == expected_mime
