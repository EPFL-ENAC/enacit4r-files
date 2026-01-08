import pytest
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch
from cryptography.fernet import Fernet
from fastapi.datastructures import UploadFile
from starlette.datastructures import Headers
from enacit4r_files.services import S3FilesStore
from enacit4r_files.services.s3 import S3Service
from enacit4r_files.models.files import FileNode, FileRef


@pytest.fixture
def mock_s3_service():
    """Create a mock S3Service instance."""
    service = MagicMock(spec=S3Service)
    service.s3_endpoint_url = "http://localhost:9000"
    service.bucket = "test-bucket"
    service.path_prefix = "test-prefix/"
    service.region = "us-east-1"
    
    # Mock methods
    service.path_exists = AsyncMock(return_value=False)
    service.list_files = AsyncMock(return_value=[])
    service.get_file = AsyncMock(return_value=(False, False))
    service.upload_file = AsyncMock()
    service.upload_local_file = AsyncMock()
    service.copy_file = AsyncMock(return_value=False)
    service.move_file = AsyncMock(return_value=False)
    service.delete_file = AsyncMock(return_value=False)
    service.to_s3_key = MagicMock(side_effect=lambda x: x)
    
    return service


@pytest.fixture
def s3_files_store(mock_s3_service):
    """Create an S3FilesStore instance with mocked S3Service."""
    return S3FilesStore(s3_service=mock_s3_service, key=None)


@pytest.fixture
def fernet_key():
    """Generate a Fernet key for encryption tests."""
    return Fernet.generate_key()


class TestS3FilesStore:
    """Test suite for S3FilesStore."""

    @pytest.mark.asyncio
    async def test_init_creates_instance(self, mock_s3_service):
        """Test that initialization creates an S3FilesStore instance."""
        service = S3FilesStore(s3_service=mock_s3_service)
        assert service.s3_service == mock_s3_service
        assert service.fernet is None

    @pytest.mark.asyncio
    async def test_init_with_encryption_key(self, mock_s3_service, fernet_key):
        """Test initialization with encryption key."""
        service = S3FilesStore(s3_service=mock_s3_service, key=fernet_key)
        assert service.fernet is not None

    @pytest.mark.asyncio
    async def test_write_file(self, s3_files_store, mock_s3_service):
        """Test writing a file via UploadFile."""
        content = b"Test file content"
        headers = Headers({'content-type': 'text/plain'})
        upload_file = UploadFile(
            filename="test.txt",
            file=BytesIO(content),
            headers=headers
        )
        
        # Mock S3Service upload_file to return a FileRef
        mock_file_ref = FileRef(
            name="test.txt",
            path="uploads/test.txt",
            size=len(content),
            mime_type="text/plain"
        )
        mock_s3_service.upload_file.return_value = mock_file_ref
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        result = await s3_files_store.write_file(upload_file, folder="uploads")
        
        assert isinstance(result, FileNode)
        assert result.name == "test.txt"
        assert result.path == "uploads/test.txt"
        assert result.size == len(content)
        assert result.is_file is True
        assert result.mime_type == "text/plain"
        
        # Verify S3Service methods were called
        assert mock_s3_service.upload_file.called
        assert mock_s3_service.upload_local_file.called  # For metadata

    @pytest.mark.asyncio
    async def test_upload_file_to_root(self, s3_files_store, mock_s3_service):
        """Test writing a file to the root folder."""
        content = b"Root file content"
        upload_file = UploadFile(
            filename="root.txt",
            file=BytesIO(content)
        )
        
        mock_file_ref = FileRef(
            name="root.txt",
            path="root.txt",
            size=len(content),
            mime_type="text/plain"
        )
        mock_s3_service.upload_file.return_value = mock_file_ref
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        result = await s3_files_store.write_file(upload_file)
        
        assert result.name == "root.txt"
        assert result.path == "root.txt"
        assert result.is_file is True

    @pytest.mark.asyncio
    async def test_write_local_file(self, s3_files_store, mock_s3_service, tmp_path):
        """Test writing a local file."""
        # Create a temporary file
        test_file = tmp_path / "sample.txt"
        test_content = b"Sample content"
        test_file.write_bytes(test_content)
        
        mock_file_ref = FileRef(
            name="sample.txt",
            path="docs/sample.txt",
            size=len(test_content),
            mime_type="text/plain"
        )
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        result = await s3_files_store.write_local_file(str(test_file), folder="docs")
        
        assert isinstance(result, FileNode)
        assert result.name == "sample.txt"
        assert result.path == "docs/sample.txt"
        assert result.size == len(test_content)
        assert result.is_file is True

    @pytest.mark.asyncio
    async def test_upload_local_file_not_found(self, s3_files_store):
        """Test writing a non-existent local file."""
        with pytest.raises(FileNotFoundError):
            await s3_files_store.write_local_file("/non/existent/file.txt")

    @pytest.mark.asyncio
    async def test_get_file(self, s3_files_store, mock_s3_service):
        """Test retrieving a file."""
        test_content = b"Test content"
        mock_s3_service.get_file.return_value = (test_content, "text/plain")
        
        content, mime_type = await s3_files_store.get_file("test.txt")
        
        assert content == test_content
        assert mime_type == "text/plain"
        mock_s3_service.get_file.assert_called_once_with("test.txt")

    @pytest.mark.asyncio
    async def test_get_file_not_found(self, s3_files_store, mock_s3_service):
        """Test retrieving a non-existent file."""
        mock_s3_service.get_file.return_value = (False, False)
        
        with pytest.raises(FileNotFoundError):
            await s3_files_store.get_file("nonexistent.txt")

    @pytest.mark.asyncio
    async def test_list_files_empty(self, s3_files_store, mock_s3_service):
        """Test listing files in an empty directory."""
        mock_s3_service.list_files.return_value = []
        mock_s3_service.to_s3_key.return_value = ""
        
        result = await s3_files_store.list_files("")
        assert result == []

    @pytest.mark.asyncio
    async def test_list_files(self, s3_files_store, mock_s3_service):
        """Test listing files in a directory."""
        # Mock S3 keys with metadata files
        mock_keys = [
            "file1.txt",
            "file1.txt.meta",
            "file2.txt",
            "file2.txt.meta",
            "subdir/file3.txt"
        ]
        mock_s3_service.list_files.return_value = mock_keys
        mock_s3_service.to_s3_key.return_value = ""
        
        # Mock metadata reading
        async def mock_read_metadata(key):
            if "file1.txt" in key and not key.endswith(".meta"):
                return FileNode(name="file1.txt", path="file1.txt", size=100, mime_type="text/plain", is_file=True)
            elif "file2.txt" in key and not key.endswith(".meta"):
                return FileNode(name="file2.txt", path="file2.txt", size=200, mime_type="text/plain", is_file=True)
            return None
        
        with patch.object(s3_files_store, '_read_file_node', side_effect=mock_read_metadata):
            result = await s3_files_store.list_files("")
        
        # Should have 2 files and 1 directory
        assert len(result) == 3
        
        files = [node for node in result if node.is_file]
        assert len(files) == 2
        file_names = {f.name for f in files}
        assert "file1.txt" in file_names
        assert "file2.txt" in file_names
        
        dirs = [node for node in result if not node.is_file]
        assert len(dirs) == 1
        assert dirs[0].name == "subdir"
    
    
    @pytest.mark.asyncio
    async def test_list_files_recursively(self, s3_files_store, mock_s3_service):
        """Test listing files recursively."""
        mock_keys = [
            "file1.txt",
            "file1.txt.meta",
            "subdir/file2.txt",
            "subdir/file2.txt.meta",
            "subdir/nested/file3.txt",
            "subdir/nested/file3.txt.meta"
        ]
        mock_s3_service.list_files.return_value = mock_keys
        mock_s3_service.to_s3_key.return_value = ""
        
        async def mock_read_metadata(key):
            if "file1.txt" in key and not key.endswith(".meta"):
                return FileNode(name="file1.txt", path="file1.txt", size=100, mime_type="text/plain", is_file=True)
            elif "file2.txt" in key and not key.endswith(".meta"):
                return FileNode(name="file2.txt", path="subdir/file2.txt", size=200, mime_type="text/plain", is_file=True)
            elif "file3.txt" in key and not key.endswith(".meta"):
                return FileNode(name="file3.txt", path="subdir/nested/file3.txt", size=300, mime_type="text/plain", is_file=True)
            return None
        
        with patch.object(s3_files_store, '_read_file_node', side_effect=mock_read_metadata):
            result = await s3_files_store.list_files("", recursive=True)
        
        # Should have 3 files and 2 directories
        assert len(result) == 5
        
        files = [node for node in result if node.is_file]
        assert len(files) == 3
        file_names = {f.name for f in files}
        assert "file1.txt" in file_names
        assert "file2.txt" in file_names
        assert "file3.txt" in file_names
        
        dirs = [node for node in result if not node.is_file]
        assert len(dirs) == 2
        dir_names = {d.name for d in dirs}
        assert "subdir" in dir_names
        assert "nested" in dir_names

    @pytest.mark.asyncio
    async def test_list_files_in_subfolder(self, s3_files_store, mock_s3_service):
        """Test listing files in a subfolder."""
        mock_keys = [
            "subdir/file1.txt",
            "subdir/file1.txt.meta",
            "subdir/file2.txt",
            "subdir/file2.txt.meta"
        ]
        mock_s3_service.list_files.return_value = mock_keys
        mock_s3_service.to_s3_key.return_value = "subdir/"
        
        async def mock_read_metadata(key):
            if "file1.txt" in key and not key.endswith(".meta"):
                return FileNode(name="file1.txt", path="subdir/file1.txt", size=100, mime_type="text/plain", is_file=True)
            elif "file2.txt" in key and not key.endswith(".meta"):
                return FileNode(name="file2.txt", path="subdir/file2.txt", size=200, mime_type="text/plain", is_file=True)
            return None
        
        with patch.object(s3_files_store, '_read_file_node', side_effect=mock_read_metadata):
            result = await s3_files_store.list_files("subdir")
        
        assert len(result) == 2
        assert all(node.is_file for node in result)
        file_names = {node.name for node in result}
        assert file_names == {"file1.txt", "file2.txt"}

    @pytest.mark.asyncio
    async def test_file_exists_file(self, s3_files_store, mock_s3_service):
        """Test checking if a file exists."""
        mock_s3_service.path_exists.return_value = True
        
        assert await s3_files_store.file_exists("exists.txt") is True
        
        mock_s3_service.path_exists.return_value = False
        assert await s3_files_store.file_exists("notexists.txt") is False

    @pytest.mark.asyncio
    async def test_copy_file(self, s3_files_store, mock_s3_service):
        """Test copying a file."""
        mock_s3_service.copy_file.return_value = "destination.txt"
        
        # Mock metadata reading and writing
        mock_node = FileNode(name="source.txt", path="source.txt", size=100, mime_type="text/plain", is_file=True)
        with patch.object(s3_files_store, '_read_file_node', return_value=mock_node):
            with patch.object(s3_files_store, '_dump_file_node', new_callable=AsyncMock):
                result = await s3_files_store.copy_file("source.txt", "destination.txt")
        
        assert result is True
        mock_s3_service.copy_file.assert_called_once_with("source.txt", "destination.txt")

    @pytest.mark.asyncio
    async def test_copy_file_not_found(self, s3_files_store, mock_s3_service):
        """Test copying a non-existent file."""
        mock_s3_service.copy_file.return_value = False
        
        result = await s3_files_store.copy_file("nonexistent.txt", "destination.txt")
        assert result is False

    @pytest.mark.asyncio
    async def test_move_file(self, s3_files_store, mock_s3_service):
        """Test moving a file."""
        mock_s3_service.move_file.return_value = "destination.txt"
        
        # Mock metadata operations
        mock_node = FileNode(name="source.txt", path="source.txt", size=100, mime_type="text/plain", is_file=True)
        with patch.object(s3_files_store, '_read_file_node', return_value=mock_node):
            with patch.object(s3_files_store, '_dump_file_node', new_callable=AsyncMock):
                with patch.object(s3_files_store, '_delete_file_node', new_callable=AsyncMock):
                    result = await s3_files_store.move_file("source.txt", "destination.txt")
        
        assert result is True
        mock_s3_service.move_file.assert_called_once_with("source.txt", "destination.txt")

    @pytest.mark.asyncio
    async def test_move_file_not_found(self, s3_files_store, mock_s3_service):
        """Test moving a non-existent file."""
        mock_s3_service.move_file.return_value = False
        
        result = await s3_files_store.move_file("nonexistent.txt", "destination.txt")
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_file(self, s3_files_store, mock_s3_service):
        """Test deleting a file."""
        mock_s3_service.delete_file.return_value = "delete_me.txt"
        
        with patch.object(s3_files_store, '_delete_file_node', new_callable=AsyncMock):
            result = await s3_files_store.delete_file("delete_me.txt")
        
        assert result is True
        mock_s3_service.delete_file.assert_called_once_with("delete_me.txt")

    @pytest.mark.asyncio
    async def test_delete_file_not_found(self, s3_files_store, mock_s3_service):
        """Test deleting a non-existent file."""
        mock_s3_service.delete_file.return_value = False
        
        result = await s3_files_store.delete_file("nonexistent.txt")
        assert result is False


class TestS3FilesStoreWithEncryption:
    """Test suite for S3FilesStore with Fernet encryption."""

    @pytest.mark.asyncio
    async def test_upload_file_with_encryption(self, mock_s3_service, fernet_key):
        """Test writing a file with encryption enabled."""
        service = S3FilesStore(s3_service=mock_s3_service, key=fernet_key)
        
        content = b"Secret file content"
        upload_file = UploadFile(
            filename="encrypted.txt",
            file=BytesIO(content)
        )
        
        mock_file_ref = FileRef(
            name="encrypted.txt",
            path="secure/encrypted.txt",
            size=len(content),
            mime_type="text/plain"
        )
        mock_s3_service.upload_file.return_value = mock_file_ref
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        result = await service.write_file(upload_file, folder="secure")
        
        assert isinstance(result, FileNode)
        assert result.name == "encrypted.txt"
        assert result.is_file is True
        assert result.size == len(content)  # Original size, not encrypted size
        
        # Verify that the content sent to S3 was encrypted
        call_args = mock_s3_service.upload_file.call_args
        uploaded_file = call_args[0][0]
        uploaded_content = uploaded_file.file.read()
        
        # Encrypted content should be different from original
        assert uploaded_content != content
        
        # Decrypt to verify
        fernet = Fernet(fernet_key)
        decrypted = fernet.decrypt(uploaded_content)
        assert decrypted == content

    @pytest.mark.asyncio
    async def test_get_file_with_encryption(self, mock_s3_service, fernet_key):
        """Test retrieving an encrypted file."""
        service = S3FilesStore(s3_service=mock_s3_service, key=fernet_key)
        
        # Encrypt content
        original_content = b"Secret content"
        fernet = Fernet(fernet_key)
        encrypted_content = fernet.encrypt(original_content)
        
        mock_s3_service.get_file.return_value = (encrypted_content, "text/plain")
        
        # Get file through service - should decrypt automatically
        content, mime_type = await service.get_file("encrypted.txt")
        
        assert content == original_content
        assert mime_type == "text/plain"

    @pytest.mark.asyncio
    async def test_upload_local_file_with_encryption(self, mock_s3_service, fernet_key, tmp_path):
        """Test writing a local file with encryption."""
        service = S3FilesStore(s3_service=mock_s3_service, key=fernet_key)
        
        # Create a source file
        source_path = tmp_path / "source.txt"
        original_content = b"Local file content"
        source_path.write_bytes(original_content)
        
        mock_file_ref = FileRef(
            name="source.txt",
            path="encrypted/source.txt",
            size=len(original_content),
            mime_type="text/plain"
        )
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        result = await service.write_local_file(str(source_path), folder="encrypted")
        
        assert result.name == "source.txt"
        assert result.is_file is True
        assert result.size == len(original_content)

    @pytest.mark.asyncio
    async def test_round_trip_with_encryption(self, mock_s3_service, fernet_key):
        """Test writing and retrieving a file with encryption."""
        service = S3FilesStore(s3_service=mock_s3_service, key=fernet_key)
        
        # Upload a file
        original_content = b"Round trip test content"
        upload_file = UploadFile(
            filename="roundtrip.txt",
            file=BytesIO(original_content)
        )
        
        mock_file_ref = FileRef(
            name="roundtrip.txt",
            path="roundtrip.txt",
            size=len(original_content),
            mime_type="text/plain"
        )
        mock_s3_service.upload_file.return_value = mock_file_ref
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        await service.write_file(upload_file)
        
        # Get the encrypted content that was uploaded
        call_args = mock_s3_service.upload_file.call_args
        uploaded_file = call_args[0][0]
        encrypted_content = uploaded_file.file.read()
        
        # Mock S3 to return the encrypted content
        mock_s3_service.get_file.return_value = (encrypted_content, "text/plain")
        
        # Retrieve the file
        retrieved_content, mime_type = await service.get_file("roundtrip.txt")
        
        # Content should match original after decryption
        assert retrieved_content == original_content
        assert mime_type == "text/plain"

    @pytest.mark.asyncio
    async def test_encryption_with_binary_file(self, mock_s3_service, fernet_key):
        """Test encryption with binary file content."""
        service = S3FilesStore(s3_service=mock_s3_service, key=fernet_key)
        
        # Create binary content
        binary_content = bytes(range(256))
        upload_file = UploadFile(
            filename="binary.bin",
            file=BytesIO(binary_content)
        )
        
        mock_file_ref = FileRef(
            name="binary.bin",
            path="binary.bin",
            size=len(binary_content),
            mime_type="application/octet-stream"
        )
        mock_s3_service.upload_file.return_value = mock_file_ref
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        await service.write_file(upload_file)
        
        # Get encrypted content
        call_args = mock_s3_service.upload_file.call_args
        uploaded_file = call_args[0][0]
        encrypted_content = uploaded_file.file.read()
        
        # Mock retrieval
        mock_s3_service.get_file.return_value = (encrypted_content, "application/octet-stream")
        
        # Retrieve and verify
        retrieved_content, _ = await service.get_file("binary.bin")
        assert retrieved_content == binary_content

    @pytest.mark.asyncio
    async def test_encryption_with_large_content(self, mock_s3_service, fernet_key):
        """Test encryption with larger file content."""
        service = S3FilesStore(s3_service=mock_s3_service, key=fernet_key)
        
        # Create a larger content (1MB)
        large_content = b"X" * (1024 * 1024)
        upload_file = UploadFile(
            filename="large.txt",
            file=BytesIO(large_content)
        )
        
        mock_file_ref = FileRef(
            name="large.txt",
            path="large.txt",
            size=len(large_content),
            mime_type="text/plain"
        )
        mock_s3_service.upload_file.return_value = mock_file_ref
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        await service.write_file(upload_file)
        
        # Get encrypted content
        call_args = mock_s3_service.upload_file.call_args
        uploaded_file = call_args[0][0]
        encrypted_content = uploaded_file.file.read()
        
        # Mock retrieval
        mock_s3_service.get_file.return_value = (encrypted_content, "text/plain")
        
        # Retrieve and verify
        retrieved_content, _ = await service.get_file("large.txt")
        assert retrieved_content == large_content
        assert len(retrieved_content) == len(large_content)

    @pytest.mark.asyncio
    async def test_encryption_methods_directly(self, mock_s3_service, fernet_key):
        """Test encrypt and decrypt methods directly."""
        service = S3FilesStore(s3_service=mock_s3_service, key=fernet_key)
        
        original_content = b"Test encryption methods"
        
        # Encrypt
        encrypted = service.encrypt_content(original_content)
        assert encrypted != original_content
        assert len(encrypted) > len(original_content)  # Encrypted is longer
        
        # Decrypt
        decrypted = service.decrypt_content(encrypted)
        assert decrypted == original_content

    @pytest.mark.asyncio
    async def test_no_encryption_when_key_not_provided(self, mock_s3_service):
        """Test that files are not encrypted when no key is provided."""
        service = S3FilesStore(s3_service=mock_s3_service, key=None)
        
        original_content = b"Unencrypted content"
        upload_file = UploadFile(
            filename="plain.txt",
            file=BytesIO(original_content)
        )
        
        mock_file_ref = FileRef(
            name="plain.txt",
            path="plain.txt",
            size=len(original_content),
            mime_type="text/plain"
        )
        mock_s3_service.upload_file.return_value = mock_file_ref
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        await service.write_file(upload_file)
        
        # Verify content was not encrypted
        call_args = mock_s3_service.upload_file.call_args
        uploaded_file = call_args[0][0]
        uploaded_content = uploaded_file.file.read()
        
        # Should be the same as original
        assert uploaded_content == original_content

    @pytest.mark.asyncio
    async def test_encryption_with_special_characters(self, mock_s3_service, fernet_key):
        """Test encryption with special characters and unicode."""
        service = S3FilesStore(s3_service=mock_s3_service, key=fernet_key)
        
        # Content with special characters
        special_content = "Hello 世界! 🌍 Special: @#$%^&*()".encode('utf-8')
        upload_file = UploadFile(
            filename="special.txt",
            file=BytesIO(special_content)
        )
        
        mock_file_ref = FileRef(
            name="special.txt",
            path="special.txt",
            size=len(special_content),
            mime_type="text/plain"
        )
        mock_s3_service.upload_file.return_value = mock_file_ref
        mock_s3_service.upload_local_file.return_value = mock_file_ref
        
        await service.write_file(upload_file)
        
        # Get encrypted content
        call_args = mock_s3_service.upload_file.call_args
        uploaded_file = call_args[0][0]
        encrypted_content = uploaded_file.file.read()
        
        # Mock retrieval
        mock_s3_service.get_file.return_value = (encrypted_content, "text/plain")
        
        # Retrieve and verify
        retrieved_content, _ = await service.get_file("special.txt")
        assert retrieved_content == special_content


class TestS3MetadataOperations:
    """Test suite for S3 metadata operations."""

    @pytest.mark.asyncio
    async def test_dump_file_node(self, s3_files_store, mock_s3_service):
        """Test dumping FileNode metadata to S3."""
        node = FileNode(
            name="test.txt",
            path="folder/test.txt",
            size=100,
            mime_type="text/plain",
            is_file=True
        )
        
        mock_s3_service.upload_local_file.return_value = FileRef(
            name="test.txt.meta",
            path="folder/test.txt.meta",
            size=200,
            mime_type="application/json"
        )
        
        await s3_files_store._dump_file_node(node, "folder")
        
        # Verify upload was called
        assert mock_s3_service.upload_local_file.called

    @pytest.mark.asyncio
    async def test_read_file_node(self, s3_files_store, mock_s3_service):
        """Test reading FileNode metadata from S3."""
        # Mock metadata content
        mock_metadata = FileNode(
            name="test.txt",
            path="test.txt",
            size=100,
            mime_type="text/plain",
            is_file=True
        )
        metadata_json = mock_metadata.model_dump_json()
        
        mock_s3_service.get_file.return_value = (metadata_json.encode('utf-8'), "application/json")
        
        result = await s3_files_store._read_file_node("test.txt")
        
        assert result is not None
        assert result.name == "test.txt"
        assert result.path == "test.txt"
        assert result.size == 100

    @pytest.mark.asyncio
    async def test_read_file_node_not_found(self, s3_files_store, mock_s3_service):
        """Test reading metadata when file doesn't exist."""
        mock_s3_service.get_file.return_value = (False, False)
        
        result = await s3_files_store._read_file_node("nonexistent.txt")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_file_node(self, s3_files_store, mock_s3_service):
        """Test deleting FileNode metadata from S3."""
        mock_s3_service.delete_file.return_value = "test.txt.meta"
        
        await s3_files_store._delete_file_node("test.txt")
        
        mock_s3_service.delete_file.assert_called_once_with("test.txt.meta")

    @pytest.mark.asyncio
    async def test_metadata_preserved_on_copy(self, s3_files_store, mock_s3_service):
        """Test that metadata is preserved when copying a file."""
        mock_s3_service.copy_file.return_value = "destination.txt"
        
        # Mock reading metadata
        source_node = FileNode(
            name="source.txt",
            path="source.txt",
            size=100,
            mime_type="text/plain",
            is_file=True
        )
        
        with patch.object(s3_files_store, '_read_file_node', return_value=source_node):
            with patch.object(s3_files_store, '_dump_file_node', new_callable=AsyncMock) as mock_dump:
                await s3_files_store.copy_file("source.txt", "destination.txt")
                
                # Verify metadata was dumped for destination
                assert mock_dump.called

    @pytest.mark.asyncio
    async def test_metadata_updated_on_move(self, s3_files_store, mock_s3_service):
        """Test that metadata is updated when moving a file."""
        mock_s3_service.move_file.return_value = "destination.txt"
        
        # Mock operations
        source_node = FileNode(
            name="source.txt",
            path="source.txt",
            size=100,
            mime_type="text/plain",
            is_file=True
        )
        
        with patch.object(s3_files_store, '_read_file_node', return_value=source_node):
            with patch.object(s3_files_store, '_dump_file_node', new_callable=AsyncMock) as mock_dump:
                with patch.object(s3_files_store, '_delete_file_node', new_callable=AsyncMock) as mock_delete:
                    await s3_files_store.move_file("source.txt", "destination.txt")
                    
                    # Verify metadata was updated and old one deleted
                    assert mock_dump.called
                    assert mock_delete.called

    @pytest.mark.asyncio
    async def test_metadata_deleted_with_file(self, s3_files_store, mock_s3_service):
        """Test that metadata is deleted when file is deleted."""
        mock_s3_service.delete_file.return_value = "test.txt"
        
        with patch.object(s3_files_store, '_delete_file_node', new_callable=AsyncMock) as mock_delete:
            await s3_files_store.delete_file("test.txt")
            
            # Verify metadata was deleted
            mock_delete.assert_called_once_with("test.txt")
