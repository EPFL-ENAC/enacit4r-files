import pytest
from enacit4r_files.services.files import FilesStore


class TestSanitizePath:
    """Test suite for FilesStore().sanitize_path method."""

    def test_simple_path(self):
        """Test sanitization of a simple valid path."""
        result = FilesStore().sanitize_path("folder/file.txt")
        assert result == "folder/file.txt"

    def test_path_with_spaces(self):
        """Test sanitization of path with spaces."""
        result = FilesStore().sanitize_path("my folder/my file.txt")
        assert result == "my folder/my file.txt"

    def test_path_with_underscores(self):
        """Test sanitization of path with underscores."""
        result = FilesStore().sanitize_path("my_folder/my_file.txt")
        assert result == "my_folder/my_file.txt"

    def test_path_with_hyphens(self):
        """Test sanitization of path with hyphens."""
        result = FilesStore().sanitize_path("my-folder/my-file.txt")
        assert result == "my-folder/my-file.txt"

    def test_path_with_dots(self):
        """Test sanitization of path with dots in filename."""
        result = FilesStore().sanitize_path("folder/file.backup.txt")
        assert result == "folder/file.backup.txt"

    def test_path_with_parentheses(self):
        """Test sanitization of path with parentheses."""
        result = FilesStore().sanitize_path("folder/file(1).txt")
        assert result == "folder/file(1).txt"

    def test_path_with_square_brackets(self):
        """Test sanitization of path with square brackets."""
        result = FilesStore().sanitize_path("folder/file[draft].txt")
        assert result == "folder/file[draft].txt"

    def test_path_with_colons(self):
        """Test sanitization of path with colons."""
        result = FilesStore().sanitize_path("folder/file:v1.txt")
        assert result == "folder/file:v1.txt"

    def test_path_with_all_special_chars(self):
        """Test sanitization of path with all allowed special characters."""
        result = FilesStore().sanitize_path("my-folder_v2/file(1)[draft]:backup.txt")
        assert result == "my-folder_v2/file(1)[draft]:backup.txt"

    def test_leading_slash_removal(self):
        """Test that leading slashes are removed."""
        result = FilesStore().sanitize_path("/folder/file.txt")
        assert result == "folder/file.txt"

    def test_multiple_leading_slashes_removal(self):
        """Test that multiple leading slashes are removed."""
        result = FilesStore().sanitize_path("///folder/file.txt")
        assert result == "folder/file.txt"

    def test_newline_removal(self):
        """Test that newline characters are removed."""
        result = FilesStore().sanitize_path("folder/file\n.txt")
        assert result == "folder/file.txt"

    def test_carriage_return_removal(self):
        """Test that carriage return characters are removed."""
        result = FilesStore().sanitize_path("folder/file\r.txt")
        assert result == "folder/file.txt"

    def test_newline_and_carriage_return_removal(self):
        """Test that both newline and carriage return are removed."""
        result = FilesStore().sanitize_path("folder/file\r\n.txt")
        assert result == "folder/file.txt"

    def test_directory_traversal_attack_raises_error(self):
        """Test that paths with '..' raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/../etc/passwd")
        assert "Invalid path: '..' not allowed" in str(exc_info.value)

    def test_directory_traversal_at_start_raises_error(self):
        """Test that paths starting with '..' raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("../etc/passwd")
        assert "Invalid path: '..' not allowed" in str(exc_info.value)

    def test_directory_traversal_at_end_raises_error(self):
        """Test that paths ending with '..' raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/..")
        assert "Invalid path: '..' not allowed" in str(exc_info.value)

    def test_forbidden_character_asterisk_raises_error(self):
        """Test that paths with asterisk raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/*.txt")
        assert "Invalid path: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_question_mark_raises_error(self):
        """Test that paths with question mark raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/file?.txt")
        assert "Invalid path: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_pipe_raises_error(self):
        """Test that paths with pipe character raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/file|.txt")
        assert "Invalid path: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_less_than_raises_error(self):
        """Test that paths with less-than character raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/file<.txt")
        assert "Invalid path: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_greater_than_raises_error(self):
        """Test that paths with greater-than character raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/file>.txt")
        assert "Invalid path: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_quotes_raises_error(self):
        """Test that paths with quote characters raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path('folder/file".txt')
        assert "Invalid path: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_backslash_raises_error(self):
        """Test that paths with backslash raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder\\file.txt")
        assert "Invalid path: contains forbidden characters" in str(exc_info.value)

    def test_empty_path(self):
        """Test sanitization of empty path."""
        result = FilesStore().sanitize_path("")
        assert result == ""

    def test_path_with_numbers(self):
        """Test sanitization of path with numbers."""
        result = FilesStore().sanitize_path("folder123/file456.txt")
        assert result == "folder123/file456.txt"

    def test_alphanumeric_only_path(self):
        """Test sanitization of alphanumeric only path."""
        result = FilesStore().sanitize_path("abc123/def456")
        assert result == "abc123/def456"
