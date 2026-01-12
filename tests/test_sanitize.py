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

    def test_filename_with_consecutive_dots(self):
        """Test that filenames with consecutive dots are allowed."""
        result = FilesStore().sanitize_path("folder/file..txt")
        assert result == "folder/file..txt"

    def test_filename_with_triple_dots(self):
        """Test that filenames with three consecutive dots are allowed."""
        result = FilesStore().sanitize_path("folder/archive...tar.gz")
        assert result == "folder/archive...tar.gz"

    def test_filename_starting_with_double_dots(self):
        """Test that filenames starting with double dots are allowed."""
        result = FilesStore().sanitize_path("folder/..config")
        assert result == "folder/..config"

    def test_filename_ending_with_double_dots(self):
        """Test that filenames ending with double dots are allowed."""
        result = FilesStore().sanitize_path("folder/backup..")
        assert result == "folder/backup.."

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
        """Test that paths with '..' as path component raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/../etc/passwd")
        assert "Invalid path: '..' not allowed" in str(exc_info.value)

    def test_directory_traversal_at_start_raises_error(self):
        """Test that paths starting with '..' as path component raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("../etc/passwd")
        assert "Invalid path: '..' not allowed" in str(exc_info.value)

    def test_directory_traversal_at_end_raises_error(self):
        """Test that paths ending with '..' as path component raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/..")
        assert "Invalid path: '..' not allowed" in str(exc_info.value)

    def test_directory_traversal_with_dot_dot_only_raises_error(self):
        """Test that standalone '..' path component raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("..")
        assert "Invalid path: '..' not allowed" in str(exc_info.value)

    def test_directory_traversal_multiple_raises_error(self):
        """Test that multiple '..' path components raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("../../etc/passwd")
        assert "Invalid path: '..' not allowed" in str(exc_info.value)

    def test_forbidden_character_asterisk_raises_error(self):
        """Test that paths with asterisk raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/*.txt")
        assert "Invalid path: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_pipe_raises_error(self):
        """Test that paths with pipe character raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path("folder/file|.txt")
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

    def test_none_path_raises_error(self):
        """Test that None path raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_path(None)
        assert "Invalid path: path cannot be None" in str(exc_info.value)

    def test_path_with_numbers(self):
        """Test sanitization of path with numbers."""
        result = FilesStore().sanitize_path("folder123/file456.txt")
        assert result == "folder123/file456.txt"

    def test_alphanumeric_only_path(self):
        """Test sanitization of alphanumeric only path."""
        result = FilesStore().sanitize_path("abc123/def456")
        assert result == "abc123/def456"

    def test_path_with_french_accents(self):
        """Test sanitization of path with French accented characters."""
        result = FilesStore().sanitize_path("dossier/fichier_été.txt")
        assert result == "dossier/fichier_été.txt"

    def test_path_with_french_cedilla(self):
        """Test sanitization of path with French characters."""
        result = FilesStore().sanitize_path("français/leçon_d'été_par_cœur.txt")
        assert result == "français/leçon_d'été_par_cœur.txt"


class TestSanitizeFileName:
    """Test suite for FilesStore().sanitize_file_name method."""

    def test_simple_file_name(self):
        """Test sanitization of a simple valid file name."""
        result = FilesStore().sanitize_file_name("file.txt")
        assert result == "file.txt"

    def test_file_name_with_spaces(self):
        """Test sanitization of file name with spaces."""
        result = FilesStore().sanitize_file_name("my file.txt")
        assert result == "my file.txt"

    def test_file_name_with_underscores(self):
        """Test sanitization of file name with underscores."""
        result = FilesStore().sanitize_file_name("my_file.txt")
        assert result == "my_file.txt"

    def test_file_name_with_hyphens(self):
        """Test sanitization of file name with hyphens."""
        result = FilesStore().sanitize_file_name("my-file.txt")
        assert result == "my-file.txt"

    def test_file_name_with_dots(self):
        """Test sanitization of file name with multiple dots."""
        result = FilesStore().sanitize_file_name("file.backup.txt")
        assert result == "file.backup.txt"

    def test_file_name_with_consecutive_dots(self):
        """Test that file names with consecutive dots are allowed."""
        result = FilesStore().sanitize_file_name("file..txt")
        assert result == "file..txt"

    def test_file_name_with_parentheses(self):
        """Test sanitization of file name with parentheses."""
        result = FilesStore().sanitize_file_name("file(1).txt")
        assert result == "file(1).txt"

    def test_file_name_with_square_brackets(self):
        """Test sanitization of file name with square brackets."""
        result = FilesStore().sanitize_file_name("file[draft].txt")
        assert result == "file[draft].txt"

    def test_file_name_with_colons(self):
        """Test sanitization of file name with colons."""
        result = FilesStore().sanitize_file_name("file:v1.txt")
        assert result == "file:v1.txt"

    def test_file_name_with_all_special_chars(self):
        """Test sanitization of file name with all allowed special characters."""
        result = FilesStore().sanitize_file_name("file(1)[draft]:backup-v2.txt")
        assert result == "file(1)[draft]:backup-v2.txt"

    def test_file_name_with_numbers(self):
        """Test sanitization of file name with numbers."""
        result = FilesStore().sanitize_file_name("file456.txt")
        assert result == "file456.txt"

    def test_newline_removal_in_file_name(self):
        """Test that newline characters are removed from file name."""
        result = FilesStore().sanitize_file_name("file\n.txt")
        assert result == "file.txt"

    def test_carriage_return_removal_in_file_name(self):
        """Test that carriage return characters are removed from file name."""
        result = FilesStore().sanitize_file_name("file\r.txt")
        assert result == "file.txt"

    def test_newline_and_carriage_return_removal_in_file_name(self):
        """Test that both newline and carriage return are removed from file name."""
        result = FilesStore().sanitize_file_name("file\r\n.txt")
        assert result == "file.txt"

    def test_none_file_name_raises_error(self):
        """Test that None file name raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_file_name(None)
        assert "Invalid file name: file name cannot be None" in str(exc_info.value)

    def test_file_name_with_path_separator_raises_error(self):
        """Test that file names with path separators raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_file_name("folder/file.txt")
        assert "Invalid file name: path separators not allowed" in str(exc_info.value)

    def test_file_name_with_multiple_path_separators_raises_error(self):
        """Test that file names with multiple path separators raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_file_name("folder/subfolder/file.txt")
        assert "Invalid file name: path separators not allowed" in str(exc_info.value)

    def test_forbidden_character_asterisk_in_file_name_raises_error(self):
        """Test that file names with asterisk raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_file_name("file*.txt")
        assert "Invalid file name: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_pipe_in_file_name_raises_error(self):
        """Test that file names with pipe character raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_file_name("file|.txt")
        assert "Invalid file name: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_quotes_in_file_name_raises_error(self):
        """Test that file names with quote characters raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_file_name('file".txt')
        assert "Invalid file name: contains forbidden characters" in str(exc_info.value)

    def test_forbidden_character_backslash_in_file_name_raises_error(self):
        """Test that file names with backslash raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FilesStore().sanitize_file_name("file\\name.txt")
        assert "Invalid file name: contains forbidden characters" in str(exc_info.value)

    def test_empty_file_name(self):
        """Test sanitization of empty file name."""
        result = FilesStore().sanitize_file_name("")
        assert result == ""

    def test_file_name_with_french_accents(self):
        """Test sanitization of file name with French accented characters."""
        result = FilesStore().sanitize_file_name("fichier_été.txt")
        assert result == "fichier_été.txt"
