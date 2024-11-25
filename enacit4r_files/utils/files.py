from fastapi.exceptions import HTTPException
from fastapi.datastructures import UploadFile
from enacit4r_files.models.files import FileRef, FileNode
from urllib.parse import quote

# 100 MB in binary
DEFAULT_MAX_FILE_SIZE = 100 * 1024 * 1024

# verify extension/content-type is valid
pdf_mimetypes = [
    "application/acrobat", "application/pdf", "application/x-pdf", "text/pdf",
    "text/x-pdf"
]
png_mimetypes = ["image/png", "application/png", "application/x-png"]
jpg_mimetypes = [
    "image/jpg", "application/jpg", "application/x-jpg", "image/jpeg",
    "application/jpeg"
]
gif_mimetypes = ["image/gif"]
binary_mimetypes = ["application/octet-stream"]
text_mimetypes = ["text/plain", "text/csv"]
other_images: list[str] = ["image/bmp", "image/webp"]
image_mimetypes: list[str] = png_mimetypes + \
    jpg_mimetypes + gif_mimetypes + other_images
zip_mimetypes: list[str] = ["application/zip", "application/x-zip-compressed"]


class FileChecker:
    """A class that checks the size of files
    """

    def __init__(self, max_size: int = DEFAULT_MAX_FILE_SIZE):
        self.max_size = max_size

    async def check_size(self, files: list[UploadFile]):
        for file in files:
            content = await file.read()
            await self._check_content_size(content)
            await file.seek(0)
        return files
    
    async def _check_content_size(self, content: bytes | str):
        file_size = len(content)
        if file_size > self.max_size:
            detail = f"File size {file_size} exceeds max size {self.max_size}"
            raise HTTPException(400, detail=detail)

class FileNodeBuilder:
    """A node in a tree representing a file system, used to represent a list of file objects in S3
    """

    def __init__(self, name, path=None, size=None, alt_name=None, alt_path=None, alt_size=None, is_file=False):
        self.root = FileNode(name = name,
                             path = path,
                             size = size,
                             alt_name = alt_name,
                             alt_path = alt_path,
                             alt_size = alt_size,
                             is_file=is_file)

    @classmethod
    def from_name(cls, name: str, path: str = None, size: int = None):
        """Make a root file node from which children will be added.

        Args:
            name (str): The root node name

        Returns:
            FileNodeBuilder: The builder
        """
        return cls(name = name, path = path, size = size, is_file = False)

    @classmethod
    def from_ref(cls, file_ref: FileRef):
        """Make a single file node representing a file reference in S3.

        Args:
            file_ref (FileRef): The file reference

        Returns:
            FileNodeBuilder: The builder
        """
        return cls(name = file_ref.name, path = file_ref.path, size = file_ref.size,
                   is_file = True,
                   alt_name = file_ref.alt_name, alt_path = file_ref.alt_path, alt_size = file_ref.alt_size)

    def add_files(self, file_refs: list[FileRef]):
        for file_ref in file_refs:
            self.add_file(file_ref)
        return self

    def add_file(self, file_ref: FileRef):
        """Add a file to the tree, from the root node makes the intermediate folder nodes.

        Args:
            file_ref (FileRef): A dictionary representing a file object in S3
        """
        current_node = self.root
        parts = file_ref.path.split("/")
        current_parts = []

        for part in parts:
            current_parts.append(part)
            matching_child = next(
                (child for child in current_node.children if child.name == part), None)

            if matching_child is None:
                is_file = True if part == parts[-1] else False
                new_path = file_ref.path if is_file else quote("/".join(
                    current_parts), safe="/")
                if not is_file:
                    new_path = file_ref.path.split(new_path)[0] + new_path
                new_size = file_ref.size if is_file else None
                new_mime_type = file_ref.mime_type if is_file else None
                new_node = FileNode(name = part, path = new_path, size = new_size, mime_type=new_mime_type, is_file = is_file)
                if is_file and file_ref.alt_name:
                    new_node.alt_name = file_ref.alt_name
                    new_node.alt_path = file_ref.alt_path
                    new_node.alt_size = file_ref.alt_size
                    new_node.alt_mime_type = file_ref.alt_mime_type
                current_node.children.append(new_node)
                current_node = new_node
            else:
                current_node = matching_child
                
        return self

    def build(self) -> FileNode:
        """Get the root of the tree of file nodes.

        Returns:
            FileNode: The root file node
        """
        return self.root
