import json
from enacit4r_files.models.files import FileRef
from enacit4r_files.utils.files import FileNodeBuilder

def test_empty_file_node():
  file_node = FileNodeBuilder.from_name(name=".").build()
  assert file_node.name == "."
  assert file_node.path == None
  assert file_node.size == None
  assert file_node.alt_name == None
  assert file_node.alt_path == None
  assert file_node.alt_size == None
  assert file_node.is_file == False
  assert file_node.children == []

def test_single_file_node():
  ref = FileRef(name="file.txt", path="/file.txt", size=100)
  file_node = FileNodeBuilder.from_ref(ref).build()
  assert file_node.name == "file.txt"
  assert file_node.path == "/file.txt"
  assert file_node.size == 100
  assert file_node.alt_name == None
  assert file_node.alt_path == None
  assert file_node.alt_size == None
  assert file_node.is_file == True
  assert file_node.children == []


def test_single_file_node_with_alt():
  ref = FileRef(name="file.webp", path="/file.webp", size=50, alt_name="file.png", alt_path="/file.png", alt_size=100)
  file_node = FileNodeBuilder.from_ref(ref).build()
  assert file_node.name == "file.webp"
  assert file_node.path == "/file.webp"
  assert file_node.size == 50
  assert file_node.alt_name == "file.png"
  assert file_node.alt_path == "/file.png"
  assert file_node.alt_size == 100
  assert file_node.is_file == True
  assert file_node.children == []

def test_multiple_file_node():
  refs = [
    FileRef(name="README.md", path="README.md", size=100),
    FileRef(name="file.txt", path="docs/file.txt", size=100),
    FileRef(name="file.webp", path="pub/images/file.webp", size=50, alt_name="file.png", alt_path="pub/images/file.png", alt_size=100)
  ]
  file_node = FileNodeBuilder.from_name(name=".", path=".").add_files(refs).build()
  #[print(node.model_dump_json(indent = 2)) for node in file_node.children]
  assert file_node.name == "."
  assert file_node.path == "."
  assert file_node.size == None
  assert file_node.alt_name == None
  assert file_node.alt_path == None
  assert file_node.alt_size == None
  assert file_node.is_file == False
  assert len(file_node.children) == 3
  for child_node in file_node.children:
    if child_node.name == "README.md":
      assert child_node.path == "README.md"
      assert child_node.size == 100
      assert child_node.alt_name == None
      assert child_node.alt_path == None
      assert child_node.alt_size == None
      assert child_node.is_file == True
      assert child_node.children == []
    elif child_node.name == "docs":
      assert child_node.path == "docs"
      assert child_node.size == None
      assert child_node.alt_name == None
      assert child_node.alt_path == None
      assert child_node.alt_size == None
      assert child_node.is_file == False
      assert len(child_node.children) == 1
      for grandchild_node in child_node.children:
        assert grandchild_node.name == "file.txt"
        assert grandchild_node.path == "docs/file.txt"
        assert grandchild_node.size == 100
        assert grandchild_node.alt_name == None
        assert grandchild_node.alt_path == None
        assert grandchild_node.alt_size == None
        assert grandchild_node.is_file == True
        assert grandchild_node.children == []
    elif child_node.name == "pub":
      assert child_node.path == "pub"
      assert child_node.size == None
      assert child_node.alt_name == None
      assert child_node.alt_path == None
      assert child_node.alt_size == None
      assert child_node.is_file == False
      assert len(child_node.children) == 1
      for grandchild_node in child_node.children:
        assert grandchild_node.name == "images"
        assert grandchild_node.path == "pub/images"
        assert grandchild_node.size == None
        assert grandchild_node.alt_name == None
        assert grandchild_node.alt_path == None
        assert grandchild_node.alt_size == None
        assert grandchild_node.is_file == False
        assert len(grandchild_node.children) == 1
        for greatgrandchild_node in grandchild_node.children:
          assert greatgrandchild_node.name == "file.webp"
          assert greatgrandchild_node.path == "pub/images/file.webp"
          assert greatgrandchild_node.size == 50
          assert greatgrandchild_node.alt_name == "file.png"
          assert greatgrandchild_node.alt_path == "pub/images/file.png"
          assert greatgrandchild_node.alt_size == 100
          assert greatgrandchild_node.is_file == True
          assert greatgrandchild_node.children == []    
    
