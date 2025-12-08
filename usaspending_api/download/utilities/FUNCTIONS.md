# About
Documentation about non-Python functions, their signatures and how to use them.

---

### `AppendFilesToZipFile`

**Signature:**
```C
int AppendFilesToZipFile(char** filePaths, int fileCount, char* zipFilePath)
```

**Parameters:**
- `filePaths` (char**): Array of C strings (byte strings) representing file paths to add to the zip
- `fileCount` (int): Number of files in the filePaths array
- `zipFilePath` (char*): Path where the zip file will be created/appended

**Returns:**
- `int`: 0 on success, 1 on error

**Python Usage:**
```python
from ctypes import *

lib = CDLL('./libziputil.so')
lib.AppendFilesToZipFile.argtypes = [POINTER(c_char_p), c_int, c_char_p]  # Document the function argument data types
lib.AppendFilesToZipFile.restype = c_int  # Document the function response data type

file_paths = [b'/path/to/file1.txt', b'/path/to/file2.txt']
c_file_paths = (c_char_p * len(file_paths))(*file_paths)
result = lib.AppendFilesToZipFile(c_file_paths, len(file_paths), b'/output/archive.zip')
```
