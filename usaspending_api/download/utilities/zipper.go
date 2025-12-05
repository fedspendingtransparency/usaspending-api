package main

import (
    "C"
    "archive/zip"
    "io"
    "os"
    "path/filepath"
    "unsafe"
)

//export AppendFilesToZipFile
func AppendFilesToZipFile(filePaths **C.char, fileCount C.int, zipFilePath *C.char) C.int {
        // Convert C strings to Go strings
        goZipPath := C.GoString(zipFilePath)

        // Convert array of C strings to Go slice of strings
        goFilePaths := make([]string, fileCount)
        filePathsSlice := (*[1 << 30]*C.char)(unsafe.Pointer(filePaths))[:fileCount:fileCount]
        for i, cPath := range filePathsSlice {
                goFilePaths[i] = C.GoString(cPath)
        }

        // Create the directory if it doesn't exist
        zipDir := filepath.Dir(goZipPath)
        if zipDir != "." && zipDir != "" {
                err := os.MkdirAll(zipDir, 0755)
                if err != nil {
                        return 1
                }
        }

        // Open or create the zip file
        zipFile, err := os.OpenFile(goZipPath, os.O_CREATE|os.O_RDWR, 0666)
        if err != nil {
                return 1
        }
        defer zipFile.Close()

        // Create a zip writer
        zipWriter := zip.NewWriter(zipFile)
        defer zipWriter.Close()

        // Add each file to the zip archive
        for _, filePath := range goFilePaths {
                // Open the file to add
                file, err := os.Open(filePath)
                if err != nil {
                        return 1
                }
                defer file.Close()

                // Get the base name of the file (archive name)
                archiveName := filepath.Base(filePath)

                // Create a header for the file in the zip
                header := &zip.FileHeader{
                        Name:   archiveName,
                        Method: zip.Deflate,
                }

                // Write the file to the zip archive
                writer, err := zipWriter.CreateHeader(header)
                if err != nil {
                        return 1
                }

                _, err = io.Copy(writer, file)
                if err != nil {
                        return 1
                }
        }

        return 0
}

func main() {}
