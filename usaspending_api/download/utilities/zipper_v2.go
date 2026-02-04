package main

import (
    "archive/zip"
    "flag"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "time"
)

func main() {
    //Create a zip file of the specified files.

    startTime := time.Now()

    // CLI flags
    outPath := flag.String("out", "", "Path to output zip file (required)")
    flag.Parse()

    if *outPath == "" {
        fmt.Println("Error: -out is required")
        flag.Usage()
        os.Exit(1)
    }

    // The remaining arguments are the files to be zipped
    files := flag.Args()
    if len(files) == 0 {
        fmt.Println("Error: no files specified")
        flag.Usage()
        os.Exit(1)
    }

    // Create the output directory, if needed
    if dir := filepath.Dir(*outPath); dir != "." && dir != "" {
        if err := os.MkdirAll(dir, 0755); err != nil {
            panic(err)
        }
    }

    // Create zip file
    zipfile, err := os.Create(*outPath)
    if err != nil {
        panic(err)
    }
    defer zipfile.Close()

    zw := zip.NewWriter(zipfile)
    defer zw.Close()

    for _, fpath := range files {
        if err := addFileToZip(zw, fpath); err != nil {
            panic(err)
        }
    }

    fmt.Println("Created %s with %d files in %s\n", *outPath, len(files), time.Since(startTime))
}


func addFileToZip(zw *zip.Writer, path string) error {
    // Add the given file to the zip archive

    file, err := os.Open(path)
    if err != nil {
        return err
    }
    defer file.Close()

    // Use base name so the zip entries don't include full directory names
    name := filepath.Base(path)

    w, err := zw.Create(name)
    if err != nil {
        return err
    }

    _, err = io.Copy(w, file)
    return err
}
