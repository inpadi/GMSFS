package GMSFS

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
)

// FileInfo stores comprehensive metadata about a file or directory
type FileInfo struct {
	Exists       bool
	Size         int64
	Mode         os.FileMode
	LastModified time.Time
	IsDir        bool
	Contents     []string // Names of files for directories
	OriginalName string
}

// Global variables for caches
var FileCache = cmap.New[FileInfo]()
var FileHandleCache = cmap.New[*os.File]()

// Global variables for file handles and timers
var FileHandles = cmap.New[*os.File]()
var FileTimers = cmap.New[*time.Timer]()

// FileHandleInstance to store file and timer information
type FileHandleInstance struct {
	File  *os.File
	Timer *time.Timer
}

func Update(name string, info FileInfo) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))

	if info.Exists {
		// Preserve the original name in FileInfo
		namex := filepath.Base(name)
		info.OriginalName = namex
		FileCache.Set(lowerCaseName, info)
	} else {
		FileCache.Remove(lowerCaseName)
	}
}

func GetFileInfo(name string) (FileInfo, bool) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	fileInfo, ok := FileCache.Get(lowerCaseName)
	if ok {
		return fileInfo, true
	}
	return FileInfo{}, false
}

func AddFileHandle(name string, file *os.File) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	FileHandles.Set(lowerCaseName, file)
}

// Function to get a file handle
func GetFileHandle(name string) (*os.File, bool) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	file, ok := FileHandles.Get(lowerCaseName)
	if ok {
		return file, ok
	}
	return nil, false
}

// Function to remove a file handle
func RemoveFileHandle(name string) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	FileHandles.Remove(lowerCaseName)
}

func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	// Close any existing file handle first
	CloseFile(lowerCaseName)

	// Open the new file
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	// Add the new file handle to the map
	FileHandles.Set(lowerCaseName, file)

	// Reset or start a timer for this file handle
	resetTimer(lowerCaseName)

	return file, nil
}

func CloseFile(name string) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	if file, ok := FileHandles.Get(lowerCaseName); ok {
		file.Close()
		FileHandles.Remove(lowerCaseName)
	}
	if timer, ok := FileTimers.Get(lowerCaseName); ok {
		timer.Stop()
		FileTimers.Remove(lowerCaseName)
	}
}

func resetTimer(name string) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	if timer, ok := FileTimers.Get(lowerCaseName); ok {
		timer.Reset(2 * time.Minute)
	} else {
		timer := time.AfterFunc(2*time.Minute, func() {
			CloseFile(lowerCaseName)
		})
		FileTimers.Set(lowerCaseName, timer)
	}
}

func Create(name string) (*os.File, error) {
	name = filepath.Clean(name)
	lowerCaseName := strings.ToLower(name)

	file, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	fileInfo := FileInfo{
		Exists:       true,
		Size:         0, // New file has size 0
		Mode:         stat.Mode(),
		LastModified: stat.ModTime(),
		IsDir:        false,
		OriginalName: name,
	}
	FileCache.Set(lowerCaseName, fileInfo)

	return file, nil
}

func Open(name string) (*os.File, error) {
	name = filepath.Clean(name)
	lowerCaseName := strings.ToLower(name)

	// Open the file using os.Open
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	// Check if file info is already in the cache
	if _, ok := FileCache.Get(lowerCaseName); !ok {
		// If not in cache, get file info and update cache
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}

		fileInfo := FileInfo{
			Exists:       true,
			Size:         stat.Size(),
			Mode:         stat.Mode(),
			LastModified: stat.ModTime(),
			IsDir:        stat.IsDir(),
			OriginalName: name,
		}
		FileCache.Set(lowerCaseName, fileInfo)
	}

	return file, nil
}

func Delete(name string) error {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	// Close the file handle if it exists
	CloseFile(lowerCaseName)

	// Remove the file from the filesystem
	err := os.Remove(name) // Use original case for filesystem operations
	if err != nil {
		return err
	}

	// Update file info in the cache
	Update(lowerCaseName, FileInfo{Exists: false})

	// Optionally, update the directory contents in the cache
	UpdateDirectoryContents(filepath.Dir(lowerCaseName))

	return nil
}

func Append(name string, content []byte) error {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	var file *os.File
	var err error

	// Check if file handle exists in the map
	if temp, ok := FileHandles.Get(lowerCaseName); ok {
		file = temp
	} else {
		// If not, open the file and store the handle in the map
		file, err = os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		FileHandles.Set(lowerCaseName, file)
		defer file.Close() // Ensure the file is closed after writing
	}

	// Write the content to the file
	written, err := file.Write(content)
	if err != nil {
		return err
	}

	// Reset the timer for the file handle
	resetTimer(lowerCaseName)

	// Update file info in the cache
	UpdateFileInfoWithSize(lowerCaseName, int64(written))

	return nil
}

func UpdateFileInfoWithSize(name string, sizeIncrement int64) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	if fileInfo, ok := FileCache.Get(lowerCaseName); ok {
		updatedFileInfo := fileInfo
		updatedFileInfo.Size += sizeIncrement
		updatedFileInfo.LastModified = time.Now() // Update the last modified time
		FileCache.Set(lowerCaseName, updatedFileInfo)
	} else {
		// If the file is not in cache, retrieve the full info
		UpdateFileInfo(name)
	}
}

func UpdateFileInfo(name string) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	var info FileInfo

	// Check if the file exists
	stat, err := os.Stat(name) // Use the original case for filesystem operations
	if err != nil {
		if os.IsNotExist(err) {
			info = FileInfo{Exists: false}
		} else {
			return // Handle other potential errors
		}
	} else {
		info = FileInfo{
			Exists:       true,
			Size:         stat.Size(),
			Mode:         stat.Mode(),
			LastModified: stat.ModTime(),
			IsDir:        stat.IsDir(),
			OriginalName: stat.Name(), // Preserve the original file name
		}
	}

	// Update the FileCache
	FileCache.Set(lowerCaseName, info)
}

func UpdateDirectoryContents(dirName string) {
	lowerCaseDirName := strings.ToLower(filepath.Clean(dirName))

	files, err := os.ReadDir(dirName) // Use the original case for filesystem operations
	if err != nil {
		return // Handle error
	}

	var contents []string
	for _, file := range files {
		contents = append(contents, file.Name())
	}

	dirNameOnly := filepath.Base(dirName) // Get only the directory name
	dirInfo := FileInfo{Exists: true, IsDir: true, OriginalName: dirNameOnly, Contents: contents}

	FileCache.Set(lowerCaseDirName, dirInfo)
}

func ReadFile(name string) ([]byte, error) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	// Close any open file handle before reading
	CloseFile(lowerCaseName)

	// Read the file contents
	content, err := os.ReadFile(name) // Use the original case for filesystem operations
	if err != nil {
		return nil, err
	}

	// Update the cache with the current file information
	UpdateFileInfo(name) // Use the original case for updating FileInfo

	return content, nil
}

func FileExists(name string) bool {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	if temp, ok := FileCache.Get(lowerCaseName); ok {
		fileInfo := temp
		return fileInfo.Exists
	}

	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		FileCache.Set(lowerCaseName, FileInfo{Exists: false})
		return false
	} else if err == nil {
		UpdateFileInfo(name)
		return true
	}
	return false
}

func Mkdir(name string, perm os.FileMode) error {
	name = filepath.Clean(name) // Preserve original name for file operation
	err := os.Mkdir(name, perm)
	if err != nil {
		return err
	}

	UpdateFileInfo(name) // Use the original name
	return nil
}

func MkdirAll(path string, perm os.FileMode) error {
	path = filepath.Clean(path) // Preserve original path for file operation
	err := os.MkdirAll(path, perm)
	if err != nil {
		return err
	}

	UpdateFileInfo(path) // Use the original path
	return nil
}

func AppendStringToFile(name string, content string) error {
	lowerCaseName := strings.ToLower(name)

	// Open or create the file
	file, err := OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Append the content
	written, err := file.Write([]byte(content))
	if err != nil {
		return err
	}

	// Update file size in the cache
	if fileInfo, ok := FileCache.Get(lowerCaseName); ok {
		updatedFileInfo := fileInfo
		updatedFileInfo.Size += int64(written) // Update the size
		FileCache.Set(lowerCaseName, updatedFileInfo)
	} else {
		// If file info is not in cache, fetch and update it
		UpdateFileInfo(name)
	}

	return nil
}

func WriteFile(name string, content []byte, perm os.FileMode) error {
	name = filepath.Clean(name)
	lowerCaseName := strings.ToLower(name)

	// Close any open file handle before writing
	CloseFile(lowerCaseName)

	// Write the new content to the file
	err := os.WriteFile(name, content, perm)
	if err != nil {
		return err
	}

	// Update the cache with the new file information
	UpdateFileInfo(name) // Use the original name for updating FileInfo

	// Update the directory contents in the cache
	updateCacheWithNewFile(filepath.Dir(name), filepath.Base(name))

	return nil
}

func updateCacheWithNewFile(dirName, fileName string) {
	lowerCaseDirName := strings.ToLower(filepath.Clean(dirName))

	if temp, ok := FileCache.Get(lowerCaseDirName); ok {
		dirInfo := temp
		if dirInfo.Exists && dirInfo.IsDir {
			// Add the new file to the directory contents
			dirNameOnly := filepath.Base(dirName)
			dirInfo.Contents = append(dirInfo.Contents, dirNameOnly)
			FileCache.Set(lowerCaseDirName, dirInfo)
		}
	}
}

func FileSize(name string) (int64, error) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))

	// Check if file information is available in the cache
	if fileInfo, ok := FileCache.Get(lowerCaseName); ok {
		if fileInfo.Exists {
			return fileInfo.Size, nil
		}
	}

	// If not in cache, get file size from the filesystem
	stat, err := os.Stat(name) // Original name for filesystem operation
	if err != nil {
		return 0, err // File does not exist or other error occurred
	}

	// Update the cache with the new file information
	UpdateFileInfo(name) // Original name for updating FileInfo

	return stat.Size(), nil
}

func FileSizeZeroOnError(name string) int64 {
	lowerCaseName := strings.ToLower(filepath.Clean(name))

	// Check if file information is available in the cache
	if fileInfo, ok := FileCache.Get(lowerCaseName); ok {
		if fileInfo.Exists {
			return fileInfo.Size
		}
	}

	// If not in cache, get file size from the filesystem
	stat, err := os.Stat(name) // Original name for filesystem operation
	if err != nil {
		return 0 // Return 0 if file does not exist or other error occurred
	}

	// Update the cache with the new file information
	UpdateFileInfo(name) // Original name for updating FileInfo

	return stat.Size()
}

func Rename(oldName, newName string) error {
	lowerOldName := strings.ToLower(filepath.Clean(oldName))
	lowerNewName := strings.ToLower(filepath.Clean(newName))

	CloseFile(lowerOldName)

	err := os.Rename(oldName, newName)
	if err != nil {
		return err
	}

	if fileInfo, ok := FileCache.Get(lowerOldName); ok {
		dirNameOnly := filepath.Base(newName)
		fileInfo.OriginalName = dirNameOnly
		FileCache.Set(lowerNewName, fileInfo)
		FileCache.Remove(lowerOldName)
	}

	return nil
}

func CopyFile(src, dst string) (err error) {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		if e := out.Close(); e != nil {
			err = e
		}
	}()

	_, err = io.Copy(out, in)
	if err != nil {
		return
	}

	err = out.Sync()
	if err != nil {
		return
	}

	si, err := os.Stat(src)
	if err != nil {
		return
	}
	err = os.Chmod(dst, si.Mode())
	if err != nil {
		return
	}

	UpdateFileInfo(dst)

	return
}

func Remove(name string) error {
	lowerCaseName := strings.ToLower(filepath.Clean(name))

	CloseFile(lowerCaseName)

	err := os.Remove(name)
	if err != nil {
		return err
	}

	FileCache.Remove(lowerCaseName)

	return nil
}

func Stat(name string) (FileInfo, error) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))

	// Check if file information is available in the cache
	if fileInfo, ok := FileCache.Get(lowerCaseName); ok {
		return fileInfo, nil
	}

	// If not in cache, get file info from the filesystem
	stat, err := os.Stat(name)
	if err != nil {
		return FileInfo{}, err
	}

	dirNameOnly := filepath.Base(name)
	// Create FileInfo from os.FileInfo
	info := FileInfo{
		Exists:       true,
		Size:         stat.Size(),
		Mode:         stat.Mode(),
		LastModified: stat.ModTime(),
		IsDir:        stat.IsDir(),
		OriginalName: dirNameOnly, // Store the original name
	}

	// Update the cache with this new information
	FileCache.Set(lowerCaseName, info)

	return info, nil
}

func CopyDir(src string, dst string) error {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	si, err := Stat(src) // Stat uses cache
	if err != nil {
		return err
	}
	if !si.IsDir {
		return fmt.Errorf("source is not a directory")
	}

	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		return fmt.Errorf("destination already exists")
	}

	err = os.MkdirAll(dst, si.Mode)
	if err != nil {
		return err
	}
	UpdateFileInfo(dst) // Update cache for the new directory

	entries, err := ReadDir(src) // ReadDir uses cache
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			err = CopyDir(srcPath, dstPath)
			if err != nil {
				return err
			}
		} else {
			// Skip symlinks
			if entry.Mode()&os.ModeSymlink != 0 {
				continue
			}

			err = CopyFile(srcPath, dstPath)
			if err != nil {
				return err
			}
			UpdateFileInfo(dstPath) // Update cache for the new file
		}
	}

	return nil
}

func ReadDir(dirName string) ([]os.FileInfo, error) {
	lowerCaseDirName := strings.ToLower(filepath.Clean(dirName))

	// Check if directory contents are available in the cache
	if temp, ok := FileCache.Get(lowerCaseDirName); ok && temp.IsDir {
		var fileInfos []os.FileInfo
		for _, fileName := range temp.Contents {
			fileInfo, err := os.Stat(filepath.Join(dirName, fileName))
			if err != nil {
				return nil, err // Handle error appropriately
			}
			fileInfos = append(fileInfos, fileInfo)
		}
		return fileInfos, nil
	}

	// If not in cache, read directory contents from the filesystem
	fileEntries, err := os.ReadDir(dirName)
	if err != nil {
		return nil, err
	}

	var contents []string
	var fileInfos []os.FileInfo
	for _, entry := range fileEntries {
		fileInfo, err := entry.Info()
		if err != nil {
			return nil, err
		}
		fileInfos = append(fileInfos, fileInfo)
		contents = append(contents, fileInfo.Name())
	}
	dirNameOnly := filepath.Base(dirName)
	FileCache.Set(lowerCaseDirName, FileInfo{
		Exists:       true,
		IsDir:        true,
		Contents:     contents,
		OriginalName: dirNameOnly,
	})

	return fileInfos, nil
}

func RemoveAll(path string) error {
	path = filepath.Clean(path)
	err := updateCacheAfterRemoveAll(strings.ToLower(path))
	if err != nil {
		return err
	}

	return os.RemoveAll(path)
}

func updateCacheAfterRemoveAll(path string) error {
	lowerCasePath := strings.ToLower(filepath.Clean(path))

	entries, err := os.ReadDir(path) // Original case for filesystem operation
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	for _, entry := range entries {
		fullPath := filepath.Join(path, entry.Name())
		lowerCaseFullPath := strings.ToLower(fullPath)
		if entry.IsDir() {
			err := updateCacheAfterRemoveAll(lowerCaseFullPath)
			if err != nil {
				return err
			}
		}
		FileCache.Remove(lowerCaseFullPath)
	}

	FileCache.Remove(lowerCasePath)
	return nil
}

func ListFS(path string) []string {
	var sysSlices []string
	lowerCasePath := strings.ToLower(filepath.Clean(path))

	// First, check if the path is a directory
	fileInfo, err := Stat(path)
	if err != nil || !fileInfo.IsDir {
		return sysSlices // Return empty slice if it's not a directory
	}

	if temp, ok := FileCache.Get(lowerCasePath); ok && temp.IsDir {
		if len(temp.Contents) == 0 {
			UpdateDirectoryContents(path)
		}
		for _, name := range temp.Contents {
			fullPath := filepath.Join(path, name)
			f, err := Stat(fullPath)
			if err == nil {
				if f.IsDir {
					sysSlices = append(sysSlices, "*"+f.OriginalName)
				} else {
					sysSlices = append(sysSlices, f.OriginalName)
				}
			}
		}
	} else {
		// Read directory contents from the filesystem
		UpdateDirectoryContents(path)
		return ListFS(path) // Recurse with updated cache
	}

	return sysSlices
}

func RecurseFS(path string) (sysSlices []string) {
	lowerCasePath := strings.ToLower(filepath.Clean(path))

	temp, ok := FileCache.Get(lowerCasePath)
	var files []os.FileInfo

	if ok && temp.IsDir {
		for _, name := range temp.Contents {
			fileInfo, err := os.Stat(filepath.Join(path, name)) // Use original path for stat
			if err != nil {
				continue // Handle error as needed
			}
			files = append(files, fileInfo)
		}
	} else {
		var err error
		files, err = ioutil.ReadDir(path) // Read from filesystem if not in cache
		if err != nil {
			return // Handle error as needed
		}
		// Update the cache here if needed
	}

	for _, f := range files {
		fullPath := filepath.Join(path, f.Name())
		if f.IsDir() {
			sysSlices = append(sysSlices, "*"+fullPath)
			sysSlices = append(sysSlices, RecurseFS(fullPath)...)
		} else {
			sysSlices = append(sysSlices, fullPath)
		}
	}

	return sysSlices
}

func FileAgeInSec(filename string) (age time.Duration, err error) {
	lowerCaseFilename := strings.ToLower(filepath.Clean(filename))

	// Check if file information is available in the cache
	fileInfo, ok := FileCache.Get(lowerCaseFilename)
	if !ok {
		// If not in cache, get file info from the filesystem and update the cache
		var stat os.FileInfo
		stat, err = os.Stat(filename)
		if err != nil {
			return -1, err
		}

		fileInfo = FileInfo{
			Exists:       true,
			Size:         stat.Size(),
			Mode:         stat.Mode(),
			LastModified: stat.ModTime(),
			IsDir:        stat.IsDir(),
			OriginalName: filename,
		}
		FileCache.Set(lowerCaseFilename, fileInfo)
	}

	return time.Now().Sub(fileInfo.LastModified), nil
}
