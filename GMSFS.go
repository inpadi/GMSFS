package GMSFS

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
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
	name = filepath.Clean(name)
	if info.Exists {
		FileCache.Set(name, info)
	} else {
		FileCache.Remove(name)
	}
}

func GetFileInfo(name string) (FileInfo, bool) {
	name = filepath.Clean(name)
	fileInfo, ok := FileCache.Get(name)
	if ok {
		return fileInfo, ok
	}
	return FileInfo{}, false
}

// Function to add a file handle
func AddFileHandle(name string, file *os.File) {
	name = filepath.Clean(name)
	FileHandles.Set(name, file)
}

// Function to get a file handle
func GetFileHandle(name string) (*os.File, bool) {
	name = filepath.Clean(name)
	file, ok := FileHandles.Get(name)
	if ok {
		return file, ok
	}
	return nil, false
}

// Function to remove a file handle
func RemoveFileHandle(name string) {
	name = filepath.Clean(name)
	FileHandles.Remove(name)
}

func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	name = filepath.Clean(name)
	// Close any existing file handle first
	CloseFile(name)

	// Open the new file
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	// Add the new file handle to the map
	FileHandles.Set(name, file)

	// Reset or start a timer for this file handle
	resetTimer(name)

	return file, nil
}

func CloseFile(name string) {
	name = filepath.Clean(name)
	// Retrieve the file handle
	if file, ok := FileHandles.Get(name); ok {
		if ok {
			file.Close()
			FileHandles.Remove(name) // Remove after closing
		}
	}

	// Stop and remove the timer
	if timer, ok := FileTimers.Get(name); ok {
		if ok {
			timer.Stop()
			FileTimers.Remove(name) // Remove after stopping
		}
	}
}

func resetTimer(name string) {
	name = filepath.Clean(name)
	if timer, ok := FileTimers.Get(name); ok {
		timer.Reset(2 * time.Minute)
	} else {
		timer := time.AfterFunc(2*time.Minute, func() {
			CloseFile(name)
		})
		FileTimers.Set(name, timer)
	}
}

func Create(name string, content []byte) error {
	name = filepath.Clean(name)
	err := os.WriteFile(name, content, 0644)
	if err != nil {
		return err
	}

	UpdateFileInfo(name)
	UpdateDirectoryContents(filepath.Dir(name))

	return nil
}

func Delete(name string) error {
	name = filepath.Clean(name)
	// Close the file handle if it exists
	CloseFile(name)

	// Remove the file from the filesystem
	err := os.Remove(name)
	if err != nil {
		return err
	}

	// Update file info in the cache
	Update(name, FileInfo{Exists: false})

	// Optionally, update the directory contents in the cache
	// Assuming a function UpdateDirectoryContents is defined
	// UpdateDirectoryContents(filepath.Dir(name))

	return nil
}

func Append(name string, content []byte) error {
	name = filepath.Clean(name)
	var file *os.File
	var err error

	// Check if file handle exists in the map
	if temp, ok := FileHandles.Get(name); ok {
		file = temp
	} else {
		// If not, open the file and store the handle in the map
		file, err = os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		FileHandles.Set(name, file)
		defer file.Close() // Ensure the file is closed after writing
	}

	// Write the content to the file
	written, err := file.Write(content)
	if err != nil {
		return err
	}

	// Reset the timer for the file handle
	resetTimer(name)

	// Update file info in the cache
	UpdateFileInfoWithSize(name, int64(written))

	return nil
}

// UpdateFileInfoWithSize updates the FileInfo in the cache with the new size
func UpdateFileInfoWithSize(name string, sizeIncrement int64) {
	name = filepath.Clean(name)
	temp, ok := FileCache.Get(name)
	if ok {
		fileInfo := temp
		fileInfo.Size += sizeIncrement
		fileInfo.LastModified = time.Now() // Update the last modified time
		FileCache.Set(name, fileInfo)
	} else {
		// If the file is not in cache, retrieve the full info
		UpdateFileInfo(name)
	}
}

// Implement additional methods for Write, Move, etc., as needed

func UpdateFileInfo(name string) {
	name = filepath.Clean(name)
	// Create FileInfo struct
	var info FileInfo

	// Check if the file exists
	stat, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			// If the file does not exist, set Exists to false
			info = FileInfo{Exists: false}
		} else {
			// Handle other potential errors (e.g., log them)
			return
		}
	} else {
		// If the file exists, populate the FileInfo struct
		info = FileInfo{
			Exists:       true,
			Size:         stat.Size(),
			Mode:         stat.Mode(),
			LastModified: stat.ModTime(),
			IsDir:        stat.IsDir(),
		}
	}

	// Put the FileInfo into the FileCache
	FileCache.Set(name, info)
}

func UpdateDirectoryContents(dirName string) {
	dirName = filepath.Clean(dirName)

	files, err := os.ReadDir(dirName)
	if err != nil {
		// Handle error
		return
	}

	var contents []string
	for _, file := range files {
		contents = append(contents, file.Name())
	}

	// Retrieve the directory info from the cache, if it exists
	temp, ok := FileCache.Get(dirName)
	var dirInfo FileInfo
	if ok {
		dirInfo = temp
	} else {
		dirInfo = FileInfo{Exists: true, IsDir: true}
	}

	dirInfo.Contents = contents
	FileCache.Set(dirName, dirInfo)
}

func ReadFile(name string) ([]byte, error) {
	name = filepath.Clean(name)
	// Close any open file handle before reading
	CloseFile(name)

	// Read the file contents
	content, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}

	// Update the cache with the current file information
	UpdateFileInfo(name)

	return content, nil
}

func FileExists(name string) bool {
	name = filepath.Clean(name)
	// Check cache first
	if temp, ok := FileCache.Get(name); ok {
		fileInfo := temp
		return fileInfo.Exists
	}

	// If not in cache, check filesystem
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		// File does not exist; cache this information
		FileCache.Set(name, FileInfo{Exists: false})
		return false
	} else if err == nil {
		// File exists; update cache
		UpdateFileInfo(name)
		return true
	} else {
		// Some other error occurred
		// Handle error as needed
		return false
	}
}

func Mkdir(name string, perm os.FileMode) error {
	name = filepath.Clean(name)
	err := os.Mkdir(name, perm)
	if err != nil {
		return err
	}

	// Update the cache to reflect the new directory
	UpdateFileInfo(name)
	return nil
}

func MkdirAll(path string, perm os.FileMode) error {
	path = filepath.Clean(path)
	err := os.MkdirAll(path, perm)
	if err != nil {
		return err
	}

	// Update the cache for the top-level directory
	UpdateFileInfo(path)
	return nil
}

func AppendStringToFile(name string, content string) error {
	name = filepath.Clean(name)
	// Retrieve the file handle from the map
	if file, ok := FileHandles.Get(name); ok {
		// Write the content to the file
		_, err := file.Write([]byte(content))
		if err != nil {
			return err
		}

		// Reset the timer after appending
		resetTimer(name)
		return nil
	}

	// If the file is not open, open it first, append the content, and then close it
	file, err := OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write([]byte(content))
	return err
}

func WriteFile(name string, content []byte, perm os.FileMode) error {
	name = filepath.Clean(name)
	// Close any open file handle before writing
	CloseFile(name)

	// Write the new content to the file
	err := ioutil.WriteFile(name, content, perm)
	if err != nil {
		return err
	}

	// Update the cache with the new file information
	UpdateFileInfo(name)

	// Update the directory contents in the cache
	updateCacheWithNewFile(filepath.Dir(name), filepath.Base(name))

	return nil
}

func updateCacheWithNewFile(dirName, fileName string) {
	dirName = filepath.Clean(dirName)
	fileName = filepath.Clean(fileName)

	temp, ok := FileCache.Get(dirName)
	if ok {
		dirInfo := temp
		if dirInfo.Exists && dirInfo.IsDir {
			// Add the new file to the directory contents
			dirInfo.Contents = append(dirInfo.Contents, fileName)
			FileCache.Set(dirName, dirInfo)
		}
	}
}

func FileSize(name string) (int64, error) {
	name = filepath.Clean(name)
	// Check if file information is available in the cache
	if temp, ok := FileCache.Get(name); ok {
		fileInfo := temp
		if fileInfo.Exists {
			return fileInfo.Size, nil
		}
	}

	// If not in cache, get file size from the filesystem
	stat, err := os.Stat(name)
	if err != nil {
		// File does not exist or other error occurred
		return 0, err
	}

	// Update the cache with the new file information
	UpdateFileInfo(name)

	return stat.Size(), nil
}

func FileSizeZeroOnError(name string) int64 {
	name = filepath.Clean(name)
	// Check if file information is available in the cache
	if temp, ok := FileCache.Get(name); ok {
		fileInfo := temp
		if fileInfo.Exists {
			return fileInfo.Size
		}
	}

	// If not in cache, get file size from the filesystem
	stat, err := os.Stat(name)
	if err != nil {
		// File does not exist or other error occurred
		return 0
	}

	// Update the cache with the new file information
	UpdateFileInfo(name)

	return stat.Size()
}

func Rename(oldName, newName string) error {
	oldName = filepath.Clean(oldName)
	newName = filepath.Clean(newName)
	// Close any open file handles associated with the old name
	CloseFile(oldName)

	// Rename the file
	err := os.Rename(oldName, newName)
	if err != nil {
		return err
	}

	// Update the cache for the new file name
	if fileInfo, ok := FileCache.Get(oldName); ok {
		FileCache.Set(newName, fileInfo)
		FileCache.Remove(oldName)
	}

	// Handle any additional logic for updating directory contents if necessary

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

	// Update the cache for the new file
	UpdateFileInfo(dst)

	return
}

func Remove(name string) error {
	name = filepath.Clean(name)
	// Close any open file handle associated with the name
	CloseFile(name)

	// Remove the file from the filesystem
	err := os.Remove(name)
	if err != nil {
		return err
	}

	// Update the cache to reflect the file has been removed
	FileCache.Remove(name)

	// Optionally, update directory contents in the cache
	// UpdateDirectoryContents(filepath.Dir(name))

	return nil
}

func Stat(name string) (FileInfo, error) {
	name = filepath.Clean(name)
	// Check if file information is available in the cache
	if fileInfo, ok := FileCache.Get(name); ok {
		return fileInfo, nil
	}

	// If not in cache, get file info from the filesystem
	stat, err := os.Stat(name)
	if err != nil {
		return FileInfo{}, err
	}

	// Create FileInfo from os.FileInfo
	info := FileInfo{
		Exists:       true,
		Size:         stat.Size(),
		Mode:         stat.Mode(),
		LastModified: stat.ModTime(),
		IsDir:        stat.IsDir(),
	}

	// Optionally update the cache with this new information
	FileCache.Set(name, info)

	return info, nil
}

func CopyDir(src string, dst string) error {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	si, err := Stat(src)
	if err != nil {
		return err
	}
	if si.IsDir == false {
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

	entries, err := ReadDir(src)
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
			UpdateFileInfo(dstPath)
		}
	}

	return nil
}

func ReadDir(dirName string) ([]os.FileInfo, error) {
	dirName = filepath.Clean(dirName)

	// Check if directory contents are available in the cache
	if temp, ok := FileCache.Get(dirName); ok && temp.IsDir {
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
	FileCache.Set(dirName, FileInfo{
		Exists:   true,
		IsDir:    true,
		Contents: contents,
	})

	return fileInfos, nil
}

func RemoveAll(path string) error {
	path = filepath.Clean(path)
	// Update the cache to reflect the removal first
	err := updateCacheAfterRemoveAll(path)
	if err != nil {
		return err
	}

	// Then, remove the directory and its contents
	return os.RemoveAll(path)
}

func updateCacheAfterRemoveAll(path string) error {
	path = filepath.Clean(path)

	entries, err := os.ReadDir(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	for _, entry := range entries {
		fullPath := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			err := updateCacheAfterRemoveAll(fullPath) // Recursive call for subdirectories
			if err != nil {
				return err
			}
		}
		FileCache.Remove(fullPath)
	}

	// Remove the directory itself from the cache
	FileCache.Remove(path)

	return nil
}

func ListFS(path string) []string {
	var sysSlices []string

	path = filepath.Clean(path)

	// Check cache first
	if temp, ok := FileCache.Get(path); ok && temp.IsDir {
		for _, name := range temp.Contents {
			sysSlices = append(sysSlices, name)
		}
	} else {
		// If not in cache, read directory contents from the filesystem
		files, _ := ioutil.ReadDir(path)
		for _, f := range files {
			if f.IsDir() {
				sysSlices = append(sysSlices, "*"+f.Name())
			} else {
				sysSlices = append(sysSlices, f.Name())
			}
		}

		// Optionally, update the cache with the directory contents
		// Assuming you have a function UpdateDirectoryContents
		// UpdateDirectoryContents(path)
	}

	return sysSlices
}

func RecurseFS(path string) (sysSlices []string) {
	path = filepath.Clean(path)

	temp, ok := FileCache.Get(path)
	var files []os.FileInfo

	if ok && temp.IsDir {
		// Use cached directory contents
		for _, name := range temp.Contents {
			fileInfo, err := os.Stat(filepath.Join(path, name))
			if err != nil {
				continue // Handle error as needed
			}
			files = append(files, fileInfo)
		}
	} else {
		// Read from filesystem if not in cache
		var err error
		files, err = ioutil.ReadDir(path)
		if err != nil {
			return // Handle error as needed
		}
		// Optionally update cache with new directory contents
		// Assuming you have a function UpdateDirectoryContents
		UpdateDirectoryContents(path)
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
