package GMSFS

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto"
)

// FileInfo stores comprehensive metadata about a file or directory
type FileInfo struct {
	Exists       bool
	Size         int64
	Mode         os.FileMode
	LastModified time.Time
	IsDir        bool
	Contents     []FileInfo // Names of files for directories
	Name         string
	CacheTime    time.Time
}

type CachedFile struct {
	*os.File
	path string
}

const timeFlat = "20060102_1504"

// FileHandleInstance to store file and timer information
type FileHandleInstance struct {
	File  *os.File
	Timer *time.Timer
}

type CacheItem struct {
	Value     interface{}
	Timestamp time.Time
}

var cache = initCache()
var MaxCacheTime = 300 * time.Second
var MacCacheDirDepth = 4

func initCache() (cache *ristretto.Cache[string, CacheItem]) {
	cache, err := ristretto.NewCache[string, CacheItem](&ristretto.Config[string, CacheItem]{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		log.Fatal(err)
	}
	return cache
}

func CacheAdd(key string, value FileInfo) {
	// set a value with a cost of 1
	ks := strings.Split(key, "/")
	//	if len(ks) > MacCacheDirDepth {
	//		return
	//	}
	cache.Set(key, CacheItem{Value: value, Timestamp: time.Now()}, int64(len(ks)))
}

func CacheGet(key string) (FileInfo, bool) {
	item, found := cache.Get(key)
	if !found {
		return FileInfo{}, false
	}
	value := item.Value.(FileInfo) // Type assert to FileInfo
	// Check if the item has expired
	if time.Since(value.CacheTime) > MaxCacheTime {
		CacheDelete(key)
		return FileInfo{}, false
	}
	return value, true
}

func CacheDelete(key string) {
	cache.Del(key)
}

func errorPrinter(log string, object string) {
	go invistiageError(object)
	if _, err := os.Stat("GMSFS.Debug"); err != nil {
		if os.IsNotExist(err) {
			return
		}
	}

	stack := ""
	pc, _, _, ok := runtime.Caller(2) // 2 level up the call stack
	if ok {
		fn := runtime.FuncForPC(pc)
		if fn != nil {
			file, line := fn.FileLine(fn.Entry())
			stack = " (2):" + fn.Name() + " file: " + file + " line: " + strconv.Itoa(line)
		}
	}

	AppendStringToFile("GMSFS."+time.Now().Format(timeFlat)+".log", log+" stacktrace: "+stack+"\r\n")
}

// This function catches errors from the filecache - some errors can be that a file
// was tried to op read that does not exist, other can be cache objects not consisten
// with file system and needs to be fixed.
func invistiageError(name string) {
	if name == "" {
		return
	}
	name = cleanPath(name)
	fmt.Println("Invistiage object: " + name)
	_, ok := CacheGet(strings.ToLower(cleanPath(name)))
	if ok == true {
		_, err := os.Stat(cleanPath(name))
		if err != nil {
			//We know the filesystem seems to have a issue with this object, so we clean it form the cache
			CacheDelete(cleanPath(name))
			UpdateDirectoryContents(filepath.Dir(strings.ToLower(cleanPath(name))))
		}
	}
}

func cleanPath(path string) string {
	path = filepath.Clean(path)
	fs := strings.SplitN(path, ":", 2)
	if len(fs) == 2 {
		path = fs[1]
	}

	return path
}

func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	lowerCaseName := strings.ToLower(name)
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		errorPrinter("OpenFile: "+err.Error(), name)
		return nil, err
	}

	// Check if the file was newly created and update cache
	if flag&os.O_CREATE != 0 {
		UpdateFileInfo(name)
		UpdateDirectoryContents(filepath.Dir(name))
	}

	// Check if file info is already in the cache
	if _, ok := CacheGet(lowerCaseName); !ok {
		// If not in cache, get file info and update cache
		stat, err := file.Stat()
		if err != nil {
			errorPrinter("Open: "+err.Error(), name)
			file.Close()
			return nil, err
		}

		fileInfo := FileInfo{
			Exists:       true,
			Size:         stat.Size(),
			Mode:         stat.Mode(),
			LastModified: stat.ModTime(),
			IsDir:        stat.IsDir(),
			Name:         name,
		}
		CacheAdd(lowerCaseName, fileInfo)
	}

	return file, nil
}

func (cf *CachedFile) Close() error {
	// Update file info in cache before closing
	stat, err := cf.File.Stat()
	if err != nil {
		errorPrinter("Close: "+err.Error(), cf.Name())
		return err // or handle error differently
	}

	fileInfo := FileInfo{
		Exists:       true,
		Size:         stat.Size(),
		Mode:         stat.Mode(),
		LastModified: stat.ModTime(),
		IsDir:        false,
		Name:         filepath.Base(cf.path),
	}

	lowerCasePath := strings.ToLower(cf.path)
	CacheAdd(lowerCasePath, fileInfo)
	CacheDelete(strings.ToLower(filepath.Dir(cf.path)))
	// Now close the file
	return cf.File.Close()
}

func Create(name string) (*CachedFile, error) {
	name = cleanPath(name)

	file, err := os.Create(name)
	if err != nil {
		errorPrinter("Create: "+err.Error(), name)
		return nil, err
	}

	sname := strings.ToLower(name)
	d, _ := filepath.Split(sname)
	UpdateFileInfo(sname)
	CacheDelete(strings.ToLower(d))

	// Wrap the *os.File in CachedFile
	return &CachedFile{File: file, path: name}, nil
}

func Open(name string) (*os.File, error) {
	name = cleanPath(name)
	lowerCaseName := strings.ToLower(name)

	// Open the file using os.Open
	file, err := os.Open(name)
	if err != nil {
		errorPrinter("Open: "+err.Error(), name)
		return nil, err
	}

	// Check if file info is already in the cache
	if _, ok := CacheGet(lowerCaseName); !ok {
		// If not in cache, get file info and update cache
		stat, err := file.Stat()
		if err != nil {
			errorPrinter("Open: "+err.Error(), name)
			file.Close()
			return nil, err
		}

		fileInfo := FileInfo{
			Exists:       true,
			Size:         stat.Size(),
			Mode:         stat.Mode(),
			LastModified: stat.ModTime(),
			IsDir:        stat.IsDir(),
			Name:         name,
		}
		CacheAdd(lowerCaseName, fileInfo)
	}

	return file, nil
}

func Delete(name string) error {
	lowerCaseName := strings.ToLower(cleanPath(name))

	// Remove the file from the filesystem
	err := os.Remove(name) // Use original case for filesystem operations
	if err != nil {
		errorPrinter("Delete: "+err.Error(), name)
		return err
	}

	// Expire file info in the cache
	CacheDelete(lowerCaseName)
	// Expire the directory contents in the cache
	CacheDelete(filepath.Dir(lowerCaseName))
	return nil
}

func ReadFile(name string) ([]byte, error) {
	// Read the file contents
	content, err := os.ReadFile(name) // Use the original case for filesystem operations
	if err != nil {
		errorPrinter("ReadFile: "+err.Error(), name)
		return nil, err
	}

	return content, nil
}

func FileExists(name string) bool {
	lowerCaseName := strings.ToLower(cleanPath(name))
	if temp, ok := CacheGet(lowerCaseName); ok {
		fileInfo := temp
		return fileInfo.Exists
	}

	_, err := Stat(name)
	if os.IsNotExist(err) {
		return false
	} else if err == nil {
		UpdateFileInfo(lowerCaseName)
		return true
	}
	return false
}

func Mkdir(name string, perm os.FileMode) error {
	name = cleanPath(name) // Preserve original name for file operation
	err := os.Mkdir(name, perm)
	if err != nil {
		errorPrinter("Mkdir: "+err.Error(), name)
		return err
	}

	UpdateFileInfo(name) // Use the original name
	UpdateDirectoryContents(filepath.Dir(name))
	return nil
}

func MkdirAll(path string, perm os.FileMode) error {
	path = cleanPath(path) // Preserve original path for file operation

	if FileExists(path) == true {
		return nil
	}

	err := os.MkdirAll(path, perm)
	if err != nil {
		return err
	}

	UpdateDirectoryContents(path)
	UpdateDirectoryContents(filepath.Dir(path))

	return nil
}

func Append(name string, content []byte) error {
	lowerCaseName := strings.ToLower(cleanPath(name))
	var file *os.File
	var err error

	// If not, open the file and store the handle in the map
	file, err = os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the content to the file
	written, err := file.Write(content)
	if err != nil {
		errorPrinter("Append: "+err.Error(), name)
		return err
	}

	_, b := CacheGet(lowerCaseName)
	if b == false {
		UpdateFileInfo(name)
		UpdateDirectoryContents(filepath.Dir(lowerCaseName))
	}
	UpdateFileInfoWithSize(lowerCaseName, int64(written))
	return nil
}

func AppendStringToFile(name string, content string) error {
	return Append(name, []byte(content))
}

func WriteFile(name string, content []byte, perm os.FileMode) error {
	name = cleanPath(name)
	lowerCaseName := strings.ToLower(name)

	// Write the new content to the file
	err := os.WriteFile(name, content, perm)

	CacheDelete(filepath.Dir(lowerCaseName))
	CacheDelete(lowerCaseName)
	if err != nil {
		return err
	}

	return nil
}

func FileSize(name string) (int64, error) {
	lowerCaseName := strings.ToLower(cleanPath(name))

	// Check if file information is available in the cache
	if f, ok := CacheGet(lowerCaseName); ok {
		if f.Exists {
			return f.Size, nil
		}
	}

	// If not in cache, get file size from the filesystem
	stat, err := os.Stat(name) // Original name for filesystem operation
	if err != nil {
		errorPrinter("FileSize: "+err.Error(), name)
		return 0, err // File does not exist or other error occurred
	}

	// Update the cache with the new file information
	UpdateFileInfo(name) // Original name for updating FileInfo

	return stat.Size(), nil
}

func FileSizeZeroOnError(name string) int64 {
	lowerCaseName := strings.ToLower(cleanPath(name))

	// Check if file information is available in the cache
	if f, ok := CacheGet(lowerCaseName); ok {
		if f.Exists {
			return f.Size
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
	lowerOldName := strings.ToLower(cleanPath(oldName))
	lowerNewName := strings.ToLower(cleanPath(newName))
	fmt.Println(oldName, newName)

	if lowerOldName == lowerNewName {
		return nil
	}

	err := os.Rename(oldName, newName)
	if err != nil {
		errorPrinter("Rename: "+err.Error(), oldName)
		errorPrinter("Rename: "+err.Error(), newName)
		return err
	}

	CacheDelete(lowerOldName)
	UpdateDirectoryContents(filepath.Dir(lowerOldName))
	UpdateDirectoryContents(filepath.Dir(lowerNewName))

	return nil
}

func CopyFile(src, dst string) (err error) {
	src = cleanPath(src)
	dst = cleanPath(dst)

	in, err := os.Open(src)
	if err != nil {
		errorPrinter("CopyFile (os.Open): "+err.Error(), src)
		return
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		errorPrinter("CopyFile (os.Create): "+err.Error(), dst)
		return
	}
	defer func() {
		if e := out.Close(); e != nil {
			err = e
		}
	}()

	_, err = io.Copy(out, in)
	if err != nil {
		errorPrinter("CopyFile (io.Copy): "+err.Error(), "")
		return
	}

	err = out.Sync()
	if err != nil {
		errorPrinter("CopyFile (out.Sync): "+err.Error(), "")
		return
	}

	si, err := os.Stat(src)
	if err != nil {
		errorPrinter("CopyFile (os.Stat): "+err.Error(), "")
		return
	}
	err = os.Chmod(dst, si.Mode())
	if err != nil {
		errorPrinter("CopyFile (os.Chmod): "+err.Error(), "")
		return
	}

	UpdateDirectoryContents(filepath.Dir(dst))

	return
}

func Remove(name string) error {
	lowerCaseName := strings.ToLower(cleanPath(name))

	CacheDelete(lowerCaseName)

	err := os.Remove(name)
	if err != nil {
		errorPrinter("Remove: "+err.Error(), name)
		return err
	}

	UpdateDirectoryContents(filepath.Dir(lowerCaseName))

	return nil
}

func CopyDir(src string, dst string) error {
	src = cleanPath(src)
	dst = cleanPath(dst)

	_, ok := CacheGet(strings.ToLower(src))
	if ok == false {
		ListFS(strings.ToLower(src))
	}

	si, err := Stat(src) // Stat uses cache
	if err != nil {
		errorPrinter("CopyDir (Stat): "+err.Error(), src)
		return err
	}
	if !si.IsDir {
		return fmt.Errorf("source is not a directory")
	}

	if FileExists(dst) == true {
		errorPrinter("CopyDir: File already exist", dst)
		return fmt.Errorf("destination already exists")
	}

	err = MkdirAll(dst, si.Mode)
	if err != nil {
		errorPrinter("CopyDir (os.MkdirAll): "+err.Error(), dst)
		return err
	}
	UpdateFileInfo(dst)          // Update cache for the new directory
	entries, err := ReadDir(src) // ReadDir uses cache
	if err != nil {
		errorPrinter("CopyDir (ReadDir): "+err.Error(), src)
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name)
		dstPath := filepath.Join(dst, entry.Name)

		if entry.IsDir {
			err = CopyDir(srcPath, dstPath)
			if err != nil {
				errorPrinter("CopyDir (CopyDir-1): "+err.Error(), srcPath)
				errorPrinter("CopyDir (CopyDir-2): "+err.Error(), dstPath)
				return err
			}
			UpdateDirectoryContents(dstPath)
		} else {
			// Skip symlinks
			if entry.Mode&os.ModeSymlink != 0 {
				continue
			}

			err = CopyFile(srcPath, dstPath)
			if err != nil {
				errorPrinter("CopyDir (CopyFile-1): "+err.Error(), srcPath)
				errorPrinter("CopyDir (CopyFile-2): "+err.Error(), dstPath)
				return err
			}
			UpdateDirectoryContents(dstPath)
		}
	}

	return nil
}

func ReadDir(dirName string) ([]FileInfo, error) {
	lowerCaseDirName := strings.ToLower(cleanPath(dirName))

	// Check if the directory's information is already cached
	if fc, ok := CacheGet(lowerCaseDirName); ok {
		return fc.Contents, nil
	}

	// Open the directory
	f, err := os.Open(dirName)
	if err != nil {
		log.Printf("ReadDir (os.Open): %v", err)
		return nil, err
	}
	defer f.Close()

	// Read the directory entries
	dirs, err := f.ReadDir(-1)
	if err != nil {
		log.Printf("ReadDir (f.ReadDir): %v", err)
		return nil, err
	}

	// Sort the directory entries by name
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].Name() < dirs[j].Name() })

	// Convert the directory entries to FileInfo objects
	var fileInfos []FileInfo
	for _, entry := range dirs {
		entryStat, err := entry.Info()
		if err != nil {
			log.Printf("ReadDir (entry.Info): %v", err)
			return nil, err
		}

		fileInfo := FileInfo{
			Exists:       true,
			Size:         entryStat.Size(),
			Mode:         entryStat.Mode(),
			LastModified: entryStat.ModTime(),
			IsDir:        entryStat.IsDir(),
			Name:         entryStat.Name(),
		}

		fileInfos = append(fileInfos, fileInfo)
	}

	// Cache the directory's information
	dirInfo := FileInfo{
		Exists:   true,
		IsDir:    true,
		Contents: fileInfos,
		Name:     filepath.Base(dirName),
	}
	CacheAdd(lowerCaseDirName, dirInfo)

	return fileInfos, nil
}

func RemoveAll(path string) error {
	path = cleanPath(path)
	oserr := os.RemoveAll(path)

	err := updateCacheAfterRemoveAll(strings.ToLower(path))
	if err != nil {
		errorPrinter("Remove: "+err.Error(), path)
		return err
	}

	UpdateDirectoryContents(filepath.Dir(path))

	return oserr
}

func ListFS(path string) []string {
	var sysSlices []string
	lowerCasePath := strings.ToLower(cleanPath(path))

	// First, check if the path is a directory
	fileInfo, err := Stat(path)
	if err != nil {
		errorPrinter("ListFS (Stat): "+err.Error(), path)
		return sysSlices // Return empty slice if there's an error
	}
	if !fileInfo.IsDir {
		return sysSlices // Return empty slice if it's not a directory
	}

	if len(fileInfo.Contents) == 0 {
		UpdateDirectoryContents(path)
	}

	//Build the directory from disk
	objs, err := ReadDir(lowerCasePath)
	if err == nil {
		for _, fi := range objs {
			if fi.IsDir {
				sysSlices = append(sysSlices, "*"+fi.Name)
			} else {
				sysSlices = append(sysSlices, fi.Name)
			}
		}
	} else {
		errorPrinter("ListFS: 9"+err.Error(), "")
	}
	return sysSlices
}

func RecurseFS(path string) (sysSlices []string) {
	lowerCasePath := strings.ToLower(cleanPath(path))

	//	temp, ok := FileCache.Get(lowerCasePath)
	var files []FileInfo

	stat, err := Stat(lowerCasePath)
	if err != nil {
		return sysSlices
	}

	temp, err := ReadDir(lowerCasePath)
	if err != nil {
		return sysSlices
	}

	if stat.IsDir {
		for _, name := range temp {
			fileInfo, err := Stat(filepath.Join(path, name.Name)) // Use original path for stat
			if err != nil {
				errorPrinter("RecureseFS (Stat): "+err.Error(), filepath.Join(path, name.Name))
				continue // Handle error as needed
			}
			files = append(files, fileInfo)
		}
	}

	for _, f := range files {
		fullPath := path + "/" + f.Name
		if f.IsDir {
			sysSlices = append(sysSlices, "*"+fullPath)
			childSlices := RecurseFS(fullPath)
			sysSlices = append(sysSlices, childSlices...)
		} else {
			sysSlices = append(sysSlices, fullPath)
		}
	}

	return sysSlices
}

func FileAgeInSec(filename string) (age time.Duration, err error) {
	lowerCaseFilename := strings.ToLower(cleanPath(filename))

	// Check if file information is available in the cache
	fileInfo, ok := CacheGet(lowerCaseFilename)
	if ok && !fileInfo.Exists {
		// If the file is marked as non-existent and it's been more than 5 minutes, remove it from the cache
		if time.Now().Sub(fileInfo.CacheTime) > MaxCacheTime {
			CacheDelete(lowerCaseFilename)
		}
	}

	// Check if file information is available in the cache
	fileInfo, ok = CacheGet(lowerCaseFilename)
	if !ok {
		// If not in cache, get file info from the filesystem and update the cache
		var stat FileInfo
		stat, err = Stat(filename)
		if err != nil {
			errorPrinter("FileAgeInSec: "+err.Error(), filename)
			return -1, err
		}

		fileInfo = FileInfo{
			Exists:       true,
			Size:         stat.Size,
			Mode:         stat.Mode,
			LastModified: stat.LastModified,
			IsDir:        stat.IsDir,
			Name:         filename,
			CacheTime:    time.Now(),
		}
		CacheAdd(lowerCaseFilename, fileInfo)
	}

	return time.Now().Sub(fileInfo.LastModified), nil
}

func CopyDirFilesGlob(src string, dst string, fileMatch string) (err error) {
	src = cleanPath(src)
	dst = cleanPath(dst)

	// Check if source is a directory
	srcInfo, err := Stat(src) // Use cached Stat
	if err != nil {
		errorPrinter("CopyDirFilesGlob: "+err.Error(), src)
		return fmt.Errorf("source is not a directory or does not exist")
	}
	if !srcInfo.IsDir {
		return fmt.Errorf("source is not a directory or does not exist")
	}

	// Create destination directory if it doesn't exist
	if !FileExists(dst) {
		err = MkdirAll(dst, srcInfo.Mode) // Use cached MkdirAll
		if err != nil {
			errorPrinter("CopyDirFilesGlob (MkdirAll): "+err.Error(), dst)
			return
		}
	}

	// Use CachedGlob to match files
	matches, err := Glob(src + "/" + fileMatch)
	if err != nil {
		errorPrinter("CopyDirFilesGlob (Glob): "+err.Error(), src+"/"+fileMatch)
		return err
	}

	for _, item := range matches {
		itemBaseName := filepath.Base(item)
		err = CopyFile(item, filepath.Join(dst, itemBaseName)) // Use cached CopyFile
		if err != nil {
			errorPrinter("CopyDirFilesGlob (CopyFile-1): "+err.Error(), item)
			errorPrinter("CopyDirFilesGlob (CopyFile-2): "+err.Error(), filepath.Join(dst, itemBaseName))
			return
		}
		CacheDelete(strings.ToLower(filepath.Join(dst, itemBaseName)))
	}
	CacheDelete(strings.ToLower(dst))

	return nil
}

func FindFilesInDir(dir string, pattern string) ([]string, error) {
	entries, err := ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var matches []string
	for _, entry := range entries {
		if matched, err := filepath.Match(pattern, entry.Name); err != nil {
			return nil, err
		} else if matched {
			matches = append(matches, filepath.Join(dir, entry.Name))
		}
	}

	return matches, nil
}

func Glob(pattern string) ([]string, error) {
	errorZ := errors.New("")

	// First, try to match the pattern with files in the cache
	cachedMatches, errorZ := CachedGlob(pattern)
	if errorZ != nil {
		errorPrinter("Glob: "+errorZ.Error(), "")
		return nil, errorZ
	}

	// If no matches found in cache, use the standard Glob function
	if len(cachedMatches) == 0 {
		cachedMatches, errorZ = filepath.Glob(pattern)
		for _, obj := range cachedMatches {
			UpdateFileInfo(obj)
		}
	}

	return cachedMatches, errorZ
}

func CachedGlob(pattern string) ([]string, error) {
	var matches []string
	lowerCasePattern := strings.ToLower(pattern)

	// Read the directory
	files, err := ReadDir(".")
	if err != nil {
		log.Printf("CachedGlob: %v", err)
		return nil, err
	}

	// Iterate through all items in the directory
	for _, file := range files {
		fileInfo, ok := CacheGet(strings.ToLower(file.Name))
		if ok {
			matched, err := filepath.Match(lowerCasePattern, strings.ToLower(fileInfo.Name))
			if err != nil {
				log.Printf("CachedGlob: %v", err)
				return nil, err
			}
			if matched {
				matches = append(matches, fileInfo.Name)
			}
		}
	}

	return matches, nil
}

func Stat(name string) (FileInfo, error) {
	lowerCaseName := strings.ToLower(cleanPath(name))

	// Check if file information is available in the cache
	if fileInfo, ok := CacheGet(lowerCaseName); ok {
		if fileInfo.CacheTime.Sub(time.Now()).Seconds() > MaxCacheTime.Seconds() {
			CacheDelete(lowerCaseName)
		} else if fileInfo.Name == "" {
			CacheDelete(lowerCaseName)
		} else {
			return fileInfo, nil
		}
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
		Name:         dirNameOnly, // Store the original name
		CacheTime:    time.Now(),
	}

	// Update the cache with this new information
	CacheAdd(lowerCaseName, info)
	if info.IsDir {
		UpdateDirectoryContents(name)
	}

	return info, nil
}

func updateCacheAfterRemoveAll(path string) error {
	lowerCasePath := strings.ToLower(cleanPath(path))

	fso, err := ReadDir(path)
	if err != nil {
		return nil
	}
	for _, b := range fso {
		if b.IsDir {
			updateCacheAfterRemoveAll(path + "/" + b.Name)
		} else {
			CacheDelete(path + "/" + b.Name)
		}
	}
	CacheDelete(lowerCasePath)

	return nil
}

func UpdateFileInfoWithSize(name string, sizeIncrement int64) {
	lowerCaseName := strings.ToLower(cleanPath(name))
	if fileInfo, ok := CacheGet(lowerCaseName); ok {
		updatedFileInfo := fileInfo
		updatedFileInfo.Size += sizeIncrement
		updatedFileInfo.LastModified = time.Now() // Update the last modified time
		CacheAdd(lowerCaseName, updatedFileInfo)
	} else {
		// If the file is not in cache, retrieve the full info
		UpdateFileInfo(name)
	}
}

func UpdateFileInfo(name string) {
	lowerCaseName := strings.ToLower(cleanPath(name))
	var info FileInfo

	// Check if the file exists
	stat, err := os.Stat(name) // Use the original case for filesystem operations
	if err != nil {
		if os.IsNotExist(err) {
			info = FileInfo{Exists: false, CacheTime: time.Now()}
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
			Name:         stat.Name(), // Preserve the original file name
			CacheTime:    time.Now(),
		}
	}

	// Update the FileCache
	CacheAdd(lowerCaseName, info)

	if info.IsDir {
		UpdateDirectoryContents(name)
	}
}

func UpdateDirectoryContents(dirName string) {
	dirName = cleanPath(dirName)
	lowerCaseDirName := strings.ToLower(dirName)

	files, err := os.ReadDir(dirName) // Use the original case for filesystem operations
	if err != nil {
		log.Printf("UpdateDirectoryContents (os.ReadDir): %v", err)
		return // Handle error
	}

	var contents []FileInfo
	for _, file := range files {
		fileInfo, err := file.Info()
		if err != nil {
			log.Printf("UpdateDirectoryContents (file.Info): %v", err)
			continue
		}

		info := FileInfo{
			Exists:       true,
			Size:         fileInfo.Size(),
			Mode:         fileInfo.Mode(),
			LastModified: fileInfo.ModTime(),
			IsDir:        fileInfo.IsDir(),
			Name:         fileInfo.Name(), // Preserve the original file name
			CacheTime:    time.Now(),
		}

		contents = append(contents, info)
	}

	dstat, err := Stat(dirName)
	if err != nil {
		log.Printf("UpdateDirectoryContents (Stat): %v", err)
		return
	}

	dirNameOnly := filepath.Base(dirName) // Get only the directory name
	dirInfo := FileInfo{
		Exists:       true,
		IsDir:        true,
		Name:         dirNameOnly,
		Contents:     contents,
		LastModified: dstat.LastModified,
		Mode:         dstat.Mode,
		CacheTime:    time.Now(),
	}

	CacheAdd(lowerCaseDirName, dirInfo)
}
