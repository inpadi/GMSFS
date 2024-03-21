package GMSFS

import (
	"errors"
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// FileInfo stores comprehensive metadata about a file or directory
type FileInfo struct {
	Exists       bool
	Size         int64
	Mode         os.FileMode
	LastModified time.Time
	IsDir        bool
	Contents     []string // Names of files for directories
	Name         string
	CacheTime    time.Time
}

type FileInfoRD struct {
	FileInfoRD []FileInfo
	CacheTime  time.Time
}

type CachedFile struct {
	*os.File
	path string
}

const timeFlat = "20060102_1504"

// Global variables for caches
var FileCache = cmap.New[FileInfo]()
var FileCacheRD = cmap.New[FileInfoRD]()

// Global variables for file handles and timers
var FileHandles = cmap.New[*os.File]()
var FileTimers = cmap.New[*time.Timer]()

var MaxCacheDepth = 3         //Max cache level
var CacheRoot = "/autoupdate" //Only cache files and folders below this folder

var _ = runloopCache()

// FileHandleInstance to store file and timer information
type FileHandleInstance struct {
	File  *os.File
	Timer *time.Timer
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
	name = filepath.Clean(name)
	fmt.Println("Invistiage object: " + name)
	_, ok := FileCache.Get(strings.ToLower(filepath.Clean(name)))
	if ok == true {
		_, err := os.Stat(filepath.Clean(name))
		if err != nil {
			//We know the filesystem seems to have a issue with this object, so we clean it form the cache
			FileCache.Remove(filepath.Clean(name))
			UpdateDirectoryContents(filepath.Dir(strings.ToLower(filepath.Clean(name))))
		}
	}
}

func runloopCache() string {
	go loopCache()
	return "running"
}

func loopCache() {
	lc := 0
	for {
		looptimer := time.Now()
		fc := FileCache.Items()
		for a, b := range fc {
			a := filepath.Clean(a)
			//Clean up in negative cache
			if b.Exists == false {
				t := time.Now()
				if t.Sub(b.CacheTime).Seconds() > 300 {
					FileCache.Remove(a)
				}
			}
			//Fix directory connections
			if b.IsDir == true {
				if inCacheScope(a) == true {
					fl := b.Contents
					//In cache check - compare objects to lists
					errCount := 0
					for _, obj := range fl {
						if FileCache.Has(filepath.Join(a, strings.ToLower(obj))) == false {
							errorPrinter("Cache inconsistency found: "+filepath.Join(a, strings.ToLower(obj))+" resetting cache for: "+a, "")
							errCount += 1
						}
					}
					if errCount > 0 {
						fmt.Println("Found errors: " + strconv.Itoa(errCount) + " on: " + a)
						ListFS(a)
					}

					//Ext cache check - compare cache to filesystem
					if lc == 20 {
						d, err := os.ReadDir(a)
						if err != nil {
							errorPrinter("Local dir error: "+err.Error(), "")
						} else {
							//Compare local file system to cache
							for _, r := range d {
								if slices.Contains(fl, r.Name()) == false {
									errorPrinter("Error - object not found in cache: "+r.Name(), "")
									Stat(filepath.Join(a, r.Name()))
									UpdateDirectoryContents(a)
								}
							}

							var tmpS []string
							for _, i := range d {
								tmpS = append(tmpS, i.Name())
							}

							//Compare cache to file system
							for _, r := range fl {
								if slices.Contains(tmpS, r) == false {
									errorPrinter("Error - object not found in filesystem: "+r, "")
									Stat(filepath.Join(a, r))
									UpdateDirectoryContents(a)
								}
							}
						}
					}
				}
			}
		}

		timeX := time.Now()
		fmt.Println("loopCache took: ", timeX.Sub(looptimer))

		if lc == 20 {
			lc = 0
		} else {
			lc += 1
		}

		time.Sleep(10 * time.Second)
	}
}

func inCacheScope(a string) bool {
	depth := filepath.SplitList(a)
	if len(a) > len(CacheRoot) {
		if len(depth) <= MaxCacheDepth+1 && strings.ToLower(a[:len(CacheRoot)]) == strings.ToLower(CacheRoot) {
			return true
		}
	}
	return false
}

func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	CloseFile(lowerCaseName)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		errorPrinter("OpenFile: "+err.Error(), name)
		return nil, err
	}

	FileHandles.Set(lowerCaseName, file)
	resetTimer(lowerCaseName)

	// Check if the file was newly created and update cache
	if flag&os.O_CREATE != 0 {
		UpdateFileInfo(name)
		UpdateDirectoryContents(filepath.Dir(name))
	}

	return file, nil
}

func CloseFile(name string) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))

	// Iterate over all file handles and close those in the specified directory
	for _, key := range FileHandles.Keys() {
		if strings.HasPrefix(key, lowerCaseName) {
			// Close individual file handle
			if file, ok := FileHandles.Get(key); ok {
				stat, err := file.Stat()
				if err == nil {
					UpdateFileInfoWithSize(key, stat.Size())
				}
				file.Close()
				FileHandles.Remove(key)
			}

			if timer, ok := FileTimers.Get(key); ok {
				timer.Stop()
				FileTimers.Remove(key)
			}
		}
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
		CacheTime:    time.Now(),
	}

	lowerCasePath := strings.ToLower(cf.path)
	if inCacheScope(lowerCasePath) == true {
		FileCache.Set(lowerCasePath, fileInfo)
	}
	UpdateDirectoryContents(filepath.Dir(cf.path))
	// Now close the file
	return cf.File.Close()
}

func Create(name string) (*CachedFile, error) {
	name = filepath.Clean(name)

	file, err := os.Create(name)
	if err != nil {
		errorPrinter("Create: "+err.Error(), name)
		return nil, err
	}

	sname := strings.ToLower(name)
	d, _ := filepath.Split(sname)
	UpdateFileInfo(sname)
	UpdateDirectoryContents(d)

	// Wrap the *os.File in CachedFile
	return &CachedFile{File: file, path: name}, nil
}

func Open(name string) (*os.File, error) {
	name = filepath.Clean(name)
	lowerCaseName := strings.ToLower(name)

	// Open the file using os.Open
	file, err := os.Open(name)
	if err != nil {
		errorPrinter("Open: "+err.Error(), name)
		return nil, err
	}

	// Check if file info is already in the cache
	if _, ok := FileCache.Get(lowerCaseName); !ok {
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
			CacheTime:    time.Now(),
		}
		if inCacheScope(lowerCaseName) {
			FileCache.Set(lowerCaseName, fileInfo)
		}
	}

	UpdateDirectoryContents(filepath.Dir(name))

	return file, nil
}

func Delete(name string) error {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	// Close the file handle if it exists
	CloseFile(lowerCaseName)

	// Remove the file from the filesystem
	err := os.Remove(name) // Use original case for filesystem operations
	if err != nil {
		errorPrinter("Delete: "+err.Error(), name)
		return err
	}

	// Update file info in the cache
	FileCache.Remove(lowerCaseName)
	// Optionally, update the directory contents in the cache
	UpdateDirectoryContents(filepath.Dir(lowerCaseName))
	return nil
}

func ReadFile(name string) ([]byte, error) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	// Close any open file handle before reading
	CloseFile(lowerCaseName)

	// Read the file contents
	content, err := os.ReadFile(name) // Use the original case for filesystem operations
	if err != nil {
		errorPrinter("ReadFile: "+err.Error(), name)
		return nil, err
	}

	return content, nil
}

func FileExists(name string) bool {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	if temp, ok := FileCache.Get(lowerCaseName); ok {
		fileInfo := temp
		return fileInfo.Exists
	}

	_, err := Stat(name)
	if os.IsNotExist(err) {
		if inCacheScope(lowerCaseName) {
			FileCache.Set(lowerCaseName, FileInfo{Exists: false, CacheTime: time.Now()})
		}
		return false
	} else if err == nil {
		UpdateFileInfo(lowerCaseName)
		return true
	}
	return false
}

func Mkdir(name string, perm os.FileMode) error {
	name = filepath.Clean(name) // Preserve original name for file operation
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
	path = filepath.Clean(path) // Preserve original path for file operation

	if FileExists(path) == true {
		return nil
	}

	err := os.MkdirAll(path, perm)
	if err != nil {
		return err
	}

	UpdateDirectoryContents(path)
	UpdateDirectoryContents(filepath.Dir(path))
	/*
		ps := filepath.SplitList(path)
		newPath := ""
		for _, b := range ps {
			if b != "" {
				if FileExists(filepath.Join(newPath, b)) == false {
					errO := Mkdir(filepath.Join(newPath, b), perm)
					if errO != nil {
						errorPrinter("MkdirAll: "+errO.Error(), newPath+"/"+b)
						return errO
					}
					UpdateFileInfo(filepath.Join(newPath, b))
					updateCacheWithNewFile(newPath, b)
				}
				newPath = newPath + "/" + b
			}
		}
	*/
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
	}

	// Write the content to the file
	written, err := file.Write(content)
	if err != nil {
		errorPrinter("Append: "+err.Error(), name)
		return err
	}

	resetTimer(lowerCaseName)
	if FileCache.Has(lowerCaseName) == false {
		UpdateFileInfo(name)
	}
	UpdateFileInfoWithSize(lowerCaseName, int64(written))
	return nil
}

func AppendStringToFile(name string, content string) error {
	return Append(name, []byte(content))
}

func WriteFile(name string, content []byte, perm os.FileMode) error {
	name = filepath.Clean(name)
	lowerCaseName := strings.ToLower(name)

	// Close any open file handle before writing
	CloseFile(lowerCaseName)

	// Write the new content to the file
	err := os.WriteFile(name, content, perm)

	// Update the cache with the new file information
	UpdateFileInfo(name) // Use the original name for updating FileInfo
	UpdateDirectoryContents(filepath.Dir(lowerCaseName))

	if err != nil {
		return err
	}

	return nil
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
		errorPrinter("FileSize: "+err.Error(), name)
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
	fmt.Println(oldName, newName)

	if lowerOldName == lowerNewName {
		return nil
	}

	if FileCache.Has(lowerOldName) == true {
		ListFS(lowerOldName)
	}

	CloseFile(oldName)
	CloseFile(newName)

	FileCache.Remove(lowerOldName)

	err := os.Rename(oldName, newName)
	if err != nil {
		errorPrinter("Rename: "+err.Error(), oldName)
		errorPrinter("Rename: "+err.Error(), newName)
		return err
	}

	FileCache.Remove(lowerOldName)
	UpdateDirectoryContents(filepath.Dir(lowerOldName))
	UpdateDirectoryContents(filepath.Dir(lowerNewName))

	return nil
}

func CopyFile(src, dst string) (err error) {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	CloseFile(src)
	CloseFile(dst)

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
	lowerCaseName := strings.ToLower(filepath.Clean(name))

	CloseFile(lowerCaseName)

	FileCache.Remove(lowerCaseName)
	UpdateDirectoryContents(filepath.Dir(lowerCaseName))

	err := os.Remove(name)
	if err != nil {
		errorPrinter("Remove: "+err.Error(), name)
		return err
	}

	return nil
}

func CopyDir(src string, dst string) error {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	if FileCache.Has(strings.ToLower(src)) == false {
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
	lowerCaseDirName := strings.ToLower(filepath.Clean(dirName))

	rd, ok := FileCacheRD.Get(lowerCaseDirName)
	if ok == true && inCacheScope(dirName) == true {
		fc, ok := FileCache.Get(lowerCaseDirName)
		if ok == true {
			if fc.CacheTime.Sub(rd.CacheTime).Milliseconds() > 0 {
				FileCacheRD.Remove(lowerCaseDirName)
			} else {
				return rd.FileInfoRD, nil
			}
		} else {
			return rd.FileInfoRD, nil
		}
	}
	f, err := os.Open(dirName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dirs, err := f.ReadDir(-1)
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].Name() < dirs[j].Name() })
	var contents []string
	rds := FileInfoRD{}
	for _, entry := range dirs {
		entryStat, err := entry.Info()
		if err != nil {
			errorPrinter("ReadDir (loop): "+err.Error(), entry.Name())
			return nil, err
		}
		// Convert os.FileInfo to your FileInfo struct
		fileInfo := FileInfo{
			Exists:       true,
			Size:         entryStat.Size(),
			Mode:         entryStat.Mode(),
			LastModified: entryStat.ModTime(),
			IsDir:        entryStat.IsDir(),
			Name:         entryStat.Name(),
		}

		//		if entryStat.IsDir() {
		//			UpdateDirectoryContents(filepath.Join(dirName, entryStat.Name()))
		//		}
		rds.FileInfoRD = append(rds.FileInfoRD, fileInfo)
		contents = append(contents, entryStat.Name())
	}
	rds.CacheTime = time.Now()
	FileCacheRD.Set(lowerCaseDirName, rds)

	if inCacheScope(dirName) {
		dirNameOnly := filepath.Base(dirName)
		FileCache.Set(lowerCaseDirName, FileInfo{
			Exists:    true,
			IsDir:     true,
			Contents:  contents,
			Name:      dirNameOnly,
			CacheTime: time.Now(),
		})
	}
	return rds.FileInfoRD, nil
}

func RemoveAll(path string) error {
	path = filepath.Clean(path)
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
	lowerCasePath := strings.ToLower(filepath.Clean(path))

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
	lowerCasePath := strings.ToLower(filepath.Clean(path))

	temp, ok := FileCache.Get(lowerCasePath)
	var files []FileInfo

	if ok && temp.IsDir {
		for _, name := range temp.Contents {
			fileInfo, err := Stat(filepath.Join(path, name)) // Use original path for stat
			if err != nil {
				errorPrinter("RecureseFS (Stat): "+err.Error(), filepath.Join(path, name))
				continue // Handle error as needed
			}
			files = append(files, fileInfo)
		}
	} else {
		var err error
		files, err = ReadDir(path) // Read from filesystem if not in cache
		if err != nil {
			errorPrinter("RecurseFS (ReadDir): "+err.Error(), path)
			return // Handle error as needed
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
	lowerCaseFilename := strings.ToLower(filepath.Clean(filename))

	// Check if file information is available in the cache
	fileInfo, ok := FileCache.Get(lowerCaseFilename)
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
		if inCacheScope(lowerCaseFilename) {
			FileCache.Set(lowerCaseFilename, fileInfo)
		}
	}

	return time.Now().Sub(fileInfo.LastModified), nil
}

func CopyDirFilesGlob(src string, dst string, fileMatch string) (err error) {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

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
	}

	return nil
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

	// Iterate through all items in the cache
	for _, key := range FileCache.Keys() {
		if fileInfo, ok := FileCache.Get(key); ok {
			matched, err := filepath.Match(lowerCasePattern, strings.ToLower(fileInfo.Name))
			if err != nil {
				errorPrinter("CachedGlob: "+err.Error(), "")
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
	lowerCaseName := strings.ToLower(filepath.Clean(name))

	if inCacheScope(lowerCaseName) {
		// Check if file information is available in the cache
		if fileInfo, ok := FileCache.Get(lowerCaseName); ok {
			if fileInfo.Name == "" {
				FileCache.Remove(lowerCaseName)
			} else {
				return fileInfo, nil
			}
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

	if inCacheScope(lowerCaseName) {
		// Update the cache with this new information
		FileCache.Set(lowerCaseName, info)
		if info.IsDir {
			UpdateDirectoryContents(name)
		}
	}

	return info, nil
}

func Update(name string, info FileInfo) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))

	if info.Exists {
		// Preserve the original name in FileInfo
		namex := filepath.Base(name)
		info.Name = namex
		if inCacheScope(lowerCaseName) {
			FileCache.Set(lowerCaseName, info)
		}
	} else {
		FileCache.Remove(lowerCaseName)
	}
}

func updateCacheAfterRemoveAll(path string) error {
	lowerCasePath := strings.ToLower(filepath.Clean(path))

	fci := FileCache.Items()
	for a, _ := range fci {
		if len(a) > len(lowerCasePath) {
			if strings.ToLower(filepath.Clean(a))[:len(path)] == lowerCasePath {
				FileCache.Remove(a)
				fmt.Println("Removed: " + a + " from cache as it was deleted...")
			}
		}
	}

	fciRD := FileCacheRD.Items()
	for a, _ := range fciRD {
		if len(a) > len(lowerCasePath) {
			if strings.ToLower(filepath.Clean(a))[:len(path)] == lowerCasePath {
				FileCache.Remove(a)
				fmt.Println("Removed: " + a + " from cache (RD) as it was deleted...")
			}
		}
	}

	return nil
}

func UpdateCacheForRenamedDirectory(oldDir, newDir string) {
	oldDir = strings.ToLower(filepath.Clean(oldDir))
	newDir = strings.ToLower(filepath.Clean(newDir))

	UpdateDirectoryContents(oldDir)
	UpdateDirectoryContents(newDir)
	/*
		// Get the original cache entry for the old directory
		if dirInfo, ok := FileCache.Get(oldDir); ok && dirInfo.IsDir {
			for _, fileName := range dirInfo.Contents {
				oldPath := filepath.Join(oldDir, fileName)
				newPath := filepath.Join(newDir, fileName)

				// Recursively handle subdirectories
				if temp, ok := FileCache.Get(oldPath); ok && temp.IsDir {
					UpdateCacheForRenamedDirectory(oldPath, newPath)
				}

				// Update the cache entry for each file/subdirectory
				if fileInfo, ok := FileCache.Get(oldPath); ok {
					fileInfo.Name = filepath.Base(newPath) // Update OriginalName
					if inCacheScope(newPath) {
						FileCache.Set(newPath, fileInfo)
					}
					FileCache.Remove(oldPath)
				}
			}

			// Finally, update the cache entry for the directory itself
			dirInfo.Name = filepath.Base(newDir)
			if inCacheScope(newDir) {
				FileCache.Set(newDir, dirInfo)
			}
			FileCache.Remove(oldDir)
		}
		dir, file := filepath.Split(oldDir)
		removeObjectFromParentCache(dir, file)

	*/
}

func UpdateFileInfoWithSize(name string, sizeIncrement int64) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	if fileInfo, ok := FileCache.Get(lowerCaseName); ok {
		updatedFileInfo := fileInfo
		updatedFileInfo.Size += sizeIncrement
		updatedFileInfo.LastModified = time.Now() // Update the last modified time
		if inCacheScope(lowerCaseName) {
			FileCache.Set(lowerCaseName, updatedFileInfo)
		}
	} else {
		// If the file is not in cache, retrieve the full info
		UpdateFileInfo(name)
	}
}

func UpdateFileInfo(name string) {
	lowerCaseName := strings.ToLower(filepath.Clean(name))
	var info FileInfo

	if inCacheScope(lowerCaseName) == false {
		return
	}

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
		if stat.IsDir() {
			info.Contents = ListFS(name)
		}

	}

	// Update the FileCache
	FileCache.Set(lowerCaseName, info)

	if info.IsDir {
		UpdateDirectoryContents(name)
	}
}

func UpdateDirectoryContents(dirName string) {
	dirName = filepath.Clean(dirName)
	lowerCaseDirName := strings.ToLower(dirName)

	FileCacheRD.Remove(lowerCaseDirName)

	if inCacheScope(lowerCaseDirName) == false {
		return
	}

	files, err := os.ReadDir(dirName) // Use the original case for filesystem operations
	if err != nil {
		errorPrinter("UpdateDirectoryContents (os.ReadDir): "+err.Error(), dirName)
		return // Handle error
	}

	var contents []string
	for _, file := range files {
		contents = append(contents, file.Name())
	}

	dstat, err := Stat(dirName)
	if err != nil {
		errorPrinter("UpdateDirectoryContents (Stat): "+err.Error(), dirName)
		return
	}

	dirNameOnly := filepath.Base(dirName) // Get only the directory name
	dirInfo := FileInfo{Exists: true, IsDir: true, Name: dirNameOnly, Contents: contents, LastModified: dstat.LastModified, Mode: dstat.Mode}

	FileCache.Set(lowerCaseDirName, dirInfo)
}
