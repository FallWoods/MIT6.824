package mr

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

func DelFileByMapId(targetNumber int, path string) error {
	// 创建正则表达式
	pattern := fmt.Sprintf(`^mr-out-%d-\d+$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	// 读取当前目录中的文件
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// 遍历文件，查找匹配的文件
	for _, file := range files {
		if file.IsDir() {
			// 如果是目录，则跳过
			continue
		}
		fileName := file.Name()
		if regex.MatchString(fileName) {
			// 匹配到了文件，则删除它
			filePath := filepath.Join(path, fileName)
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// 生成最终的文件后，删除对应中间输出文件
func DelFileByReduceId(targetNumber int, path string) error {
	// 创建正则表达式，X 是可变的指定数字

	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)

	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	// 读取当前目录中的文件
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// 遍历文件，查找匹配的文件
	for _, file := range files {
		if file.IsDir() {
			// 如果是目录，则跳过
			continue
		}
		fileName := file.Name()
		if regex.MatchString(fileName) {
			// 匹配到了文件，则删除它
			filePath := filepath.Join(path, fileName)
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ReadSpecificFile(targetNumber int, path string) (fileList []*os.File, err error) {
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// 读取当前目录中的文件
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// 遍历文件，查找匹配的文件
	for _, fileEntry := range files {
		if fileEntry.IsDir() {
			continue
		}
		fileName := fileEntry.Name()
		if regex.MatchString(fileName) {
			filePath := filepath.Join(path, fileName)
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("cannot open %v", filePath)
				// 关闭之前已经打开的文件
				for _, oFile := range fileList {
					oFile.Close()
				}
				return nil, err
			}
			fileList = append(fileList, file)
		}
	}
	return fileList, nil
}