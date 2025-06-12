package loging

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

var Log = NewStd()

type logger struct {
	appName         string
	terminal        bool
	outFile         bool
	outFileWriter   *os.File
	outService      bool // 日志输出到服务
	outServiceIp    string
	outServicePort  int
	outServiceConn  *net.UDPConn
	outServiceLevel []int
	NowLevel        Level //默认debug
}

func NewStd() *logger {
	return &logger{
		terminal:        true,
		outFile:         false,
		outService:      false,
		outServiceLevel: []int{3, 4, 5},
		NowLevel:        Level(1),
	}
}

// SetLogFile 设置日志文件名称, 文件名称可含路径(绝对或相对)
func (std *logger) SetLogFile(name string) {
	std.outFile = true
	std.appName = name

	fileName := name + ".log"
I:
	read, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		// 获取文件所在目录
		dir := filepath.Dir(fileName)
		err1 := os.MkdirAll(dir, 0777)
		if err1 != nil {
			fmt.Println("无法创建目录:", err1)
			goto I
		}
		read, err = os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			goto I
		}
	}
	std.outFileWriter = read

	go std.NewFile()

	//如果当前日志文件大小超过1M，则创建新的日志文件
	//go func() {
	//	for {
	//		time.Sleep(time.Second * 10)
	//		if std.outFileWriter != nil {
	//			fileInfo, err := std.outFileWriter.Stat()
	//			if err != nil {
	//				fmt.Println(err)
	//				return
	//			}
	//			if fileInfo.Size() > 1*1024*1024 {
	//				fileNameOld := name + "_" + time.Now().Format("20060102130405") + ".log"
	//				std.outFileWriter.Close()
	//				os.Rename(name+".log", fileNameOld)
	//				readNew, err := os.OpenFile(name+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//				if err != nil {
	//					fmt.Println(err)
	//					return
	//				}
	//				std.outFileWriter = readNew
	//			}
	//		}
	//	}
	//}()
}

func (std *logger) NewFile() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("无法创建目录:", string(debug.Stack()))
			go std.NewFile()
		}
	}()
	for {
		//凌晨时刻更新
		now := time.Now()
		midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 1, 0, now.Location())
		select {
		case <-time.After(midnight.Sub(now)):
			defer std.outFileWriter.Close()
			fileNamelod := std.appName + "_" + time.Now().Add(-24*time.Hour).Format("20060102") + ".log"
			std.outFileWriter.Close()
			_ = os.Rename(std.appName+".log", fileNamelod)
			read, err := os.OpenFile(std.appName+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				fmt.Println(err)
				continue
			}
			std.outFileWriter = read
		}
		//	删除多余日志文件
		sevenDaysAgo := now.AddDate(0, 0, -7) // 计算7天前的时间
		// 打开目录
		dirPath := filepath.Dir(std.appName + ".log")
		dir, err := os.Open(dirPath)
		if err != nil {
			fmt.Println(err)
			continue
		}
		// 读取目录中的文件
		fileInfos, err := dir.Readdir(-1)
		if err != nil {
			fmt.Println(err)
			continue
		}
		// 遍历目录中的文件
		for _, fileInfo := range fileInfos {
			if fileInfo.ModTime().Before(sevenDaysAgo) {
				err := os.Remove(dirPath + "/" + fileInfo.Name())
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println("已删除文件:", fileInfo.Name())
				}
			}
		}
		_ = dir.Close()
	}
}

// SetAppName 设置项目名称
func (std *logger) SetAppName(name string) {
	std.appName = name
}

func (std *logger) SetOutService(ip string, port int) {
	var err error
	std.outService = true
	std.outServiceIp = ip
	std.outServicePort = port
	srcAddr := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	dstAddr := &net.UDPAddr{IP: net.ParseIP(std.outServiceIp), Port: std.outServicePort}
	std.outServiceConn, err = net.DialUDP("udp", srcAddr, dstAddr)
	if err != nil {
		std.Error(err)
	}
}

func (std *logger) SetOutServiceWarn2Panic() {
	std.outServiceLevel = []int{3, 4, 5}
}

func (std *logger) SetOutServiceInfo2Panic() {
	std.outServiceLevel = []int{1, 2, 3, 4, 5}
}

func (std *logger) Close() {
	std.terminal = false
	std.outFile = false
	std.outService = false
}

func (std *logger) DisableTerminal() {
	std.terminal = false
}

type Level int

var LevelMap = map[Level]string{
	0: "[Print] ",
	1: "[DEBUG] ",
	2: "[INFO] ",
	3: "[WARN] ",
	4: "[ERROR] ",
	5: "[PANIC] ",
	6: "[Http] ",
}

func ParseLogLevel(levelstr string) (Level, error) {
	switch strings.ToLower(levelstr) {
	case "print":
		return Level(0), nil
	case "debug":
		return Level(1), nil
	case "info":
		return Level(2), nil
	case "warn":
		return Level(3), nil
	case "error":
		return Level(4), nil
	case "fatal":
		return Level(5), nil
	case "http":
		return Level(6), nil
	}
	return 0, fmt.Errorf("invalid log level '%s' (info, debug, warn, error, fatal)", levelstr)
}

// 获取ANSI颜色代码
func getAnsiColor(level Level) (colorCode string, resetCode string) {
	switch level {
	case Level(1):
		colorCode = "\033[32m"
	case Level(2):
		colorCode = "\033[34m"
	case Level(3):
		colorCode = "\033[33m"
	case Level(4):
		colorCode = "\033[31m"
	case Level(5):
		colorCode = "\033[35m"
	default:
		colorCode = "\033[39m"
	}
	resetCode = "\033[0m"
	return
}

// 去除ANSI颜色代码
func removeAnsiCodes(input []byte) []byte {
	ansiEscapePattern := `\x1b\[[0-9;]*m`
	re := regexp.MustCompile(ansiEscapePattern)
	return re.ReplaceAll(input, []byte{})
}

func (l *logger) Log(level Level, args string, times int) {
	//判断日志级别
	if level < l.NowLevel {
		return
	}
	var buffer bytes.Buffer
	buffer.WriteString(time.Now().Format("2006-01-02 15:04:05.000 "))

	// 添加颜色代码
	colorCode, resetCode := getAnsiColor(level)
	buffer.WriteString(colorCode)
	buffer.WriteString(LevelMap[level])
	buffer.WriteString(resetCode)

	_, file, line, _ := runtime.Caller(2)
	fileList := strings.Split(file, "/")
	// 最多显示两级路径
	if len(fileList) > 1 {
		fileList = fileList[len(fileList)-1 : len(fileList)]
	}

	if times != -1 {
		buffer.WriteString(strings.Join(fileList, "/"))
		buffer.WriteString(":")
		buffer.WriteString(strconv.Itoa(line))
	}

	buffer.WriteString(" | ")
	buffer.WriteString(args)
	buffer.WriteString("\n")
	out := buffer.Bytes()
	if l.terminal {
		_, _ = buffer.WriteTo(os.Stdout)
	}
	// 输出到文件或远程日志服务
	if l.outFile {
		plainOut := removeAnsiCodes(out)
		_, _ = l.outFileWriter.Write(plainOut)
	}
	if l.outService {
		for _, v := range l.outServiceLevel {
			if Level(v) == level {
				out = append([]byte("1"+l.appName+"|"), out...)
				_, _ = l.outServiceConn.Write(out)
			}
		}
	}
}

func (std *logger) Print(args ...interface{}) {
	std.Log(0, fmt.Sprint(args...), 2)
}

func (std *logger) Printf(format string, args ...interface{}) {
	std.Log(0, fmt.Sprintf(format, args...), 2)
}

func (std *logger) Info(args ...interface{}) {
	std.Log(2, fmt.Sprint(args...), 2)
}

func (std *logger) Infof(format string, args ...interface{}) {
	std.Log(2, fmt.Sprintf(format, args...), 2)
}

func (std *logger) InfoF(format string, args ...interface{}) {
	std.Log(2, fmt.Sprintf(format, args...), 2)
}

// InfoTimes
// times 意思是打印第几级函数调用
func (std *logger) InfoTimes(times int, args ...interface{}) {
	std.Log(2, fmt.Sprint(args...), times)
}

// InfoFTimes
// times 意思是打印第几级函数调用
func (std *logger) InfofTimes(times int, format string, args ...interface{}) {
	std.Log(2, fmt.Sprintf(format, args...), times)
}

func (std *logger) Debug(args ...interface{}) {
	std.Log(1, fmt.Sprint(args...), 2)
}

func (std *logger) Debugf(format string, args ...interface{}) {
	std.Log(1, fmt.Sprintf(format, args...), 2)
}

func (std *logger) DebugF(format string, args ...interface{}) {
	std.Log(1, fmt.Sprintf(format, args...), 2)
}

func (std *logger) DebugTimes(times int, args ...interface{}) {
	std.Log(1, fmt.Sprint(args...), times)
}

func (std *logger) DebugfTimes(times int, format string, args ...interface{}) {
	std.Log(1, fmt.Sprintf(format, args...), times)
}

func (std *logger) Warn(args ...interface{}) {
	std.Log(3, fmt.Sprint(args...), 2)
}

func (std *logger) Warnf(format string, args ...interface{}) {
	std.Log(3, fmt.Sprintf(format, args...), 2)
}

func (std *logger) WarnTimes(times int, args ...interface{}) {
	std.Log(3, fmt.Sprint(args...), times)
}

func (std *logger) WarnFTimes(times int, format string, args ...interface{}) {
	std.Log(3, fmt.Sprintf(format, args...), times)
}

func (std *logger) Error(args ...interface{}) {
	std.Log(4, fmt.Sprint(args...), 2)
}

func (std *logger) Errorf(format string, args ...interface{}) {
	std.Log(4, fmt.Sprintf(format, args...), 2)
}

func (std *logger) ErrorF(format string, args ...interface{}) {
	std.Log(4, fmt.Sprintf(format, args...), 2)
}

func (std *logger) ErrorTimes(times int, args ...interface{}) {
	std.Log(4, fmt.Sprint(args...), times)
}

func (std *logger) ErrorfTimes(times int, format string, args ...interface{}) {
	std.Log(4, fmt.Sprintf(format, args...), times)
}

func (std *logger) Panic(args ...interface{}) {
	std.Log(5, fmt.Sprint(args...), 2)
	panic(args)
}

func (std *logger) HttpInfo(args ...interface{}) {
	std.Log(6, fmt.Sprint(args...), -1)
}

type Logger interface {
	Error(entries ...interface{})
	Errorf(format string, entries ...interface{})

	Info(entries ...interface{})
	Infof(format string, entries ...interface{})

	Warn(entries ...interface{})
	Warnf(format string, entries ...interface{})

	Debug(entries ...interface{})
	Debugf(format string, entries ...interface{})

	//Fatal(v ...interface{})
	//Fatalf(format string, v ...interface{})

	//Panic(v ...interface{})
	//Panicf(format string, v ...interface{})
	//
	//Trace(entries ...interface{})
	//Tracef(format string, entries ...interface{})
	//
	//Panic(entries ...interface{})
	//Panicln(entries ...interface{})
	//Panicf(format string, entries ...interface{})
	//
	//Fatalln(entries ...interface{})
	//Fatal(entries ...interface{})
	//Fatalf(format string, entries ...interface{})
	//
	//Println(entries ...interface{})
	//Printf(format string, entries ...interface{})
}

var Default Logger

func init() {
	if Default == nil {
		Default = Log
	}
}

func Debug(args ...interface{}) {
	Default.Debug(args...)
}

func Debugf(format string, entries ...interface{}) {
	Default.Debugf(format, entries...)
}

func Info(args ...interface{}) {
	Default.Info(args...)
}

func Infof(format string, entries ...interface{}) {
	Default.Infof(format, entries...)
}

func Error(args ...interface{}) {
	Default.Error(args...)
}

func Errorf(format string, entries ...interface{}) {
	Default.Errorf(format, entries...)
}

func Warn(args ...interface{}) {
	Default.Warn(args...)
}

func Warnf(format string, entries ...interface{}) {
	Default.Warnf(format, entries...)
}
