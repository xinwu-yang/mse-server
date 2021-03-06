package main

import (
	"net/http"
	"log"
	"os/exec"
	"bytes"
	"encoding/binary"
	"sync"
	"syscall"
	"golang.org/x/net/websocket"
	"time"
	"runtime"
	_ "net/http/pprof"
	"strings"
	"encoding/json"
)

var data = RWSyncStreamsMap{make(map[string]Stream), new(sync.RWMutex)}
var live = RWSyncLiveMap{make(map[string]*map[string]int), new(sync.RWMutex)}

const (
	StreamTimeOut = 15
	SliceCap      = 512
	BufferSize    = 32768
)

type RWSyncStreamsMap struct {
	Streams map[string]Stream
	Lock    *sync.RWMutex
}

func (m RWSyncStreamsMap) Get(key string) (val Stream, ok bool) {
	m.Lock.RLock()
	defer m.Lock.RUnlock()
	val, ok = m.Streams[key]
	return val, ok
}

func (m RWSyncStreamsMap) Set(key string, val Stream) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	m.Streams[key] = val
}

func (m RWSyncStreamsMap) Delete(key string) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	delete(m.Streams, key)
}

type RWSyncLiveMap struct {
	Stream map[string]*map[string]int
	Lock   *sync.RWMutex
}

func (m RWSyncLiveMap) Get(key string) (val *map[string]int, ok bool) {
	m.Lock.RLock()
	defer m.Lock.RUnlock()
	val, ok = m.Stream[key]
	return val, ok
}

func (m RWSyncLiveMap) Set(key string, val *map[string]int) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	m.Stream[key] = val
}

func (m RWSyncLiveMap) Delete(key string) {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	delete(m.Stream, key)
}

type Stream struct {
	id             string
	total          *int
	video          *[](*[]byte)
	boxLengthSlice *[]int
	types          *[]string
	LockChan       *chan string
}

func bytesToInt(bs []byte) int {
	var val int32
	binary.Read(bytes.NewBuffer(bs), binary.BigEndian, &val)
	return int(val)
}

func bytesToInt64(bs []byte) int64 {
	var val int64
	binary.Read(bytes.NewBuffer(bs), binary.BigEndian, &val)
	return val
}

func getLastLength(boxLengthSlice []int) int {
	var lastLength int
	for j := 0; j < len(boxLengthSlice); j++ {
		lastLength += boxLengthSlice[j]
	}
	return lastLength
}

func getLastBoxSizeTotal(boxLengthSlice []int, index int) int {
	var lastLength int
	if len(boxLengthSlice) >= index+1 {
		for j := 0; j < index; j++ {
			lastLength += boxLengthSlice[j]
		}
	}
	return lastLength
}

func readMore(boxLengthSlice *[]int, types *[]string, video []*([]byte), read int, lockChan *chan string) bool {
	lastLength := getLastLength(*boxLengthSlice)
	if read-lastLength > 8 {
		var size int
		var tmp []byte
		for _, value := range video {
			readData := *value
			length := len(readData)
			size += length
			if size > lastLength {
				tmp = append(tmp, readData[length-(size-lastLength):]...)
				if len(tmp) > 7 {
					*boxLengthSlice = append(*boxLengthSlice, bytesToInt(tmp[:4]))
					boxType := string(tmp[4:8])
					*lockChan <- boxType
					*types = append(*types, boxType)
					break
				}
			}
		}
		return true
	}
	return false
}

func getMoof(boxType string, playIndex *int, isPlay *bool, i, total int, boxLengthSlice []int, ws *websocket.Conn, video [](*[]byte), videoDecodeTimeOffset, audioDecodeTimeOffset *int64) (bool, error) {
	if boxType == "moof" && *playIndex != i && i+2 < len(boxLengthSlice) {
		lastBoxSizeTotal := getLastBoxSizeTotal(boxLengthSlice, i)
		currentBoxSize := boxLengthSlice[i]
		mdatSize := boxLengthSlice[i+1]
		if lastBoxSizeTotal+currentBoxSize+mdatSize < total {
			*playIndex = i
			var size int
			var tmp = make([]byte, 0, BufferSize)
			for _, value := range video {
				tmp = append(tmp, *value...)
				size += len(*value)
				if size >= lastBoxSizeTotal+currentBoxSize+mdatSize {
					break
				}
			}
			moof := tmp[lastBoxSizeTotal:lastBoxSizeTotal+currentBoxSize]
			mdat := tmp[lastBoxSizeTotal+currentBoxSize:lastBoxSizeTotal+currentBoxSize+mdatSize]
			lastChildBoxLength := 8
			trafs := 0
			for {
				childBoxSize := bytesToInt(moof[lastChildBoxLength:lastChildBoxLength+4])
				childBoxType := string(moof[lastChildBoxLength+4:lastChildBoxLength+8])
				if childBoxType == "traf" {
					lastTrafBoxLength := 8
					for {
						trafChildBoxSize := bytesToInt(moof[lastTrafBoxLength+lastChildBoxLength:lastTrafBoxLength+lastChildBoxLength+4])
						trafChildBoxType := string(moof[lastTrafBoxLength+lastChildBoxLength+4:lastTrafBoxLength+lastChildBoxLength+8])
						if trafChildBoxType == "tfdt" {
							decodeTime := bytesToInt64(moof[lastTrafBoxLength+lastChildBoxLength+12:lastTrafBoxLength+lastChildBoxLength+20])
							if *videoDecodeTimeOffset == 0 || *audioDecodeTimeOffset == 0 {
								if trafs == 0 {
									*videoDecodeTimeOffset = decodeTime
								} else {
									*audioDecodeTimeOffset = decodeTime
								}
								//??????0
								for i := 0; i < 8; i++ {
									moof[lastTrafBoxLength+lastChildBoxLength+12+i] = 0
								}
							} else {
								//??????reduce
								bytesBuffer := bytes.NewBuffer([]byte{})
								var reduce int64
								if trafs == 0 {
									reduce = decodeTime - *videoDecodeTimeOffset
								} else {
									reduce = decodeTime - *audioDecodeTimeOffset
								}
								binary.Write(bytesBuffer, binary.BigEndian, reduce)
								result := bytesBuffer.Bytes()
								resultSize := len(result)
								fillingCount := 8 - resultSize
								j := 0
								for i := 0; i < 8; i++ {
									if fillingCount > 0 {
										moof[lastTrafBoxLength+lastChildBoxLength+12+i] = 0
										fillingCount--
									} else {
										if resultSize-j == 0 {
											break
										}
										moof[lastTrafBoxLength+lastChildBoxLength+12+i] = result[j]
										j++
									}
								}
							}
							break
						}
						lastTrafBoxLength += trafChildBoxSize
					}
					trafs++
				} else if trafs == 1 {
					break
				}
				if trafs == 2 {
					break
				}
				lastChildBoxLength += childBoxSize
			}
			//returnData := []*[]byte{&moof, &mdat}
			err := websocket.Message.Send(ws, append(moof, mdat...))
			if err != nil {
				return false, err
			}
			*isPlay = true
			return false, nil
		}
	}
	return true, nil
}

func startFFMpeg(id, cmd string, okChan *chan bool) {
	splitCmd := strings.Split(cmd, " ")
	//??????ffmpeg
	ffmpegCmd := exec.Command(splitCmd[0], splitCmd[1:]...)
	ffmpegCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	ffmpegCmd.StdinPipe()
	ffmpegOut, _ := ffmpegCmd.StdoutPipe()
	ffmpegCmd.Start()

	//???????????????
	read := 0
	timeout := 0
	boxLengthSlice := make([]int, 0, SliceCap)
	video := make([](*[]byte), 0, SliceCap)
	types := make([]string, 0, SliceCap)
	lockChan := make(chan string, 20)
	data.Set(id, Stream{id, &read, &video, &boxLengthSlice, &types, &lockChan})

	//??????okChan???????????????????????????
	<-*okChan
	for {
		//????????????ffmpeg?????????buffer???????????????32K
		readData := make([]byte, BufferSize)
		i, _ := ffmpegOut.Read(readData)
		if i > 0 {
			//????????????
			timeout = 0
			tmp := readData[:i]
			video = append(video, &tmp)
			if read == 0 && i > 7 {
				read += i
				boxLengthSlice = append(boxLengthSlice, bytesToInt(readData[:4]))
				boxType := string(readData[4:8])
				types = append(types, boxType)
				lockChan <- boxType
				//??????????????????
				for reCycle := true; reCycle; {
					reCycle = readMore(&boxLengthSlice, &types, video, read, &lockChan)
				}
			} else {
				read += i
				//???????????????box?????????
				lastLength := getLastLength(boxLengthSlice)
				if lastLength > 0 && read > lastLength {
					reduce := read - lastLength
					if reduce > 7 {
						var size int
						var tmp []byte
						for _, value := range video {
							readData := *value
							length := len(readData)
							size += length
							if size > lastLength {
								tmp = append(tmp, readData[length-(size-lastLength):]...)
								if len(tmp) > 7 {
									boxLengthSlice = append(boxLengthSlice, bytesToInt(tmp[:4]))
									boxType := string(tmp[4:8])
									lockChan <- boxType
									types = append(types, boxType)
									break
								}
							}
						}
						//??????????????????
						for reCycle := true; reCycle; {
							reCycle = readMore(&boxLengthSlice, &types, video, read, &lockChan)
						}
					}
				}
			}
		} else {
			time.Sleep(time.Second)
			timeout++
			if timeout >= StreamTimeOut {
				data.Delete(id)
				live.Delete(id)
				ffmpegCmd.Process.Wait()
				runtime.Goexit()
			}
		}
		writeChanCount++
	}
}

var readChanCount = 0
var writeChanCount = 0

func countReadAndWrite() {
	for {
		log.Println("readChanCount", readChanCount)
		log.Println("writeChanCount", writeChanCount)
		time.Sleep(time.Second * 10)
	}
}

func handleWebSocketConn(ws *websocket.Conn) {
	defer ws.Close()
	var receiveData []byte
	var receiveJson map[string]string
	var id string
	var cmd string
	var playIndex int
	var videoDecodeTimeOffset int64 = 0
	var audioDecodeTimeOffset int64 = 0
	okChan := make(chan bool, 1)
	isPlay := false
	websocket.Message.Send(ws, "conn successful")
	err := websocket.Message.Receive(ws, &receiveData)
	if err != nil {
		return
	}
	json.Unmarshal(receiveData, &receiveJson)
	id = receiveJson["id"]
	cmd = receiveJson["cmd"]
connFor:
	for {
		stream, ok := data.Get(id)
		_, isLive := live.Get(id)
		if !ok && !isLive {
			okChan <- ok
			stream := make(map[string]int)
			live.Set(id, &stream)
			go startFFMpeg(id, cmd, &okChan)
		} else if ok {
			//????????????????????????
			<-*stream.LockChan
			boxTypeSize := len(*stream.LockChan)
			for i := 0; i < boxTypeSize; i++ {
				<-*stream.LockChan
			}
			//????????????????????????
			video := *(stream.video)
			boxLengthSlice := *(stream.boxLengthSlice)
			types := *(stream.types)
			typesLength := len(types)
			total := *(stream.total)
			if playIndex == 0 {
				for i := 0; i < typesLength; i++ {
					if types[i] == "moov" {
						size := 0
						playIndex = i
						currentBoxSize := boxLengthSlice[i]
						tmp := make([]byte, 0, BufferSize)
						lastBoxSizeTotal := getLastBoxSizeTotal(boxLengthSlice, i)
						for _, value := range video {
							tmp = append(tmp, *value...)
							size += len(*value)
							if size >= lastBoxSizeTotal+currentBoxSize {
								break
							}
						}
						err = websocket.Message.Send(ws, tmp[:lastBoxSizeTotal+currentBoxSize])
						if err != nil {
							break connFor
						}
						break
					}
				}
			} else if !isPlay {
				for i := typesLength - 1; i > -1; i-- {
					isContinue, err := getMoof(types[i], &playIndex, &isPlay, i, total, boxLengthSlice, ws, video, &videoDecodeTimeOffset, &audioDecodeTimeOffset)
					if err != nil {
						break connFor
					}
					if isContinue {
						continue
					} else {
						break
					}
				}
			} else {
				for i := playIndex + 2; i < typesLength; i++ {
					isContinue, err := getMoof(types[i], &playIndex, &isPlay, i, total, boxLengthSlice, ws, video, &videoDecodeTimeOffset, &audioDecodeTimeOffset)
					if err != nil {
						break connFor
					}
					if isContinue {
						continue
					} else {
						break
					}
				}
			}
		} else {
			okChan <- ok
		}
		readChanCount++
	}
}

func main() {
	go countReadAndWrite()
	http.Handle("/", websocket.Handler(handleWebSocketConn))
	if err := http.ListenAndServe(":1234", nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}
