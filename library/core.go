package library

/*
This file contains the core of the library. It can perform the main manipulations with jobs (start/stop/stream output).
If you need in-memory storage of created jobs, users and jobs accessed by users, please use default API that wraps core capabilities of the library.
Default API can be found here: /library/default_api.go
*/

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
)

const (
	InProgress    = "InProgress"
	Exited        = "Exited"
	Terminated    = "Terminated"
	ExitedWithErr = "ExitedWithErr"
)

type Job struct {
	ID        uuid.UUID
	Process   *os.Process
	Output    ByteSlice
	OutputErr ByteSlice
	ExitCode  int
	JobStatus JobStatus
}

type JobStatus struct {
	mutex     sync.RWMutex
	jobStatus string
}

func (jobStatus *JobStatus) set(status string) {
	jobStatus.mutex.Lock()
	defer jobStatus.mutex.Unlock()
	jobStatus.jobStatus = status
}

func (jobStatus *JobStatus) get() string {
	jobStatus.mutex.RLock()
	defer jobStatus.mutex.RUnlock()
	return jobStatus.jobStatus
}

type ByteSlice struct {
	mutex sync.RWMutex
	slice [][]byte
	cond  *sync.Cond
}

func makeByteSlice() ByteSlice {
	return ByteSlice{cond: sync.NewCond(&sync.Mutex{})}
}

func (byteSlice *ByteSlice) append(sliceToAdd []byte) {
	byteSlice.mutex.Lock()
	defer byteSlice.mutex.Unlock()
	byteSlice.slice = append(byteSlice.slice, sliceToAdd)
	byteSlice.broadcastCondition()
}

func (byteSlice *ByteSlice) get(index int) []byte {
	byteSlice.mutex.RLock()
	defer byteSlice.mutex.RUnlock()
	return byteSlice.slice[index]
}

func (byteSlice *ByteSlice) len() int {
	byteSlice.mutex.Lock()
	defer byteSlice.mutex.Unlock()
	return len(byteSlice.slice)
}

func (byteSlice *ByteSlice) broadcastCondition() {
	byteSlice.cond.L.Lock()
	defer byteSlice.cond.L.Unlock()
	byteSlice.cond.Broadcast()
}

func (byteSlice *ByteSlice) waitForCondition() {
	byteSlice.cond.L.Lock()
	defer byteSlice.cond.L.Unlock()
	byteSlice.cond.Wait()
}

func StartJob(processName string, parameters ...string) (*Job, error) {
	cmd := exec.Command(processName, parameters...)
	stdout, stderr, err := getOutputPipesFromCmd(cmd)
	if err != nil {
		return nil, errors.New("can't get output from process")
	}
	err = cmd.Start()
	if err != nil {
		log.Println(err)
		return nil, errors.New("job was failed to start")
	}
	job := &Job{JobStatus: JobStatus{jobStatus: InProgress}, ID: uuid.New(), Process: cmd.Process, Output: makeByteSlice(), OutputErr: makeByteSlice()}
	var wg sync.WaitGroup
	wg.Add(2)
	go captureOutput(stdout, &job.Output, &wg)
	go captureOutput(stderr, &job.OutputErr, &wg)
	go updateJobStatus(cmd, job, &wg)
	return job, nil
}

// Please note, this function will terminate initiated process, but not its children.
// I can either address the group termination in the next PR or we can put this out of scope.
func StopJob(job *Job) (result bool, err error) {
	err = job.Process.Kill()
	if err != nil {
		result = false
	} else {
		result = true
	}
	return
}

func GetJobOutput(job *Job, ch chan<- []byte, isStderr bool) {
	var output *ByteSlice
	if isStderr {
		output = &job.OutputErr
	} else {
		output = &job.Output
	}
	var startRecordIndex int
	if job.JobStatus.get() == InProgress {
		startRecordIndex = getRealtimeOutput(job, ch, output)
	}
	getFinishedJobOutput(ch, output, startRecordIndex)
	close(ch)

}

func getFinishedJobOutput(ch chan<- []byte, output *ByteSlice, startRecordIndex int) {
	for i := startRecordIndex; i < output.len(); i++ {
		ch <- output.get(i)
	}
}

//Writes output to channel. Returns index of the next record to read
func getRealtimeOutput(job *Job, ch chan<- []byte, output *ByteSlice) int {
	i := 0
	for job.JobStatus.get() == InProgress {
		if output.len() > i {
			ch <- output.get(i)
			i++
		}
		output.waitForCondition()
	}
	return i
}

func captureOutput(output io.Reader, writer *ByteSlice, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		buf := make([]byte, 1024)
		n, err := output.Read(buf)
		writer.append(buf[:n])
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
	}
}

func getOutputPipesFromCmd(cmd *exec.Cmd) (stdoutPipe, stderrPipe io.ReadCloser, err error) {
	stdoutPipe, err = cmd.StdoutPipe()
	if err != nil {
		return
	}
	stderrPipe, err = cmd.StderrPipe()
	return
}

func updateJobStatus(cmd *exec.Cmd, job *Job, wg *sync.WaitGroup) {
	wg.Wait()
	err := cmd.Wait()
	if err != nil {
		log.Println(fmt.Sprintf("Job ID '%v': %v", job.ID, err))
	}
	job.ExitCode = cmd.ProcessState.ExitCode()
	job.JobStatus.set(getProcessStatusBasedOnCode(job.ExitCode))
	job.OutputErr.broadcastCondition()
	job.Output.broadcastCondition()
}

func getProcessStatusBasedOnCode(exitCode int) string {
	switch exitCode {
	case 0:
		return Exited
	case -1:
		return Terminated
	default:
		return ExitedWithErr
	}

}
