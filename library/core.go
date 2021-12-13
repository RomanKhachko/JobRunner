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
	"sync/atomic"
)

// Job status constants
const (
	InProgress    = "InProgress"
	Exited        = "Exited"
	Terminated    = "Terminated"
	ExitedWithErr = "ExitedWithErr"
)

// Job stores the information about a job created by StartJob.
type Job struct {
	ID        uuid.UUID   // unique ID of the job
	process   *os.Process // underlying process associated with a job
	output    ByteSlice   // contains output from stdout
	outputErr ByteSlice   // contains output from stderr
	ExitCode  int         // exit code of the underlying process
	JobStatus JobStatus   // status of the job (one of InProgress, Exited, Terminated, ExitedWithErr)
}

// JobStatus represents status of the job.
type JobStatus struct {
	mutex     sync.RWMutex
	jobStatus string
}

// Set sets the status of the job.
func (jobStatus *JobStatus) Set(status string) {
	jobStatus.mutex.Lock()
	defer jobStatus.mutex.Unlock()
	jobStatus.jobStatus = status
}

// Get returns the status of the job.
func (jobStatus *JobStatus) Get() string {
	jobStatus.mutex.RLock()
	defer jobStatus.mutex.RUnlock()
	return jobStatus.jobStatus
}

type ByteSlice struct {
	mutex       sync.RWMutex
	slice       [][]byte
	cond        *sync.Cond
	doneWriting atomic.Value
}

func makeByteSlice() ByteSlice {
	var doneWriting atomic.Value
	doneWriting.Store(false)
	return ByteSlice{cond: sync.NewCond(&sync.Mutex{}), doneWriting: doneWriting}
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
	byteSlice.cond.Wait()
}

func (byteSlice *ByteSlice) lockCondition() {
	byteSlice.cond.L.Lock()
}

func (byteSlice *ByteSlice) unlockCondition() {
	byteSlice.cond.L.Unlock()
}

// StartJobs starts a requested process, starts job handler function running cuncurrently.
// Once job is started, it will return. Returned Job contains all the relevent information about job.
// If process couldn't be started or there were issues with getting output, error is returned.
func StartJob(processName string, parameters ...string) (*Job, error) {
	cmd := exec.Command(processName, parameters...)
	stdout, stderr, err := getOutputPipesFromCmd(cmd)
	if err != nil {
		log.Println(err)
		return nil, errors.New("can't Get output from process")
	}
	err = cmd.Start()
	if err != nil {
		log.Println(err)
		return nil, errors.New("job was failed to start")
	}
	job := &Job{JobStatus: JobStatus{jobStatus: InProgress}, ID: uuid.New(), process: cmd.Process, output: makeByteSlice(), outputErr: makeByteSlice()}
	go handleJob(cmd, job, stdout, stderr)
	return job, nil
}

// StopJob terminates the process associated with a job. Returns true in case of success, false and error otherwise.
// Please note, this function will terminate initiated process, but not its children.
// It can either be addressed the group termination in one of the next PRs or can be put out of scope.
func StopJob(job *Job) (result bool, err error) {
	err = job.process.Kill()
	if err != nil {
		log.Println(err)
		result = false
	} else {
		result = true
	}
	return
}

// GetJobStdoutOutput streams stdout output of the job from the beginning until the job is completed.
// Once output is returned, channel will be closed.
func GetJobStdoutOutput(job *Job, ch chan<- []byte) {
	getJobOutput(job, ch, &job.output)
}

// GetJobStderrOutput streams stderr output of the job from the beginning until the job is completed.
// Once output is returned, channel will be closed.
func GetJobStderrOutput(job *Job, ch chan<- []byte) {
	getJobOutput(job, ch, &job.outputErr)
}

// getJobOutput streams output of the job from the beginning until the job is completed.
// Once output is returned, channel will be closed.
func getJobOutput(job *Job, ch chan<- []byte, output *ByteSlice) {
	var startRecordIndex int
	if job.JobStatus.Get() == InProgress {
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
	for job.JobStatus.Get() == InProgress {
		if output.len() > i {
			ch <- output.get(i)
			i++
		}
		output.lockCondition()
		for !output.doneWriting.Load().(bool) && output.len() <= i {
			output.waitForCondition()
		}
		output.unlockCondition()
	}
	return i
}

func captureOutput(output io.Reader, writer *ByteSlice, wg *sync.WaitGroup) {
	defer wg.Done()
	defer writer.doneWriting.Store(true)
	defer writer.broadcastCondition()
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

func updateJobStatus(cmd *exec.Cmd, job *Job) {
	err := cmd.Wait()
	if err != nil {
		log.Println(fmt.Sprintf("Job ID '%v': %v", job.ID, err))
	}
	job.ExitCode = cmd.ProcessState.ExitCode()
	job.JobStatus.Set(getProcessStatusBasedOnCode(job.ExitCode))
}

func handleJob(cmd *exec.Cmd, job *Job, stdout, stderr io.ReadCloser) {
	var wg sync.WaitGroup
	wg.Add(2)
	go captureOutput(stdout, &job.output, &wg)
	go captureOutput(stderr, &job.outputErr, &wg)
	wg.Wait()
	updateJobStatus(cmd, job)
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
