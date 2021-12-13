package library

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// I'm not a big fun of 'big' tests that are testing several aspects at one place. However, due to lack of time,
// I combine job start, status and output in scope of one test
func TestJobStartStatusAndOutputOfExitedJob(t *testing.T) {
	userName := "Roman"
	jobId, _ := StartUserJob(userName, "ls")
	pollTheJobStatusWithInterval(UserJob{userName, jobId}, Exited, PollTimeout{2, 10}, t)
	jobStatus, exitCode, _ := GetUserJobStatus(userName, jobId)
	expectedJobStatus := Exited
	if jobStatus != expectedJobStatus {
		t.Fatalf(`Expected job status is %v; actual is %v`, expectedJobStatus, jobStatus)
	}
	expectedExitCode := "0"
	if exitCode != expectedExitCode {
		t.Fatalf(`Expected exit code is %v; actual is %v`, expectedExitCode, exitCode)
	}
	// test getting output. Since job is finished, there will be a streaming of finished job output
	ch := make(chan []byte)
	go GetUserJobStdoutOutput(userName, jobId, ch)
	var output string
	for i := range ch {
		output += string(i)
	}
	expectedOutputSubString := "library"
	if !strings.Contains(output, "library") {
		t.Fatalf(`Expected value %v; actual output is %v`, expectedOutputSubString, output)
	}
}

func TestRealtimeOutputAndJobTermination(t *testing.T) {
	userName := "Roman"
	jobId, err := StartUserJob(userName, "../testdata/long_running_job.sh")
	verifyJobStartPrecondition(err, t)
	ch := make(chan []byte)
	go GetUserJobStdoutOutput(userName, jobId, ch)

	// here we're giving two second for the job before terminating it
	var terminationResult atomic.Value
	go func() {
		time.Sleep(2 * time.Second)
		res, _ := StopUserJob(userName, jobId)
		terminationResult.Store(res)
	}()
	var output string
	for i := range ch {
		output += string(i)
	}
	if !strings.Contains(output, "stage  2") {
		t.Fatalf("output can't be empty")
	}
	jobStatus, exitCode, _ := GetUserJobStatus(userName, jobId)
	expectedJobStatus := Terminated
	if jobStatus != expectedJobStatus {
		t.Fatalf(`Expected job status is %v; actual is %v`, expectedJobStatus, jobStatus)
	}
	if !terminationResult.Load().(bool) {
		t.Fatalf(`Returned termination result is supposed to be %v; actual is %v`, true, terminationResult)
	}
	expectedExitCode := "-1"
	if exitCode != expectedExitCode {
		t.Fatalf(`Expected exit code is %v; actual is %v`, expectedExitCode, exitCode)
	}
}

func TestUserIsNotFound(t *testing.T) {
	userName := "Roman, but with no jobs created"
	_, exitCode, err := GetUserJobStatus(userName, "")
	expectedErrorText := fmt.Sprintf("user %v hasn't created any jobs yet", userName)
	if err.Error() != expectedErrorText {
		t.Fatalf("Actual error is '%v'; expected error text is '%v'", err, expectedErrorText)
	}
	if exitCode != "" {
		t.Fatalf(`Exit code is supposed to be empty. Actual exit code is %v`, exitCode)
	}
}

func TestUserJobNotFound(t *testing.T) {
	userName := "Roman"
	StartUserJob(userName, "ls")
	jobID := "not_exist"
	_, _, err := GetUserJobStatus(userName, jobID)
	expectedErrorText := fmt.Sprintf("user doesn't have a job with id '%v'", jobID)
	if err.Error() != expectedErrorText {
		t.Fatalf("Actual error is '%v'; expected error text is '%v'", err, expectedErrorText)
	}
}

func TestGetJobStderrOutput(t *testing.T) {
	userName := "Roman"
	jobID, err := StartUserJob(userName, "../testdata/failed_script.sh")
	verifyJobStartPrecondition(err, t)
	ch := make(chan []byte)
	go GetUserJobStderrOutput(userName, jobID, ch)
	var output string
	for i := range ch {
		output += string(i)
	}
	expectedStdErrText := "division by 0"
	if !strings.Contains(output, expectedStdErrText) {
		t.Fatalf("Actual stdeer is '%v'; expected is '%v'", output, expectedStdErrText)
	}
}

func verifyJobStartPrecondition(err error, t *testing.T) {
	if err != nil {
		t.Fatalf("Test precondition is failed. Job was supposed to start successfully")
	}
}

type PollTimeout struct {
	intervalMs, timeoutMs int
}

type UserJob struct {
	user, jobID string
}

func pollTheJobStatusWithInterval(userJobPair UserJob, expectedStatus string, pollTimeout PollTimeout, t *testing.T) {
	ch := make(chan bool)
	go func() {
		for {
			status, _, _ := GetUserJobStatus(userJobPair.user, userJobPair.jobID)
			isStatusChanged := status == expectedStatus
			if isStatusChanged {
				ch <- isStatusChanged
			} else {
				time.Sleep(time.Duration(pollTimeout.intervalMs) * time.Millisecond)
			}
		}
	}()
	waitForBoolCondition(ch, pollTimeout, t, "JobStatus is not as expected")
}

func waitForBoolCondition(ch chan bool, pollTimeout PollTimeout, t *testing.T, errorText string) {
	for {
		select {
		case <-ch:
			return
		case <-time.After(time.Duration(pollTimeout.timeoutMs) * time.Millisecond):
			t.Fatalf(errorText)
		}
	}
}
