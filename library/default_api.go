/*
Package library implements manipulations with jobs. Supported operations: start, stop, output streaming, status.

Library has two API:
1) low level (core). It provides functions to manipulate jobs.
2) high level (default). It wraps core functions and adds in memory storage and access control capabilities.
*/
package library

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
)

/*
This file contains the default implementation of library with user and job caching support.
Basically it works as a little service, providing in memory storage and user support out of the box.
/library/core should be used in case low level API is needed. In that case, it'll be pure job controlling API without user and job storage.
*/

var userJobContainer = makeUserJobsContainer()

type UserJobsContainer struct {
	userJobs map[string]map[string]*Job
	mutex    sync.RWMutex
}

func makeUserJobsContainer() UserJobsContainer {
	return UserJobsContainer{
		userJobs: make(map[string]map[string]*Job),
	}
}

func (userJobsContainer *UserJobsContainer) addJob(userName string, job *Job) {
	userJobsContainer.mutex.Lock()
	defer userJobsContainer.mutex.Unlock()
	userJobs, ok := userJobsContainer.userJobs[userName]
	jobId := job.ID.String()
	if ok {
		userJobs[jobId] = job
	} else {
		jobs := map[string]*Job{jobId: job} //make(map[string]*Job)
		userJobsContainer.userJobs[userName] = jobs
	}
}

func (userJobsContainer *UserJobsContainer) getJob(userName string, jobId string) (*Job, error) {
	userJobsContainer.mutex.RLock()
	defer userJobsContainer.mutex.RUnlock()
	if userJobs, ok := userJobsContainer.userJobs[userName]; ok {
		if job, ok := userJobs[jobId]; ok {
			return job, nil
		} else {
			errorText := fmt.Sprintf("user doesn't have a job with id '%v'", jobId)
			log.Println(errorText)
			return nil, errors.New(errorText)
		}
	} else {
		errorText := fmt.Sprintf("user %v hasn't created any jobs yet", userName)
		log.Println(errorText)
		return nil, errors.New(errorText)
	}
}

// StartUserJob initiates a job and associates it with a user requested it. Job id is returned when started successfully.
// If job can't be started, error will be returned.
func StartUserJob(user string, processName string, parameters ...string) (string, error) {
	job, err := StartJob(processName, parameters...)
	if err != nil {
		return "", err
	}
	userJobContainer.addJob(user, job)
	return job.ID.String(), nil
}

// StopUserJob terminates the job started by user. Returns true in case of success, false and error otherwise.
// If user doesn't have any jobs or requested job doesn't belong to a user, false and error will be returned.
func StopUserJob(user string, jobID string) (bool, error) {
	if job, err := userJobContainer.getJob(user, jobID); err != nil {
		return false, err
	} else {
		return StopJob(job)
	}
}

// GetUserJobStatus returns status of the job. If process exited, returns exit code as a second parameter.
// If user doesn't have any jobs or requested job doesn't belong to a user, false and error will be returned.
func GetUserJobStatus(user string, jobID string) (jobStatus string, exitCode string, err error) {
	if job, getJobErr := userJobContainer.getJob(user, jobID); getJobErr != nil {
		err = getJobErr
		return
	} else {
		if jobStatus = job.JobStatus.Get(); jobStatus != InProgress {
			exitCode = strconv.Itoa(job.ExitCode)
		}
		return
	}
}

// GetUserJobStdoutOutput streams stdout output of the job from the beginning until the job is completed.
// Once output is returned, channel will be closed.
// If user doesn't have any jobs or requested job doesn't belong to a user, false and error will be returned.
func GetUserJobStdoutOutput(user string, jobID string, ch chan<- []byte) error {
	return getUserJobOutput(user, jobID, ch, GetJobStdoutOutput)
}

// GetUserJobStderrOutput streams stderr output of the job from the beginning until the job is completed.
// Once output is returned, channel will be closed.
// If user doesn't have any jobs or requested job doesn't belong to a user, false and error will be returned.
func GetUserJobStderrOutput(user, jobID string, ch chan<- []byte) error {
	return getUserJobOutput(user, jobID, ch, GetJobStderrOutput)
}

func getUserJobOutput(user, jobID string, ch chan<- []byte, jobOutputFunc func(*Job, chan<- []byte)) error {
	if job, err := userJobContainer.getJob(user, jobID); err != nil {
		return err
	} else {
		jobOutputFunc(job, ch)
		return nil
	}
}
