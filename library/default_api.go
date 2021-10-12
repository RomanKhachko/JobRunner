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

var userJobContainer = NewUserJobsContainer()

type UserJobsContainer struct {
	userJobs map[string]map[string]*Job
	mutex    sync.RWMutex
}

func NewUserJobsContainer() UserJobsContainer {
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

func StartUserJob(user string, processName string, parameters ...string) (string, error) {
	job, err := StartJob(processName, parameters...)
	if err != nil {
		return "", err
	}
	userJobContainer.addJob(user, job)
	return job.ID.String(), nil
}

func StopUserJob(user string, jobID string) (bool, error) {
	if job, err := userJobContainer.getJob(user, jobID); err != nil {
		return false, err
	} else {
		return StopJob(job)
	}
}

func GetUserJobStatus(user string, jobID string) (jobStatus string, exitCode string, err error) {
	if job, getJobErr := userJobContainer.getJob(user, jobID); getJobErr != nil {
		err = getJobErr
		return
	} else {
		if jobStatus = job.JobStatus.get(); jobStatus != InProgress {
			exitCode = strconv.Itoa(job.ExitCode)
		}
		return
	}
}

func GetUserJobOutput(user string, jobID string, ch chan []byte, isStdErr bool) error {
	if job, err := userJobContainer.getJob(user, jobID); err != nil {
		return err
	} else {
		GetJobOutput(job, ch, isStdErr)
		return nil
	}
}
