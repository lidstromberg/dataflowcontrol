package dfmgr

import "time"

//DsJob covers the basic identifier data required to control a dataflow job
type DsJob struct {
	AppScope    string     `json:"appscope" datastore:"appscope"`
	JobID       string     `json:"jobid" datastore:"jobid"`
	JobType     string     `json:"jobtype" datastore:"jobtype"`
	LastStatus  string     `json:"laststatus" datastore:"laststatus"`
	CreatedDate *time.Time `json:"createddate,omitempty" datastore:"createddate"`
	LastTouched *time.Time `json:"lasttouched,omitempty" datastore:"lasttouched"`
}

//JobSimpleMeta contains the basic data
type JobSimpleMeta struct {
	JobID        string `json:"jobid"`
	JobType      string `json:"jobtype"`
	CurrentState string `json:"currentstate"`
}

//JobRunParameter contains the full set of parameters to run a datflow job
type JobRunParameter struct {
	CustomParameters   map[string]string `json:"customparameters"`
	RuntimeEnvironment map[string]string `json:"runtimeenvironment"`
	JobRequest         map[string]string `json:"jobrequest"`
}
