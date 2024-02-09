package dfmgr

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	cfg "github.com/lidstromberg/config"
	lg "github.com/lidstromberg/log"
	sto "github.com/lidstromberg/storage"

	google "golang.org/x/oauth2/google"
	df "google.golang.org/api/dataflow/v1b3"
)

// DfMgr covers job management functionality
type DfMgr struct {
	dfsvc *df.Service
	ds    *PgMgr
	st    *sto.StorMgr
	bc    cfg.ConfigSetting
}

// NewGoogleCredentials returns a GCP/Google credential from a supplied file.. reference... might not be needed
func NewGoogleCredentials(ctx context.Context, path string) (*google.Credentials, error) {
	data, err := os.ReadFile("/path/to/key-file.json")
	if err != nil {
		return nil, err
	}

	//use this for testing
	creds, err := google.CredentialsFromJSON(ctx, data, "https://www.googleapis.com/auth/devstorage.full_control", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive")
	if err != nil {
		return nil, err
	}

	return creds, nil
}

// NewMgr returns a new manager
func NewMgr(ctx context.Context, bc cfg.ConfigSetting) (*DfMgr, error) {
	preflight(ctx, bc)

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "NewMgr", "info", "start")
	}

	//use this for deployment (it will use the service account within appengine)
	client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/devstorage.full_control", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive")
	if err != nil {
		return nil, err
	}

	//dataflow client
	dfs, err := df.New(client)
	if err != nil {
		return nil, err
	}

	//data mgr
	ds, err := NewPgMgr(ctx, bc)
	if err != nil {
		return nil, err
	}

	//storage client
	stor, err := sto.NewMgr(ctx, bc)
	if err != nil {
		return nil, err
	}

	//dataflow mgr
	abm := &DfMgr{
		dfsvc: dfs,
		ds:    ds,
		st:    stor,
		bc:    bc,
	}

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "NewMgr", "info", "end")
	}

	return abm, nil
}

// JobStart starts a job from a template
func (dfm *DfMgr) JobStart(ctx context.Context, appscope, jobtype string, jobParam *JobRunParameter) (*JobSimpleMeta, error) {
	if EnvDebugOn {
		lg.LogEvent("DfMgr", "JobStart", "info", "start")
	}

	//runtime parameters
	param := make(map[string]string)
	param = jobParam.CustomParameters

	//convert the int64 strings
	mxwrk, err := strconv.ParseInt(jobParam.RuntimeEnvironment["maxWorkers"], 10, 64)
	if err != nil {
		return nil, err
	}

	nmwrk, err := strconv.ParseInt(jobParam.RuntimeEnvironment["numWorkers"], 10, 64)
	if err != nil {
		return nil, err
	}

	//runtime
	rn := &df.RuntimeEnvironment{}
	rn.MaxWorkers = mxwrk
	rn.MachineType = jobParam.RuntimeEnvironment["machineType"]
	rn.NumWorkers = nmwrk
	rn.TempLocation = jobParam.RuntimeEnvironment["tempLocation"]

	//current timestamp
	now := time.Now()
	dt := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.UTC).Unix()

	jobname := fmt.Sprintf(jobParam.JobRequest["jobName"], strconv.FormatInt(dt, 10))

	//job request
	jbc := &df.CreateJobFromTemplateRequest{}
	jbc.JobName = jobname
	jbc.Location = jobParam.JobRequest["location"]
	jbc.GcsPath = jobParam.JobRequest["gcsPath"]
	jbc.Environment = rn
	jbc.Parameters = param

	//get a new templates service
	svc := df.NewProjectsLocationsTemplatesService(dfm.dfsvc)

	//create the caller and set the context
	jbr := svc.Create(dfm.bc.GetConfigValue(ctx, "EnvDfGcpProject"), dfm.bc.GetConfigValue(ctx, "EnvDfGcpRegion"), jbc)
	jbr.Context(ctx)

	//then run the job
	jb, err := jbr.Do()
	if err != nil {
		return nil, err
	}

	//archive info
	dsjb := &DsJob{}

	//apply the values to the dsjob for datastore
	dsjb.AppScope = appscope
	dsjb.JobID = jb.Id
	dsjb.JobType = jobtype
	dsjb.CreatedDate = &now
	dsjb.LastTouched = &now
	dsjb.LastStatus = jb.CurrentState

	//collect the basic meta required to track the job
	jbmeta := &JobSimpleMeta{
		JobID:        jb.Id,
		JobType:      jobParam.JobRequest["jobType"],
		CurrentState: jb.CurrentState,
	}

	//save the job.. if the datastore save fails, don't fail the entire action.. just report the save failure
	err = dfm.ds.SaveJob(ctx, dsjb)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "JobStart", "info", "end")
	}

	//probably only need to track the jobid,
	return jbmeta, nil
}

// GetJobStatus gets a job from an id
func (dfm *DfMgr) GetJobStatus(ctx context.Context, jobID string) (*df.Job, error) {
	if EnvDebugOn {
		lg.LogEvent("DfMgr", "JobStatus", "info", "start")
	}

	jbsvc := df.NewProjectsLocationsJobsService(dfm.dfsvc)

	msgcall := jbsvc.Get(dfm.bc.GetConfigValue(ctx, "EnvDfGcpProject"), dfm.bc.GetConfigValue(ctx, "EnvDfGcpRegion"), jobID)
	msgcall.Context(ctx)

	jb, err := msgcall.Do()
	if err != nil {
		return nil, err
	}

	//update the job status record
	err = dfm.ds.SetJobStatus(ctx, jobID, jb.CurrentState)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "JobStatus", "info", "end")
	}

	return jb, nil
}

// JobStop stops a job
func (dfm *DfMgr) JobStop(ctx context.Context, jobID string) (*df.Job, error) {
	if EnvDebugOn {
		lg.LogEvent("DfMgr", "JobStop", "info", "start")
	}

	//first get the job status
	currJb, err := dfm.GetJobStatus(ctx, jobID)
	if err != nil {
		return nil, err
	}

	//if the job state isn't running, then it can't be cancelled, so return the job as it is
	if currJb.CurrentState != CnstStateRunning {
		return currJb, nil
	}

	jbsvc := df.NewProjectsLocationsJobsService(dfm.dfsvc)

	msgcall := jbsvc.Get(dfm.bc.GetConfigValue(ctx, "EnvDfGcpProject"), dfm.bc.GetConfigValue(ctx, "EnvDfGcpRegion"), jobID)
	msgcall.Context(ctx)

	jb, err := msgcall.Do()
	if err != nil {
		return nil, err
	}

	jb.RequestedState = CnstStateCancelled

	jbcl := jbsvc.Update(dfm.bc.GetConfigValue(ctx, "EnvDfGcpProject"), dfm.bc.GetConfigValue(ctx, "EnvDfGcpRegion"), jobID, jb)
	jbcl.Context(ctx)

	jb, err = jbcl.Do()
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "JobStop", "info", "end")
	}

	return jb, nil
}

// GetGcsJobDefinition retrieves a GCS bucket hosted set of parameters for a dataflow job
func (dfm *DfMgr) GetGcsJobDefinition(ctx context.Context, filename string) (*JobRunParameter, error) {
	if EnvDebugOn {
		lg.LogEvent("DfMgr", "GetGcsJobDefinition", "info", "start")
	}

	var param *JobRunParameter

	//get the bucket bytes
	data, err := dfm.st.GetBucketFileData(ctx, dfm.bc.GetConfigValue(ctx, "EnvDfParamsBucket"), filename)
	if err != nil {
		return nil, err
	}

	//unmarshall into the parameter object
	err = json.Unmarshal(data, &param)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "GetGcsJobDefinition", "info", "end")
	}

	return param, nil
}

// SetGcsJobDefinition retrieves a GCS bucket hosted set of parameters for a dataflow job
func (dfm *DfMgr) SetGcsJobDefinition(ctx context.Context, filename, contenttype string, jd *JobRunParameter) error {
	if EnvDebugOn {
		lg.LogEvent("DfMgr", "SetGcsJobDefinition", "info", "start")
	}

	//convert the job definition to json bytes
	data, err := json.Marshal(jd)
	if err != nil {
		return err
	}

	//get the bucket bytes
	err = dfm.st.WriteBucketFile(ctx, dfm.bc.GetConfigValue(ctx, "EnvDfParamsBucket"), contenttype, filename, data)
	if err != nil {
		return err
	}

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "SetGcsJobDefinition", "info", "end")
	}

	return nil
}

// GetJob gets a job by id
func (dfm *DfMgr) GetJob(ctx context.Context, jobID string) (*DsJob, error) {
	if EnvDebugOn {
		lg.LogEvent("DfMgr", "GetJob", "info", "start")
	}

	jb, err := dfm.ds.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "GetJob", "info", "end")
	}

	return jb, nil
}

// GetJobs gets a list of jobs for an appscope
func (dfm *DfMgr) GetJobs(ctx context.Context, appscope, jobtype, jobstate string) ([]*DsJob, error) {
	if EnvDebugOn {
		lg.LogEvent("DfMgr", "GetActiveJobs", "info", "start")
	}

	jbs, err := dfm.ds.GetAppScopeJobs(ctx, appscope, jobtype, jobstate)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "GetActiveJobs", "info", "end")
	}

	return jbs, nil
}

// GetLatestJobs gets the most recent job for an appscope
func (dfm *DfMgr) GetLatestJobs(ctx context.Context, appscope, jobtype string, limit int) ([]*DsJob, error) {
	if EnvDebugOn {
		lg.LogEvent("DfMgr", "GetLatestJob", "info", "start")
	}

	jbs, err := dfm.ds.GetLatestAppScopeJob(ctx, appscope, jobtype, limit)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("DfMgr", "GetLatestJob", "info", "end")
	}

	return jbs, nil
}
