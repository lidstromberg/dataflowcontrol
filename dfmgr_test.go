package dfmgr

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	cfg "github.com/lidstromberg/config"

	"golang.org/x/net/context"
)

var (
	params     = "dataflow/jobdef/002_featureload.json"
	jbappscope = "testapp"
	jobtype    = "testappjobtype"
)

func Test_NewMgr(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	_, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}
}
func Test_GetGcsJobDefinition(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	df, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	param, err := df.GetGcsJobDefinition(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Param: %v", param)
	t.Logf("Param: %v", param.CustomParameters)

	//Also test the jobname string substitution
	now := time.Now()
	dt := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).Unix()

	t.Logf("Jobname: %s", fmt.Sprintf(param.JobRequest["jobName"], strconv.FormatInt(dt, 10)))
}
func Test_SetGcsJobDefinition(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	df, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	param, err := df.GetGcsJobDefinition(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Param: %v", param)

	//get the namespace
	ns := strings.Replace(param.CustomParameters["etlVersionnamespace"], "v", "", -1)

	//convert it to int64
	nsint, err := strconv.ParseInt(ns, 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	//increment the value
	nsint++

	//write it back to the collection
	param.CustomParameters["etlVersionnamespace"] = fmt.Sprintf("v%s", strconv.FormatInt(nsint, 10))

	//write the file back to GCS
	err = df.SetGcsJobDefinition(ctx, params, "json", param)
	if err != nil {
		t.Fatal(err)
	}
}
func Test_StartJob(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	df, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	param, err := df.GetGcsJobDefinition(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	jb, err := df.JobStart(ctx, jbappscope, jobtype, param)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%v", jb)
}
func Test_GetJobs(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	df, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	jbs, err := df.GetJobs(ctx, jbappscope, jobtype, "")
	if err != nil {
		if err != ErrNoDataFound {
			t.Fatal(err)
		}
	}

	for _, item := range jbs {
		t.Logf("%s", item.JobID)

		jb, err := df.GetJobStatus(ctx, item.JobID)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("%s", jb.CurrentState)
	}
}
func Test_GetLatestJobs(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	df, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	jbs, err := df.GetLatestJobs(ctx, jbappscope, jobtype, 2)
	if err != nil {
		if err != ErrNoDataFound {
			t.Fatal(err)
		}
	}

	for _, item := range jbs {
		t.Logf("%s", item.JobID)

		jb, err := df.GetJobStatus(ctx, item.JobID)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("%s", jb.CurrentState)
	}
}
func Test_StopJob(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	df, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	jbs, err := df.GetLatestJobs(ctx, jbappscope, jobtype, 1)
	if err != nil {
		if err != ErrNoDataFound {
			t.Fatal(err)
		}
	}

	jbst, err := df.GetJobStatus(ctx, jbs[0].JobID)
	if err != nil {
		t.Fatal(err)
	}

	if jbst.CurrentState == CnstStateRunning {
		_, err := df.JobStop(ctx, jbs[0].JobID)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second * 5)
	}

	jbst, err = df.GetJobStatus(ctx, jbs[0].JobID)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%v", jbst)
}
func Test_PollJobs(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	df, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	//storage for the df jobs
	var jbs []*JobSimpleMeta

	//get the job history
	hst, err := df.GetJobs(ctx, appscope, jobtype, "")
	if err != nil {
		t.Fatal(err)
	}

	//iterate over the jobs and get their status from dataflow
	for _, item := range hst {
		jb, err := df.GetJobStatus(ctx, item.JobID)
		if err != nil {
			t.Fatal(err)
		}

		//collect the basic meta required to track the job
		jbmeta := &JobSimpleMeta{
			JobID:        jb.Id,
			JobType:      jobtype,
			CurrentState: jb.CurrentState,
		}

		jbs = append(jbs, jbmeta)
	}

	for _, item := range jbs {
		t.Logf("%s", item.JobID)
		t.Logf("%s", item.CurrentState)
	}
}
