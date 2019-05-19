package dfmgr

import (
	"testing"
	"time"

	cfg "github.com/lidstromberg/config"

	"golang.org/x/net/context"
)

var (
	appscope = "testapp"
)

func Test_NewPgMgr(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	_, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}
}
func Test_SaveJob(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()

	dsj := &DsJob{}
	dsj.AppScope = appscope
	dsj.JobID = "123456"
	dsj.JobType = "testerjobtype"
	dsj.LastStatus = CnstStateRunning
	dsj.CreatedDate = &now
	dsj.LastTouched = &now

	err = ab.SaveJob(ctx, dsj)
	if err != nil {
		t.Fatal(err)
	}
}
func Test_GetJob(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	JobID := "123456"

	jb, err := ab.GetJob(ctx, JobID)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Job is %v", jb)
}
func Test_GetJobCount1(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	ct, err := ab.GetAppScopeJobCount(ctx, appscope, "", "")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Job count is is %d", ct)
}
func Test_GetJobCount2(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	ct, err := ab.GetAppScopeJobCount(ctx, appscope, "testerjobtype", "")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Job count is is %d", ct)
}
func Test_GetJobCount3(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	ct, err := ab.GetAppScopeJobCount(ctx, appscope, "testerjobtype", CnstStateRunning)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Job count is is %d", ct)
}
func Test_GetAppScopeJob1(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := ab.GetAppScopeJobs(ctx, appscope, "", "")
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range jobs {
		t.Logf("Scope is %v", item.AppScope)
		t.Logf("JobID is %s", item.JobID)
		t.Logf("JobType is %s", item.JobType)
		t.Logf("CreatedDate is %v", item.CreatedDate)
		t.Logf("LastTouched is %v", item.LastTouched)
		t.Logf("LastStatus is %s", item.LastStatus)
	}
}
func Test_GetAppScopeJob2(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := ab.GetAppScopeJobs(ctx, appscope, "testerjobtype", "")
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range jobs {
		t.Logf("Scope is %v", item.AppScope)
		t.Logf("JobID is %s", item.JobID)
		t.Logf("JobType is %s", item.JobType)
		t.Logf("CreatedDate is %v", item.CreatedDate)
		t.Logf("LastTouched is %v", item.LastTouched)
		t.Logf("LastStatus is %s", item.LastStatus)
	}
}
func Test_GetAppScopeJob3(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := ab.GetAppScopeJobs(ctx, appscope, "testerjobtype", CnstStateRunning)
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range jobs {
		t.Logf("Scope is %v", item.AppScope)
		t.Logf("JobID is %s", item.JobID)
		t.Logf("JobType is %s", item.JobType)
		t.Logf("CreatedDate is %v", item.CreatedDate)
		t.Logf("LastTouched is %v", item.LastTouched)
		t.Logf("LastStatus is %s", item.LastStatus)
	}
}
func Test_DeleteJob(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	JobID := "123456"

	err = ab.DeleteJob(ctx, appscope, JobID)
	if err != nil {
		t.Fatal(err)
	}
}
func Test_DeleteJobArchive(t *testing.T) {
	ctx := context.Background()
	bc := cfg.NewConfig(ctx)

	ab, err := NewPgMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	err = ab.DeleteJobArchive(ctx, appscope)
	if err != nil {
		t.Fatal(err)
	}
}
