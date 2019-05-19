package dfmgr

import (
	"encoding/json"

	cfg "github.com/lidstromberg/config"
	lg "github.com/lidstromberg/log"

	"golang.org/x/net/context"

	"database/sql"

	//blank import required for db
	_ "github.com/lib/pq"
)

//PgMgr handles interactions with a postgres db store
type PgMgr struct {
	ds *sql.DB
}

//NewPgMgr creates a new manager
func NewPgMgr(ctx context.Context, bc cfg.ConfigSetting) (*PgMgr, error) {
	preflight(ctx, bc)

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "NewPgMgr", "info", "start")
	}

	db, err := sql.Open(bc.GetConfigValue(ctx, "EnvSqlDst"), bc.GetConfigValue(ctx, "EnvSqlConnection"))
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	pg1 := &PgMgr{
		ds: db,
	}

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "NewPgMgr", "info", "end")
	}

	return pg1, nil
}

//SaveJob saves a job
func (pgm *PgMgr) SaveJob(ctx context.Context, mdp *DsJob) error {
	if EnvDebugOn {
		lg.LogEvent("PgMgr", "SaveJob", "info", "start")
	}

	//run the query
	_, err := pgm.ds.Exec("select public.set_jobcontrol($1, $2, $3, $4)", mdp.AppScope, mdp.JobID, mdp.JobType, mdp.LastStatus)
	if err != nil {
		return err
	}

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "SaveJob", "info", "end")
	}

	return nil
}

//SetJobStatus sets a job status
func (pgm *PgMgr) SetJobStatus(ctx context.Context, jobid, jobstate string) error {
	if EnvDebugOn {
		lg.LogEvent("PgMgr", "SetJobStatus", "info", "start")
	}

	_, err := pgm.ds.Exec("select public.set_jobstatus($1,$2)", jobid, jobstate)
	if err != nil {
		return err
	}

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "SetJobStatus", "info", "end")
	}
	return nil
}

//GetJob gets a specific job
func (pgm *PgMgr) GetJob(ctx context.Context, jobid string) (*DsJob, error) {
	if EnvDebugOn {
		lg.LogEvent("PgMgr", "GetJob", "info", "start")
	}

	//run the query
	var (
		jsonString sql.NullString
		param      DsJob
	)
	err := pgm.ds.QueryRow("select get_jobcontrol as rs from public.get_jobcontrol($1)", jobid).Scan(&jsonString)
	if err != nil {
		return nil, err
	}

	//if the result is null, return the appropriate message
	if !jsonString.Valid {
		return nil, ErrNoDataFound
	}

	//convert the json result
	err = json.Unmarshal([]byte(jsonString.String), &param)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "GetJob", "info", "end")
	}

	//return the model parameter string
	return &param, nil
}

//GetAppScopeJobs gets the Jobs for a specified appscope, jobtype and jobstate (latter two can be empty string)
func (pgm *PgMgr) GetAppScopeJobs(ctx context.Context, appscope, jobtype, jobstate string) ([]*DsJob, error) {
	if EnvDebugOn {
		lg.LogEvent("PgMgr", "GetAppScopeJobs", "info", "start")
	}

	//run the query
	var (
		jsonString sql.NullString
		param      []*DsJob
	)

	err := pgm.ds.QueryRow("select get_appscopejobcontrol as rs from public.get_appscopejobcontrol($1, $2, $3)", appscope, jobtype, jobstate).Scan(&jsonString)
	if err != nil {
		return nil, err
	}

	//if the result is null, return the appropriate message
	if !jsonString.Valid {
		return nil, ErrNoDataFound
	}

	//convert the json result
	err = json.Unmarshal([]byte(jsonString.String), &param)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "GetAppScopeJobs", "info", "end")
	}

	//return the model parameter string
	return param, nil
}

//GetLatestAppScopeJob gets the lastest Job for a specified appscope
func (pgm *PgMgr) GetLatestAppScopeJob(ctx context.Context, appscope, jobtype string, limit int) ([]*DsJob, error) {
	if EnvDebugOn {
		lg.LogEvent("PgMgr", "GetLatestAppScopeJob", "info", "start")
	}

	//run the query
	var (
		jsonString sql.NullString
		param      []*DsJob
	)

	err := pgm.ds.QueryRow("select get_latestappscopejobcontrol as rs from public.get_latestappscopejobcontrol($1, $2, $3)", appscope, jobtype, limit).Scan(&jsonString)
	if err != nil {
		return nil, err
	}

	//if the result is null, return the appropriate message
	if !jsonString.Valid {
		return nil, ErrNoDataFound
	}

	//convert the json result
	err = json.Unmarshal([]byte(jsonString.String), &param)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "GetLatestAppScopeJob", "info", "end")
	}

	//return the model parameter string
	return param, nil
}

//GetAppScopeJobCount gets the count of Jobs for a specified appscope
func (pgm *PgMgr) GetAppScopeJobCount(ctx context.Context, appscope, jobtype, jobstate string) (int64, error) {
	if EnvDebugOn {
		lg.LogEvent("PgMgr", "GetAppScopeJobCount", "info", "start")
	}

	//run the query
	var result sql.NullInt64
	err := pgm.ds.QueryRow("select jsonb_array_length(get_appscopejobcontrol) as rs from public.get_appscopejobcontrol($1, $2, $3)", appscope, jobtype, jobstate).Scan(&result)
	if err != nil {
		return -1, err
	}

	//if the result is null, return zero as the count
	if !result.Valid {
		return 0, nil
	}

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "GetAppScopeJobCount", "info", "end")
	}

	//return the result
	return result.Int64, nil
}

//DeleteJob clears a job
func (pgm *PgMgr) DeleteJob(ctx context.Context, appscope, jobid string) error {
	if EnvDebugOn {
		lg.LogEvent("PgMgr", "DeleteJob", "info", "start")
	}

	//run the query
	_, err := pgm.ds.Exec("select public.delete_jobcontrol($1, $2)", appscope, jobid)
	if err != nil {
		return err
	}

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "DeleteJob", "info", "end")
	}

	return nil
}

//DeleteJobArchive clears the job archive (older than 24 hours)
func (pgm *PgMgr) DeleteJobArchive(ctx context.Context, appscope string) error {
	if EnvDebugOn {
		lg.LogEvent("PgMgr", "DeleteJobArchive", "info", "start")
	}

	//run the query
	_, err := pgm.ds.Exec("select public.delete_jobcontrolarchive($1)", appscope)
	if err != nil {
		return err
	}

	if EnvDebugOn {
		lg.LogEvent("PgMgr", "DeleteJobArchive", "info", "end")
	}

	return nil
}
