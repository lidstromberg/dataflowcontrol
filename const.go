package dfmgr

//taken from https://godoc.org/google.golang.org/api/dataflow/v1b3

const (
	//CnstStateUnknown The job's run state isn't specified.
	CnstStateUnknown = "JOB_STATE_UNKNOWN"
	//CnstStateStopped indicates that the job has not yet started to run
	CnstStateStopped = "JOB_STATE_STOPPED"
	//CnstStateRunning indicates that the job is currently running
	CnstStateRunning = "JOB_STATE_RUNNING"
	//CnstStateDone indicates that the job has successfully completed. This is a terminal job state.  This state may be set by the Cloud Dataflow service, as a transition from `JOB_STATE_RUNNING`. It may also be set via a Cloud Dataflow `UpdateJob` call, if the job has not yet reached a terminal state.
	CnstStateDone = "JOB_STATE_DONE"
	//CnstStateFailed indicates that the job has failed.  This is a terminal job state.  This state may only be set by the Cloud Dataflow service, and only as a transition from `JOB_STATE_RUNNING`.
	CnstStateFailed = "JOB_STATE_FAILED"
	//CnstStateCancelled indicates that the job has been explicitly cancelled. This is a terminal job state. This state may only be set via a Cloud Dataflow `UpdateJob` call, and only if the job has not yet reached another terminal state.
	CnstStateCancelled = "JOB_STATE_CANCELLED"
	//CnstStateUpdated indicates that the job was successfully updated, meaning that this job was stopped and another job was started, inheriting state from this one. This is a terminal job state. This state may only be set by the Cloud Dataflow service, and only as a transition from `JOB_STATE_RUNNING`.
	CnstStateUpdated = "JOB_STATE_UPDATED"
	//CnstStateDraining indicates that the job is in the process of draining. A draining job has stopped pulling from its input sources and is processing any data that remains in-flight. This state may be set via a Cloud Dataflow `UpdateJob` call, but only as a transition from `JOB_STATE_RUNNING`. Jobs that are draining may only transition to `JOB_STATE_DRAINED`,`JOB_STATE_CANCELLED`, or `JOB_STATE_FAILED`.
	CnstStateDraining = "JOB_STATE_DRAINING"
	//CnstStateDrained indicates that the job has been drained. A drained job terminated by stopping pulling from its input sources and processing any data that remained in-flight when draining was requested. This state is a terminal state, may only be set by the Cloud Dataflow service, and only as a transition from `JOB_STATE_DRAINING`.
	CnstStateDrained = "JOB_STATE_DRAINED"
	//CnstStatePending indicates that the job has been created but is not yet running.  Jobs that are pending may only transition to `JOB_STATE_RUNNING`, or `JOB_STATE_FAILED`.
	CnstStatePending = "JOB_STATE_PENDING"
	//CnstStateCancelling indicates that the job has been explicitly cancelled and is in the process of stopping.  Jobs that are cancelling may only transition to `JOB_STATE_CANCELLED` or `JOB_STATE_FAILED`.
	CnstStateCancelling = "JOB_STATE_CANCELLING"
	//CnstStateQueued indicates that the job has been created but is being delayed until launch. Jobs that are queued may only transition to `JOB_STATE_PENDING` or `JOB_STATE_CANCELLED`.
	CnstStateQueued = "JOB_STATE_QUEUED"
)
