# Dataflow-jobcontrol

This is a package for starting, monitoring and stopping GCP dataflow jobs. It uses a CloudSQL Postgres database to retain a list of jobs for progress tracking.


### Main Files

  
| File | Purpose |
| ------ | ------ |
| schema/ | Postgres db creation scripts |
| dfmgr.go | Logic manager |
| dfmgr_test.go | Tests |
| pgmgr.go | Postgres logic manager |
| pgmgr_test.go | Tests |
| pgdatamgr.go | Data repo interface |

  

### Ancillary Files

| File | Purpose |
| ------ | ------ |
| config.go | Boot package parameters, environment var collection |
| const.go | Package constants |
| entity.go | Package structs || errors.go | Package error definitions |
| env | Package environment variables for local/dev installation |
| gogets | Statements for go-getting required packages |