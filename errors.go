package dfmgr

import "errors"

//errors
var (
	//ErrNoDataFound occurs if a json result returns null
	ErrNoDataFound = errors.New("data was not found for this search")
)
