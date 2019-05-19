package dfmgr

import (
	"log"
	"os"
	"strconv"

	cfg "github.com/lidstromberg/config"

	"golang.org/x/net/context"
)

var (
	//EnvDebugOn controls verbose logging
	EnvDebugOn bool
)

//preflight checks that the incoming configuration map contains the required config elements
func preflight(ctx context.Context, bc cfg.ConfigSetting) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)
	log.Println("Started DfMgr preflight..")

	cfm1 := preflightConfigLoader()
	bc.LoadConfigMap(ctx, cfm1)

	if bc.GetConfigValue(ctx, "EnvDebugOn") == "" {
		log.Fatal("Could not parse environment variable EnvDebugOn")
	}

	if bc.GetConfigValue(ctx, "EnvDfGcpProject") == "" {
		log.Fatal("Could not parse environment variable EnvDfGcpProject")
	}

	if bc.GetConfigValue(ctx, "EnvDfGcpRegion") == "" {
		log.Fatal("Could not parse environment variable EnvDfGcpRegion")
	}

	if bc.GetConfigValue(ctx, "EnvDfParamsBucket") == "" {
		log.Fatal("Could not parse environment variable EnvDfParamsBucket")
	}

	if bc.GetConfigValue(ctx, "EnvSqlConnection") == "" {
		log.Fatal("Could not parse environment variable EnvSqlConnection")
	}

	if bc.GetConfigValue(ctx, "EnvSqlDst") == "" {
		log.Fatal("Could not parse environment variable EnvSqlDst")
	}

	//set the debug value
	constlog, err := strconv.ParseBool(bc.GetConfigValue(ctx, "EnvDebugOn"))

	if err != nil {
		log.Fatal("Could not parse environment variable EnvDebugOn")
	}

	EnvDebugOn = constlog

	log.Println("..Finished DfMgr preflight.")
}

//preflightConfigLoader loads the session config vars
func preflightConfigLoader() map[string]string {
	cfm := make(map[string]string)

	/**********************************************************************
	* DATAFLOW ENV SETTINGS
	**********************************************************************/
	//EnvDebugOn is the debug setting
	cfm["EnvDebugOn"] = os.Getenv("DF_DEBUGON")
	//EnvDfGcpProject is the project setting
	cfm["EnvDfGcpProject"] = os.Getenv("DF_GCP_PROJECT")
	//EnvDfParamsBucket is the GCS bucket storing the parameter files
	cfm["EnvDfParamsBucket"] = os.Getenv("DF_BUCKET")
	//EnvDfGcpRegion is the region setting
	cfm["EnvDfGcpRegion"] = os.Getenv("DF_GCP_REGION")

	if cfm["EnvDebugOn"] == "" {
		log.Fatal("Could not parse environment variable EnvDebugOn")
	}

	if cfm["EnvDfGcpProject"] == "" {
		log.Fatal("Could not parse environment variable EnvDfGcpProject")
	}

	if cfm["EnvDfGcpRegion"] == "" {
		log.Fatal("Could not parse environment variable EnvDfGcpRegion")
	}

	if cfm["EnvDfParamsBucket"] == "" {
		log.Fatal("Could not parse environment variablex EnvDfParamsBucket")
	}

	/**********************************************************************
	* SQL ENV SETTINGS
	**********************************************************************/
	//EnvSqlDst is the client poolsize
	cfm["EnvSqlDst"] = os.Getenv("DF_SQLDST")
	//EnvSqlConnection is the datastore namespace
	cfm["EnvSqlConnection"] = os.Getenv("DF_SQLCNX")

	if cfm["EnvSqlConnection"] == "" {
		log.Fatal("Could not parse environment variable EnvSqlConnection")
	}

	if cfm["EnvSqlDst"] == "" {
		log.Fatal("Could not parse environment variablex EnvSqlDst")
	}

	return cfm
}
