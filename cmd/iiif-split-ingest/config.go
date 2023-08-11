package main

import (
	"log"
	"os"
	"strconv"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {

	// service configuration
	InQueueName     string // SQS queue name for inbound documents
	PollTimeOut     int64  // the SQS queue timeout (in seconds)
	LocalWorkDir    string // the local work directory
	WorkerQueueSize int    // the inbound message queue size to feed the workers
	Workers         int    // the number of worker processes

	// splitting configuration
	SplitBinary              string // the file split binary
	SplitSuffix              string // the suffix of split files
	SplitCommandLine         string // the split commandline
	SplitCommandInFileToken  string // the placeholder token for the input file
	SplitCommandOutFileToken string // the placeholder token for the output file

	// conversion configuration
	ConvertBinary      string // the conversion binary
	ConvertSuffix      string // the suffix of converted files
	ConvertCommandLine string // the conversion commandline
	DeleteSource       bool   // delete the bucket object after processing

	// output location support
	OutputFSRoot       string // the converted image output directory
	OutputBucket       string // the output bucket
	OutputBucketRoot   string // the output bucket root
	PartitionOutputDir bool   // do we 'partition' output directory by id (ab/cd/ef/file(s)...) or not (abcdef/file(s)...)

	// iiif image manifest support
	ManifestTemplateName string // the name of the template for the manifest
	IIIFServiceRoot      string // the root URL for the appropriate iiif server
	IdPlaceHolder        string // the placeholder token for the ID
	ManifestOutputName   string // the manifest output name template
	ManifestOutputDir    string // the manifest output directory

	// metadata support
	ManifestMetadataQueryEndpoint string // the endpoint to use for the metadata query
	ManifestMetadataAuthEndpoint  string // the endpoint to use for query authorization
	ManifestMetadataQueryTemplate string // the template to use for the metadata query
	ManifestMetadataQueryTimeout  int    // the metadata query timeout (in seconds)

	// static metadata support
	ManifestMetadataCopyrightText string // static text for the copyright field
}

func envWithDefault(env string, defaultValue string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("[main] INFO: environment variable not set: [%s] using default value [%s]", env, defaultValue)
		return defaultValue
	}

	return val
}

func ensureSet(env string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("[main] ERROR: environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func ensureSetAndNonEmpty(env string) string {
	val := ensureSet(env)

	if val == "" {
		log.Printf("[main] ERROR: environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func envToInt(env string) int {

	number := ensureSetAndNonEmpty(env)
	n, err := strconv.Atoi(number)
	fatalIfError(err)
	return n
}

func envToBoolean(env string) bool {

	value := ensureSetAndNonEmpty(env)
	b, err := strconv.ParseBool(value)
	fatalIfError(err)
	return b
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	// service configuration
	cfg.InQueueName = ensureSetAndNonEmpty("IIIF_INGEST_IN_QUEUE")
	cfg.PollTimeOut = int64(envToInt("IIIF_INGEST_QUEUE_POLL_TIMEOUT"))
	cfg.LocalWorkDir = ensureSetAndNonEmpty("IIIF_INGEST_WORK_DIR")
	cfg.WorkerQueueSize = envToInt("IIIF_INGEST_WORK_QUEUE_SIZE")
	cfg.Workers = envToInt("IIIF_INGEST_WORKERS")

	// splitting configuration
	cfg.SplitBinary = envWithDefault("IIIF_INGEST_SPLIT_BIN", "")
	cfg.SplitSuffix = envWithDefault("IIIF_INGEST_SPLIT_SUFFIX", "")
	cfg.SplitCommandLine = envWithDefault("IIIF_INGEST_SPLIT_CMD", "")
	cfg.SplitCommandInFileToken = ensureSetAndNonEmpty("IIIF_INGEST_SPLIT_CMD_INFILE_TOKEN")
	cfg.SplitCommandOutFileToken = ensureSetAndNonEmpty("IIIF_INGEST_SPLIT_CMD_OUTFILE_TOKEN")

	// conversion configuration
	cfg.ConvertBinary = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_BIN")
	cfg.ConvertSuffix = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_SUFFIX")
	cfg.ConvertCommandLine = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_CMD")
	cfg.DeleteSource = envToBoolean("IIIF_INGEST_DELETE_SOURCE")

	// output configuration
	cfg.OutputFSRoot = envWithDefault("IIIF_INGEST_OUTPUT_FS_ROOT", "")
	cfg.OutputBucket = envWithDefault("IIIF_INGEST_OUTPUT_BUCKET", "")
	cfg.OutputBucketRoot = envWithDefault("IIIF_INGEST_OUTPUT_BUCKET_ROOT", "")
	cfg.PartitionOutputDir = envToBoolean("IIIF_INGEST_PARTITION_OUTPUT_DIR")

	// iiif image manifest support
	cfg.ManifestTemplateName = envWithDefault("IIIF_INGEST_MANIFEST_TEMPLATE", "")
	cfg.IIIFServiceRoot = envWithDefault("IIIF_SERVICE_URL", "")
	cfg.IdPlaceHolder = envWithDefault("IIIF_INGEST_ID_PLACEHOLDER", "")
	cfg.ManifestOutputName = envWithDefault("IIIF_INGEST_MANIFEST_OUTPUT_NAME", "")
	cfg.ManifestOutputDir = envWithDefault("IIIF_INGEST_MANIFEST_OUTPUT_DIR", "")

	// metadata support
	cfg.ManifestMetadataQueryEndpoint = envWithDefault("IIIF_INGEST_METADATA_QUERY_ENDPOINT", "")
	cfg.ManifestMetadataAuthEndpoint = envWithDefault("IIIF_INGEST_METADATA_AUTH_ENDPOINT", "")
	cfg.ManifestMetadataQueryTemplate = envWithDefault("IIIF_INGEST_METADATA_QUERY_TEMPLATE", "")
	cfg.ManifestMetadataQueryTimeout, _ = strconv.Atoi(envWithDefault("IIIF_INGEST_METADATA_QUERY_TIMEOUT", "30"))

	// static metadata support
	cfg.ManifestMetadataCopyrightText = envWithDefault("IIIF_INGEST_METADATA_COPYRIGHT_NOTE", "")

	// service configuration
	log.Printf("[CONFIG] InQueueName                   = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] PollTimeOut                   = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] LocalWorkDir                  = [%s]", cfg.LocalWorkDir)
	log.Printf("[CONFIG] WorkerQueueSize               = [%d]", cfg.WorkerQueueSize)
	log.Printf("[CONFIG] Workers                       = [%d]", cfg.Workers)

	// splitting configuration
	log.Printf("[CONFIG] SplitBinary                   = [%s]", cfg.SplitBinary)
	log.Printf("[CONFIG] SplitSuffix                   = [%s]", cfg.SplitSuffix)
	log.Printf("[CONFIG] SplitCommandLine              = [%s]", cfg.SplitCommandLine)
	log.Printf("[CONFIG] SplitCommandInFileToken       = [%s]", cfg.SplitCommandInFileToken)
	log.Printf("[CONFIG] SplitCommandOutFileToken      = [%s]", cfg.SplitCommandOutFileToken)

	// conversion configuration
	log.Printf("[CONFIG] ConvertBinary                 = [%s]", cfg.ConvertBinary)
	log.Printf("[CONFIG] ConvertSuffix                 = [%s]", cfg.ConvertSuffix)
	log.Printf("[CONFIG] ConvertCommandLine            = [%s]", cfg.ConvertCommandLine)
	log.Printf("[CONFIG] DeleteSource                  = [%t]", cfg.DeleteSource)

	// output location support
	log.Printf("[CONFIG] OutputFSRoot                  = [%s]", cfg.OutputFSRoot)
	log.Printf("[CONFIG] OutputBucket                  = [%s]", cfg.OutputBucket)
	log.Printf("[CONFIG] OutputBucketRoot              = [%s]", cfg.OutputBucketRoot)
	log.Printf("[CONFIG] PartitionOutputDir            = [%t]", cfg.PartitionOutputDir)

	// iiif image manifest support
	log.Printf("[CONFIG] ManifestTemplateName          = [%s]", cfg.ManifestTemplateName)
	log.Printf("[CONFIG] IdPlaceHolder                 = [%s]", cfg.IdPlaceHolder)
	log.Printf("[CONFIG] ManifestOutputName            = [%s]", cfg.ManifestOutputName)
	log.Printf("[CONFIG] ManifestOutputDir             = [%s]", cfg.ManifestOutputDir)

	// metadata support
	log.Printf("[CONFIG] ManifestMetadataQueryEndpoint = [%s]", cfg.ManifestMetadataQueryEndpoint)
	log.Printf("[CONFIG] ManifestMetadataAuthEndpoint  = [%s]", cfg.ManifestMetadataAuthEndpoint)
	log.Printf("[CONFIG] ManifestMetadataQueryTemplate = [%s]", cfg.ManifestMetadataQueryTemplate)
	log.Printf("[CONFIG] ManifestMetadataQueryTimeout  = [%d]", cfg.ManifestMetadataQueryTimeout)

	// static metadata support
	log.Printf("[CONFIG] ManifestMetadataCopyrightText = [%s]", cfg.ManifestMetadataCopyrightText)

	// validate output target values
	if len(cfg.OutputFSRoot) == 0 && len(cfg.OutputBucket) == 0 {
		log.Printf("[main] ERROR: must specify output root (IIIF_INGEST_OUTPUT_ROOT) or output bucket (IIIF_INGEST_OUTPUT_BUCKET)")
		os.Exit(1)
	}

	if len(cfg.OutputFSRoot) != 0 && len(cfg.OutputBucket) != 0 {
		log.Printf("[main] ERROR: cannot specify output root (IIIF_INGEST_OUTPUT_ROOT) and output bucket (IIIF_INGEST_OUTPUT_BUCKET)")
		os.Exit(1)
	}

	// validate the config if we have splitting behavior
	if len(cfg.SplitBinary) != 0 {
		if len(cfg.SplitSuffix) == 0 || len(cfg.SplitCommandLine) == 0 ||
			len(cfg.SplitCommandInFileToken) == 0 || len(cfg.SplitCommandOutFileToken) == 0 {
			log.Printf("[main] ERROR: split configuration incomplete")
			os.Exit(1)
		}

		// we dont partition split files
		if cfg.PartitionOutputDir == true {
			log.Printf("[main] ERROR: cannot partition split files")
			os.Exit(1)
		}
	}

	// validate the config if we have manifest behavior
	if len(cfg.ManifestTemplateName) != 0 {
		if len(cfg.IIIFServiceRoot) == 0 ||
			len(cfg.IdPlaceHolder) == 0 || len(cfg.ManifestOutputName) == 0 || len(cfg.ManifestOutputDir) == 0 {
			log.Printf("[main] ERROR: manifest configuration incomplete")
			os.Exit(1)
		}

		// verify the metadata configuration is good
		if len(cfg.ManifestMetadataQueryEndpoint) != 0 {
			if len(cfg.ManifestMetadataAuthEndpoint) == 0 || len(cfg.ManifestMetadataQueryTemplate) == 0 {
				log.Printf("[main] ERROR: metadata configuration incomplete")
				os.Exit(1)
			}
		}

		// verify the manifest template exists
		if fileExists(cfg.ManifestTemplateName) == false {
			log.Printf("[main] ERROR: manifest template [%s] does not exist", cfg.ManifestTemplateName)
			os.Exit(1)
		}

	}
	return &cfg
}
