package main

import (
	"log"
	"os"
	"strconv"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {

	// basic configuration
	InQueueName        string // SQS queue name for inbound documents
	PollTimeOut        int64  // the SQS queue timeout (in seconds)
	LocalWorkDir       string // the local work directory
	WorkerQueueSize    int    // the inbound message queue size to feed the workers
	Workers            int    // the number of worker processes
	DeleteAfterProcess bool   // delete the bucket object after processing
	FailOnOverwrite    bool   // fail if the converted file will overwrite an existing one

	// image splitting support
	SplitBinary  string // the file split binary
	SplitSuffix  string // the suffix of split files
	SplitOptions string // the split options

	// image conversion support
	ConvertBinary  string // the conversion binary
	ConvertSuffix  string // the suffix of converted files
	ConvertOptions string // the conversion options

	// output location support
	ImageOutputRoot    string // the converted image output directory
	PartitionOutputDir bool   // do we 'partition' output directory by id (ab/cd/ef/file(s)...) or not (abcdef/file(s)...)

	// iiif image manifest support
	ManifestMetadataEndpoint string // the endpoint to use for manifest metadata
	ManifestTemplateName     string // the name of the template for the manifest
	ManifestOutputDir        string // the manifest output directory
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

	// basic configuration
	cfg.InQueueName = ensureSetAndNonEmpty("IIIF_INGEST_IN_QUEUE")
	cfg.PollTimeOut = int64(envToInt("IIIF_INGEST_QUEUE_POLL_TIMEOUT"))
	cfg.LocalWorkDir = ensureSetAndNonEmpty("IIIF_INGEST_WORK_DIR")
	cfg.WorkerQueueSize = envToInt("IIIF_INGEST_WORK_QUEUE_SIZE")
	cfg.Workers = envToInt("IIIF_INGEST_WORKERS")
	cfg.DeleteAfterProcess = envToBoolean("IIIF_INGEST_DELETE_AFTER_PROCESS")
	cfg.FailOnOverwrite = envToBoolean("IIIF_INGEST_FAIL_ON_OVERWRITE")

	// image splitting support
	cfg.SplitBinary = envWithDefault("IIIF_INGEST_SPLIT_BIN", "")
	cfg.SplitSuffix = envWithDefault("IIIF_INGEST_SPLIT_SUFFIX", "")
	cfg.SplitOptions = envWithDefault("IIIF_INGEST_SPLIT_OPTS", "")

	// image conversion support
	cfg.ConvertBinary = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_BIN")
	cfg.ConvertSuffix = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_SUFFIX")
	cfg.ConvertOptions = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_OPTS")

	// output location support
	cfg.ImageOutputRoot = ensureSetAndNonEmpty("IIIF_INGEST_IMAGE_OUTPUT_ROOT")
	cfg.PartitionOutputDir = envToBoolean("IIIF_INGEST_PARTITION_OUTPUT_DIR")

	// iiif image manifest support
	cfg.ManifestMetadataEndpoint = envWithDefault("IIIF_INGEST_METADATA_ENDPOINT", "")
	cfg.ManifestTemplateName = envWithDefault("IIIF_INGEST_MANIFEST_TEMPLATE", "")
	cfg.ManifestOutputDir = envWithDefault("IIIF_INGEST_MANIFEST_OUTPUT_DIR", "")

	// basic configuration
	log.Printf("[CONFIG] InQueueName              = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] PollTimeOut              = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] LocalWorkDir             = [%s]", cfg.LocalWorkDir)
	log.Printf("[CONFIG] WorkerQueueSize          = [%d]", cfg.WorkerQueueSize)
	log.Printf("[CONFIG] Workers                  = [%d]", cfg.Workers)
	log.Printf("[CONFIG] DeleteAfterProcess       = [%t]", cfg.DeleteAfterProcess)
	log.Printf("[CONFIG] FailOnOverwrite          = [%t]", cfg.FailOnOverwrite)

	// image splitting support
	log.Printf("[CONFIG] SplitBinary              = [%s]", cfg.SplitBinary)
	log.Printf("[CONFIG] SplitSuffix              = [%s]", cfg.SplitSuffix)
	log.Printf("[CONFIG] SplitOptions             = [%s]", cfg.SplitOptions)

	// image conversion support
	log.Printf("[CONFIG] ConvertBinary            = [%s]", cfg.ConvertBinary)
	log.Printf("[CONFIG] ConvertSuffix            = [%s]", cfg.ConvertSuffix)
	log.Printf("[CONFIG] ConvertOptions           = [%s]", cfg.ConvertOptions)

	// output location support
	log.Printf("[CONFIG] ImageOutputRoot          = [%s]", cfg.ImageOutputRoot)
	log.Printf("[CONFIG] PartitionOutputDir       = [%t]", cfg.PartitionOutputDir)

	// iiif image manifest support
	log.Printf("[CONFIG] ManifestMetadataEndpoint = [%s]", cfg.ManifestMetadataEndpoint)
	log.Printf("[CONFIG] ManifestTemplateName     = [%s]", cfg.ManifestTemplateName)
	log.Printf("[CONFIG] ManifestOutputDir        = [%s]", cfg.ManifestOutputDir)

	// validate the config if we have splitting behavior
	if len(cfg.SplitBinary) != 0 {
		if len(cfg.SplitSuffix) == 0 || len(cfg.SplitOptions) == 0 {
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
	if len(cfg.ManifestMetadataEndpoint) != 0 {
		if len(cfg.ManifestTemplateName) == 0 || len(cfg.ManifestOutputDir) == 0 {
			log.Printf("[main] ERROR: manifest configuration incomplete")
			os.Exit(1)
		}
	}
	return &cfg
}