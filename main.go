// Copyright © 2020 - 2023 Weald Technology Trading.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"net/http"

	// #nosec G108
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	eth2client "github.com/attestantio/go-eth2-client"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
	zerologger "github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/wealdtech/chaind/handlers"
	standardbeaconcommittees "github.com/wealdtech/chaind/services/beaconcommittees/standard"
	"github.com/wealdtech/chaind/services/blocks"
	standardblocks "github.com/wealdtech/chaind/services/blocks/standard"
	"github.com/wealdtech/chaind/services/chaindb"
	postgresqlchaindb "github.com/wealdtech/chaind/services/chaindb/postgresql"
	"github.com/wealdtech/chaind/services/chaintime"
	standardchaintime "github.com/wealdtech/chaind/services/chaintime/standard"
	getlogseth1deposits "github.com/wealdtech/chaind/services/eth1deposits/getlogs"
	standardfinalizer "github.com/wealdtech/chaind/services/finalizer/standard"
	"github.com/wealdtech/chaind/services/metrics"
	nullmetrics "github.com/wealdtech/chaind/services/metrics/null"
	prometheusmetrics "github.com/wealdtech/chaind/services/metrics/prometheus"
	standardproposerduties "github.com/wealdtech/chaind/services/proposerduties/standard"
	standardscheduler "github.com/wealdtech/chaind/services/scheduler/standard"
	standardspec "github.com/wealdtech/chaind/services/spec/standard"
	"github.com/wealdtech/chaind/services/summarizer"
	standardsummarizer "github.com/wealdtech/chaind/services/summarizer/standard"
	standardsynccommittees "github.com/wealdtech/chaind/services/synccommittees/standard"
	standardvalidators "github.com/wealdtech/chaind/services/validators/standard"
	"github.com/wealdtech/chaind/util"
	"golang.org/x/sync/semaphore"
)

// ReleaseVersion is the release version for the code.
var ReleaseVersion = "0.7.0-rc1"

func main() {
	os.Exit(main2())
}

func main2() int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := fetchConfig(); err != nil {
		zerologger.Error().Err(err).Msg("Failed to fetch configuration")
		return 1
	}

	majordomo, err := util.InitMajordomo(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialise majordomo: %v\n", err)
		return 1
	}
	if err := initLogging(); err != nil {
		log.Error().Err(err).Msg("Failed to initialise logging")
		return 1
	}

	// runCommands will not return if a command is run.
	exit, err := runCommands(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Command returned error")
	}
	if exit {
		if err == nil {
			return 0
		}
		return 1
	}

	logModules()
	log.Info().Str("version", ReleaseVersion).Msg("Starting chaind")

	if err := initTracing(ctx, majordomo); err != nil {
		log.Error().Err(err).Msg("Failed to initialise tracing")
		return 1
	}

	initProfiling()

	runtime.GOMAXPROCS(runtime.NumCPU() * 8)

	log.Trace().Msg("Starting metrics service")
	monitor, err := startMonitor(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to start metrics service")
		return 1
	}
	if err := registerMetrics(ctx, monitor); err != nil {
		log.Error().Err(err).Msg("Failed to register metrics")
		return 1
	}
	setRelease(ctx, ReleaseVersion)
	setReady(ctx, false)

	if err := startServices(ctx, monitor); err != nil {
		log.Error().Err(err).Msg("Failed to initialise services")
		return 1
	}
	setReady(ctx, true)

	log.Info().Msg("All services operational")

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	for {
		sig := <-sigCh
		if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == os.Interrupt || sig == os.Kill {
			break
		}
	}

	log.Info().Msg("Stopping chaind")
	return 0
}

// fetchConfig fetches configuration from various sources.
func fetchConfig() error {
	pflag.String("base-dir", "", "base directory for configuration files")
	pflag.Bool("version", false, "show version and exit")
	pflag.String("log-level", "info", "minimum level of messsages to log")
	pflag.String("log-file", "", "redirect log output to a file")
	pflag.String("profile-address", "", "Address on which to run Go profile server")
	pflag.String("tracing-address", "", "Address to which to send tracing data")
	pflag.String("eth2client.address", "", "Address for beacon node")
	pflag.Duration("eth2client.timeout", 2*time.Minute, "Timeout for beacon node requests")
	pflag.Bool("blocks.enable", true, "Enable fetching of block-related information")
	pflag.Int32("blocks.start-slot", -1, "Slot from which to start fetching blocks")
	pflag.Bool("blocks.refetch", false, "Refetch all blocks even if they are already in the database")
	pflag.Bool("finalizer.enable", true, "Enable additional information on receipt of finality checkpoint")
	pflag.Bool("summarizer.enable", true, "Enable summary information")
	pflag.Bool("summarizer.epochs.enable", true, "Enable summary information for epochs")
	pflag.Bool("summarizer.blocks.enable", true, "Enable summary information for blocks")
	pflag.Bool("summarizer.validators.enable", false, "Enable summary information for validators (warning: creates a lot of data)")
	pflag.Uint64("summarizer.max-days-per-run", 28, "Maximum number of days' of data to summarize in a single run (when pruning)")
	pflag.Bool("validators.enable", true, "Enable fetching of validator-related information")
	pflag.Bool("validators.balances.enable", false, "Enable fetching of validator balances (warning: creates a lot of data)")
	pflag.Bool("beacon-committees.enable", true, "Enable fetching of beacon committee-related information")
	pflag.Bool("proposer-duties.enable", true, "Enable fetching of proposer duty-related information")
	pflag.Bool("sync-committees.enable", true, "Enable fetching of sync committee-related information")
	pflag.Int32("sync-committees.start-period", -1, "Period from which to start fetching sync committees")
	pflag.Bool("eth1deposits.enable", false, "Enable fetching of Ethereum 1 deposit information")
	pflag.String("eth1deposits.start-block", "", "Ethereum 1 block from which to start fetching deposits")
	pflag.String("eth1client.address", "", "Address for Ethereum 1 node")
	pflag.String("chaindb.url", "", "URL for database")
	pflag.Uint("chaindb.max-connections", 16, "maximum number of concurrent database connections")
	pflag.Parse()
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		return errors.Wrap(err, "failed to bind pflags to viper")
	}

	if viper.GetString("base-dir") != "" {
		// User-defined base directory.
		viper.AddConfigPath(resolvePath(""))
		viper.SetConfigName("chaind")
	} else {
		// Home directory.
		home, err := homedir.Dir()
		if err != nil {
			return errors.Wrap(err, "failed to obtain home directory")
		}
		viper.AddConfigPath(home)
		viper.SetConfigName(".chaind")
	}

	// Environment settings.
	viper.SetEnvPrefix("CHAIND")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		switch {
		case errors.As(err, &viper.ConfigFileNotFoundError{}):
			// It is allowable for chaind to not have a configuration file, but only if
			// we have the information from elsewhere (e.g. environment variables).  Check
			// to see if we have any beacon nodes configured, as if not we aren't going to
			// get very far anyway.
			if viper.GetString("eth2client.address") == "" {
				// Assume the underlying issue is that the configuration file is missing.
				return errors.Wrap(err, "could not find the configuration file")
			}
		case errors.As(err, &viper.ConfigParseError{}):
			return errors.Wrap(err, "could not parse the configuration file")
		default:
			return errors.Wrap(err, "failed to obtain configuration")
		}
	}

	return nil
}

// initProfiling initialises the profiling server.
func initProfiling() {
	profileAddress := viper.GetString("profile-address")
	if profileAddress != "" {
		go func() {
			log.Info().Str("profile_address", profileAddress).Msg("Starting profile server")
			server := &http.Server{
				Addr:              profileAddress,
				ReadHeaderTimeout: 5 * time.Second,
			}
			runtime.SetMutexProfileFraction(1)
			if err := server.ListenAndServe(); err != nil {
				log.Warn().Str("profile_address", profileAddress).Err(err).Msg("Failed to run profile server")
			}
		}()
	}
}

func startMonitor(ctx context.Context) (metrics.Service, error) {
	var monitor metrics.Service
	if viper.Get("metrics.prometheus.listen-address") != nil {
		var err error
		monitor, err = prometheusmetrics.New(ctx,
			prometheusmetrics.WithLogLevel(util.LogLevel("metrics.prometheus")),
			prometheusmetrics.WithAddress(viper.GetString("metrics.prometheus.listen-address")),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to start prometheus metrics service")
		}
		log.Info().Str("listen_address", viper.GetString("metrics.prometheus.listen-address")).Msg("Started prometheus metrics service")
	} else {
		log.Debug().Msg("No metrics service supplied; monitor not starting")
		monitor = &nullmetrics.Service{}
	}
	return monitor, nil
}

func startDatabase(ctx context.Context) (chaindb.Service, error) {
	log.Trace().Msg("Starting chain database service")
	chainDB, err := postgresqlchaindb.New(ctx,
		postgresqlchaindb.WithLogLevel(util.LogLevel("chaindb")),
		postgresqlchaindb.WithConnectionURL(viper.GetString("chaindb.url")),
		postgresqlchaindb.WithMaxConnections(viper.GetUint("chaindb.max-connections")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start chain database service")
	}
	return chainDB, err
}

func startServices(ctx context.Context, monitor metrics.Service) error {
	log.Trace().Msg("Checking for schema upgrades")
	chainDB, err := startDatabase(ctx)
	if err != nil {
		return err
	}

	if _, isUpgrader := chainDB.(*postgresqlchaindb.Service); isUpgrader {
		requiresRefetch, err := chainDB.(*postgresqlchaindb.Service).Upgrade(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to upgrade chain database")
		}
		if requiresRefetch {
			// The upgrade requires us to refetch blocks, so set up the options accordingly.
			// These will be picked up by the blocks service.
			viper.Set("blocks.start-slot", 0)
			viper.Set("blocks.refetch", true)
		}
	}

	log.Trace().Msg("Starting Ethereum 2 client service")
	eth2Client, err := fetchClient(ctx, viper.GetString("eth2client.address"))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("eth2client.address")))
	}
	if err != nil {
		return errors.Wrap(err, "failed to start Ethereum 2 client service")
	}

	log.Trace().Msg("Starting chain time service")
	chainTime, err := standardchaintime.New(ctx,
		standardchaintime.WithLogLevel(util.LogLevel("chaintime")),
		standardchaintime.WithGenesisTimeProvider(eth2Client.(eth2client.GenesisTimeProvider)),
		standardchaintime.WithSpecProvider(eth2Client.(eth2client.SpecProvider)),
		standardchaintime.WithForkScheduleProvider(eth2Client.(eth2client.ForkScheduleProvider)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to start chain time service")
	}

	// Wait for chainstart.
	specServiceStarted := false
	timeToGenesis := time.Until(chainTime.GenesisTime())
	if timeToGenesis > 0 {
		// See if we can obtain spec before the chain starts.  Not all beacon nodes support this,
		// so don't worry if it fails but do note it so that the service can be started later.
		log.Trace().Msg("Starting spec service (speculative pre-chain)")
		if err := startSpec(ctx, eth2Client, chainDB, monitor); err == nil {
			specServiceStarted = true
		}

		log.Info().Time("chain_start", chainTime.GenesisTime()).Msg("Waiting for chain start.")
		time.Sleep(timeToGenesis)
	}

	// Wait for the node to sync.
	for {
		syncState, err := eth2Client.(eth2client.NodeSyncingProvider).NodeSyncing(ctx)
		if err != nil {
			log.Debug().Err(err).Msg("Failed to obtain node sync state; will re-test in 1 minute")
			time.Sleep(time.Minute)
			continue
		}
		if syncState == nil {
			log.Debug().Msg("No node sync state; will re-test in 1 minute")
			time.Sleep(time.Minute)
			continue
		}
		if syncState.IsSyncing {
			log.Debug().Msg("Node syncing; will re-test in 1 minute")
			time.Sleep(time.Minute)
			continue
		}
		break
	}

	// Spec should be the first service that starts.  This adds configuration data to
	// chaindb so it is accessible to other services.
	if !specServiceStarted {
		log.Trace().Msg("Starting spec service")
		if err := startSpec(ctx, eth2Client, chainDB, monitor); err != nil {
			return errors.Wrap(err, "failed to start spec service")
		}
	}

	// Sync committees service is needed by blocks service.
	log.Trace().Msg("Starting sync committees service")
	if err := startSyncCommittees(ctx, eth2Client, chainDB, chainTime, monitor); err != nil {
		return errors.Wrap(err, "failed to start sync committees service")
	}

	// Shared activity semaphore for blocks and finalizer, to avoid potential deadlock.
	activitySem := semaphore.NewWeighted(1)

	log.Trace().Msg("Starting blocks service")
	blocks, err := startBlocks(ctx, eth2Client, chainDB, chainTime, monitor, activitySem)
	if err != nil {
		return errors.Wrap(err, "failed to start blocks service")
	}

	var summarizerSvc summarizer.Service
	if blocks != nil {
		log.Trace().Msg("Starting summarizer service")
		summarizerSvc, err = startSummarizer(ctx, eth2Client, chainDB, chainTime, monitor)
		if err != nil {
			return errors.Wrap(err, "failed to start summarizer service")
		}
	}

	log.Trace().Msg("Starting finalizer service")
	finalityHandlers := make([]handlers.FinalityHandler, 0)
	if summarizerSvc != nil {
		finalityHandlers = append(finalityHandlers, summarizerSvc.(handlers.FinalityHandler))
	}
	if err := startFinalizer(ctx, eth2Client, chainDB, chainTime, blocks, monitor, finalityHandlers, activitySem); err != nil {
		return errors.Wrap(err, "failed to start finalizer service")
	}

	log.Trace().Msg("Starting validators service")
	if err := startValidators(ctx, eth2Client, chainDB, chainTime, monitor); err != nil {
		return errors.Wrap(err, "failed to start validators service")
	}

	log.Trace().Msg("Starting beacon committees service")
	if err := startBeaconCommittees(ctx, eth2Client, chainDB, chainTime, monitor); err != nil {
		return errors.Wrap(err, "failed to start beacon committees service")
	}

	log.Trace().Msg("Starting proposer duties service")
	if err := startProposerDuties(ctx, eth2Client, chainDB, chainTime, monitor); err != nil {
		return errors.Wrap(err, "failed to start proposer duties service")
	}

	log.Trace().Msg("Starting Ethereum 1 deposits service")
	if err := startETH1Deposits(ctx, chainDB, monitor); err != nil {
		return errors.Wrap(err, "failed to start Ethereum 1 deposits service")
	}

	return nil
}

func logModules() {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		log.Trace().Str("path", buildInfo.Path).Msg("Main package")
		for _, dep := range buildInfo.Deps {
			log := log.Trace()
			if dep.Replace == nil {
				log = log.Str("path", dep.Path).Str("version", dep.Version)
			} else {
				log = log.Str("path", dep.Replace.Path).Str("version", dep.Replace.Version)
			}
			log.Msg("Dependency")
		}
	}
}

// resolvePath resolves a potentially relative path to an absolute path.
func resolvePath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	baseDir := viper.GetString("base-dir")
	if baseDir == "" {
		homeDir, err := homedir.Dir()
		if err != nil {
			log.Fatal().Err(err).Msg("Could not determine a home directory")
		}
		baseDir = homeDir
	}
	return filepath.Join(baseDir, path)
}

func startSpec(
	ctx context.Context,
	eth2Client eth2client.Service,
	chainDB chaindb.Service,
	monitor metrics.Service,
) error {
	var err error
	if viper.GetString("spec.address") != "" {
		eth2Client, err = fetchClient(ctx, viper.GetString("spec.address"))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("spec.address")))
		}
	}

	scheduler, err := standardscheduler.New(ctx,
		standardscheduler.WithLogLevel(util.LogLevel("scheduler")),
		standardscheduler.WithMonitor(monitor))
	if err != nil {
		return errors.Wrap(err, "failed to initialise scheduler")
	}

	_, err = standardspec.New(ctx,
		standardspec.WithLogLevel(util.LogLevel("spec")),
		standardspec.WithETH2Client(eth2Client),
		standardspec.WithChainDB(chainDB),
		standardspec.WithScheduler(scheduler),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create spec service")
	}

	return nil
}

func startBlocks(
	ctx context.Context,
	eth2Client eth2client.Service,
	chainDB chaindb.Service,
	chainTime chaintime.Service,
	monitor metrics.Service,
	activitySem *semaphore.Weighted,
) (
	blocks.Service,
	error,
) {
	if !viper.GetBool("blocks.enable") {
		return nil, nil
	}

	var err error
	if viper.GetString("blocks.address") != "" {
		eth2Client, err = fetchClient(ctx, viper.GetString("blocks.address"))
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("blocks.address")))
		}
	}

	s, err := standardblocks.New(ctx,
		standardblocks.WithLogLevel(util.LogLevel("blocks")),
		standardblocks.WithMonitor(monitor),
		standardblocks.WithETH2Client(eth2Client),
		standardblocks.WithChainTime(chainTime),
		standardblocks.WithChainDB(chainDB),
		standardblocks.WithStartSlot(viper.GetInt64("blocks.start-slot")),
		standardblocks.WithRefetch(viper.GetBool("blocks.refetch")),
		standardblocks.WithActivitySem(activitySem),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create blocks service")
	}

	return s, nil
}

func startFinalizer(
	ctx context.Context,
	eth2Client eth2client.Service,
	chainDB chaindb.Service,
	chainTime chaintime.Service,
	blocks blocks.Service,
	monitor metrics.Service,
	finalityHandlers []handlers.FinalityHandler,
	activitySem *semaphore.Weighted,
) error {
	if !viper.GetBool("finalizer.enable") {
		return nil
	}

	var err error
	if viper.GetString("finalizer.address") != "" {
		eth2Client, err = fetchClient(ctx, viper.GetString("finalizer.address"))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("finalizer.address")))
		}
	}

	_, err = standardfinalizer.New(ctx,
		standardfinalizer.WithLogLevel(util.LogLevel("finalizer")),
		standardfinalizer.WithMonitor(monitor),
		standardfinalizer.WithETH2Client(eth2Client),
		standardfinalizer.WithChainTime(chainTime),
		standardfinalizer.WithChainDB(chainDB),
		standardfinalizer.WithBlocks(blocks),
		standardfinalizer.WithFinalityHandlers(finalityHandlers),
		standardfinalizer.WithActivitySem(activitySem),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create finalizer service")
	}

	return nil
}

func startSummarizer(
	ctx context.Context,
	eth2Client eth2client.Service,
	chainDB chaindb.Service,
	chainTime chaintime.Service,
	monitor metrics.Service,
) (
	summarizer.Service,
	error,
) {
	if !viper.GetBool("summarizer.enable") {
		return nil, nil
	}

	standardSummarizer, err := standardsummarizer.New(ctx,
		standardsummarizer.WithLogLevel(util.LogLevel("summarizer")),
		standardsummarizer.WithMonitor(monitor),
		standardsummarizer.WithETH2Client(eth2Client),
		standardsummarizer.WithChainTime(chainTime),
		standardsummarizer.WithChainDB(chainDB),
		standardsummarizer.WithEpochSummaries(viper.GetBool("summarizer.epochs.enable")),
		standardsummarizer.WithBlockSummaries(viper.GetBool("summarizer.blocks.enable")),
		standardsummarizer.WithValidatorSummaries(viper.GetBool("summarizer.validators.enable")),
		standardsummarizer.WithMaxDaysPerRun(viper.GetUint64("summarizer.max-days-per-run")),
		standardsummarizer.WithValidatorEpochRetention(viper.GetString("summarizer.validators.epoch-retention")),
		standardsummarizer.WithValidatorBalanceRetention(viper.GetString("summarizer.validators.balance-retention")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create summarizer service")
	}

	return standardSummarizer, nil
}

func startValidators(
	ctx context.Context,
	eth2Client eth2client.Service,
	chainDB chaindb.Service,
	chainTime chaintime.Service,
	monitor metrics.Service,
) error {
	if !viper.GetBool("validators.enable") {
		return nil
	}

	var err error
	if viper.GetString("validators.address") != "" {
		eth2Client, err = fetchClient(ctx, viper.GetString("validators.address"))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("validators.address")))
		}
	}

	_, err = standardvalidators.New(ctx,
		standardvalidators.WithLogLevel(util.LogLevel("validators")),
		standardvalidators.WithMonitor(monitor),
		standardvalidators.WithETH2Client(eth2Client),
		standardvalidators.WithChainTime(chainTime),
		standardvalidators.WithChainDB(chainDB),
		standardvalidators.WithBalances(viper.GetBool("validators.balances.enable")),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create validators service")
	}

	return nil
}

func startBeaconCommittees(
	ctx context.Context,
	eth2Client eth2client.Service,
	chainDB chaindb.Service,
	chainTime chaintime.Service,
	monitor metrics.Service,
) error {
	if !viper.GetBool("beacon-committees.enable") {
		return nil
	}

	var err error
	if viper.GetString("beacon-committees.address") != "" {
		eth2Client, err = fetchClient(ctx, viper.GetString("beacon-committees.address"))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("beacon-committees.address")))
		}
	}

	_, err = standardbeaconcommittees.New(ctx,
		standardbeaconcommittees.WithLogLevel(util.LogLevel("beacon-committees")),
		standardbeaconcommittees.WithMonitor(monitor),
		standardbeaconcommittees.WithETH2Client(eth2Client),
		standardbeaconcommittees.WithChainTime(chainTime),
		standardbeaconcommittees.WithChainDB(chainDB),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create beacon committees service")
	}

	return nil
}

func startProposerDuties(
	ctx context.Context,
	eth2Client eth2client.Service,
	chainDB chaindb.Service,
	chainTime chaintime.Service,
	monitor metrics.Service,
) error {
	if !viper.GetBool("proposer-duties.enable") {
		return nil
	}

	var err error
	if viper.GetString("proposer-duties.address") != "" {
		eth2Client, err = fetchClient(ctx, viper.GetString("proposer-duties.address"))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("proposer-duties.address")))
		}
	}

	_, err = standardproposerduties.New(ctx,
		standardproposerduties.WithLogLevel(util.LogLevel("proposer-duties")),
		standardproposerduties.WithMonitor(monitor),
		standardproposerduties.WithETH2Client(eth2Client),
		standardproposerduties.WithChainTime(chainTime),
		standardproposerduties.WithChainDB(chainDB),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create proposer duties service")
	}

	return nil
}

func startETH1Deposits(
	ctx context.Context,
	chainDB chaindb.Service,
	monitor metrics.Service,
) error {
	if !viper.GetBool("eth1deposits.enable") {
		return nil
	}

	log.Trace().Msg("Starting Ethereum 1 deposits service")
	_, err := getlogseth1deposits.New(ctx,
		getlogseth1deposits.WithLogLevel(util.LogLevel("eth1deposits.log-level")),
		getlogseth1deposits.WithMonitor(monitor),
		getlogseth1deposits.WithChainDB(chainDB),
		getlogseth1deposits.WithConnectionURL(viper.GetString("eth1client.address")),
		getlogseth1deposits.WithStartBlock(viper.GetString("eth1deposits.start-block")),
		getlogseth1deposits.WithETH1DepositsSetter(chainDB.(chaindb.ETH1DepositsSetter)),
		getlogseth1deposits.WithETH1Confirmations(viper.GetUint64("eth1deposits.confirmations")),
	)
	if err != nil {
		return errors.Wrap(err, "failed to start Ethereum 1 deposits service")
	}

	return nil
}

func startSyncCommittees(
	ctx context.Context,
	eth2Client eth2client.Service,
	chainDB chaindb.Service,
	chainTime chaintime.Service,
	monitor metrics.Service,
) error {
	if !viper.GetBool("sync-committees.enable") {
		return nil
	}

	var err error
	if viper.GetString("sync-committees.address") != "" {
		eth2Client, err = fetchClient(ctx, viper.GetString("sync-committees.address"))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("sync-committees.address")))
		}
	}

	_, err = standardsynccommittees.New(ctx,
		standardsynccommittees.WithLogLevel(util.LogLevel("sync-committees")),
		standardsynccommittees.WithMonitor(monitor),
		standardsynccommittees.WithETH2Client(eth2Client),
		standardsynccommittees.WithChainTime(chainTime),
		standardsynccommittees.WithChainDB(chainDB),
		standardsynccommittees.WithSpecProvider(chainDB.(eth2client.SpecProvider)),
		standardsynccommittees.WithStartPeriod(viper.GetInt64("sync-committees.start-period")),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create sync committees service")
	}

	return nil
}

// runCommands runs commands if required.
// Returns true if an exit is required.
//
//nolint:unparam
func runCommands(_ context.Context) (bool, error) {
	if viper.GetBool("version") {
		fmt.Printf("%s\n", ReleaseVersion)
		return true, nil
	}

	return false, nil
}
