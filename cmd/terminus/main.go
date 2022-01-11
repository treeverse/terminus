package main

import (
	"context"
	dbsql "database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"syscall"

	"github.com/treeverse/terminus/pkg/http"
	"github.com/treeverse/terminus/pkg/queue_handler"
	"github.com/treeverse/terminus/pkg/store/sql"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dustin/go-humanize"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func main() {
	Execute()
}

func DieOnErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func NewSQS() (*sqs.SQS, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, fmt.Errorf("New AWS session: %w", err)
	}
	sqs := sqs.New(sess)
	return sqs, nil
}

var rootCmd = &cobra.Command{
	Use:   "terminus",
	Short: "Terminus monitors and optionally controls quotas for lakeFS users on S3",
	Long: `Terminus is a service that listens to S3 events in order to track bytes used.
It may can track S3 resources used by lakeFS installations or users.`,
}

// GetFlagBytes returns the value of flag using humanized bytes.  E.g. "8K" -> 8192.
func GetFlagBytes(flags *pflag.FlagSet, flag string) (int64, error) {
	s, err := flags.GetString(flag)
	if err != nil {
		return 0, fmt.Errorf("get flag %s: %w", flag, err)
	}
	bytes, err := humanize.ParseBytes(s)
	if err != nil {
		return 0, fmt.Errorf("parse flag %s value %s: %w", flag, s)
	}
	if int64(bytes) < 0 {
		return int64(bytes), fmt.Errorf("Quota bytes %s too large", flag)
	}
	return int64(bytes), nil
}

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "Start the Terminus server",
	Example: "terminus run --sqs-name=terminus-queue --db-dsn=postgres:/// --default-quota=1G",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		dbDriver, err := cmd.Flags().GetString("db-driver")
		DieOnErr(err)
		dbDSN, err := cmd.Flags().GetString("db-dsn")
		DieOnErr(err)
		db, err := dbsql.Open(dbDriver, dbDSN)
		DieOnErr(err)
		err = db.PingContext(ctx)
		DieOnErr(err)

		fmt.Println("Open DB")
		defaultQuotaBytes, err := GetFlagBytes(cmd.Flags(), "default-quota")
		DieOnErr(err)
		store, err := sql.NewSQLStore(db, defaultQuotaBytes)
		DieOnErr(err)

		fmt.Println("Open SQS")
		sqs, err := NewSQS()
		DieOnErr(err)

		logger := log.Default()
		logger.SetPrefix("[terminus] ")
		queueName, err := cmd.Flags().GetString("sqs-name")
		DieOnErr(err)
		keyPattern, err := cmd.Flags().GetString("pattern")
		DieOnErr(err)
		keyRegexp, err := regexp.Compile(keyPattern)
		DieOnErr(err)
		keyReplacement, err := cmd.Flags().GetString("replacement")
		DieOnErr(err)

		pollCtx, _ := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

		server := &http.Server{Store: store}
		listenAddress, err := cmd.Flags().GetString("listen")
		DieOnErr(err)
		fmt.Printf("Starting webserver on %s...\n", listenAddress)
		server.Serve(ctx, listenAddress)

		fmt.Println("Starting to listen on queue...")
		queue_handler.Poll(pollCtx, logger, sqs, queueName, keyRegexp, keyReplacement, store)
		fmt.Println("Done!")
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().StringP("listen", "l", "localhost:80", "Address for webserver to listen")
	runCmd.Flags().StringP("sqs-name", "q", "", "Name of topic on SQS with S3 events to process")
	runCmd.MarkFlagRequired("sqs-name")

	runCmd.Flags().StringP("default-quota", "Q", "5KB", "Default quota size")

	runCmd.Flags().String("db-driver", "pgx", "Database driver code")
	runCmd.Flags().StringP("db-dsn", "d", "", "DSN to connect to database")
	runCmd.MarkFlagRequired("db-dsn")

	runCmd.Flags().StringP("pattern", "p", `^s3://[^/]*/user/([^/]*)/.*$`, "Regexp matching paths to track")
	runCmd.Flags().StringP("replacement", "r", `\1`, "Replacement on path matched by `--pattern' generating key for quota")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
