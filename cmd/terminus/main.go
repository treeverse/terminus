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

	"github.com/treeverse/terminus/pkg/queue_handler"
	"github.com/treeverse/terminus/pkg/store/sql"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/spf13/cobra"
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

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "Start the Terminus server",
	Example: "terminus run --sqs-name=terminus-queue --db-dsn=postgres:///",
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
		store, err := sql.NewSQLStore(db)
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

		fmt.Println("Starting to listen...")
		queue_handler.Poll(pollCtx, logger, sqs, queueName, keyRegexp, keyReplacement, store)
		fmt.Println("Done!")
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().StringP("sqs-name", "q", "", "Name of topic on SQS with S3 events to process")
	runCmd.MarkFlagRequired("sqs-name")

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
