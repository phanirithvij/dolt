// Copyright 2019 Dolthub, Inc.
//
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

package commands

import (
	"context"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
)

type remoteInfo struct {
	Name       string
	Url        string
	FetchSpecs types.JSONDocument
	Params     types.JSONDocument
}

var pushDocs = cli.CommandDocumentationContent{
	ShortDesc: "Update remote refs along with associated objects",
	LongDesc: `Updates remote refs using local refs, while sending objects necessary to complete the given refs.

When the command line does not specify where to push with the {{.LessThan}}remote{{.GreaterThan}} argument, an attempt is made to infer the remote.  If only one remote exists it will be used, if multiple remotes exists, a remote named 'origin' will be attempted.  If there is more than one remote, and none of them are named 'origin' then the command will fail and you will need to specify the correct remote explicitly.

When the command line does not specify what to push with {{.LessThan}}refspec{{.GreaterThan}}... then the current branch will be used.

When neither the command-line does not specify what to push, the default behavior is used, which corresponds to the current branch being pushed to the corresponding upstream branch, but as a safety measure, the push is aborted if the upstream branch does not have the same name as the local one.
`,

	Synopsis: []string{
		"[-u | --set-upstream] [{{.LessThan}}remote{{.GreaterThan}}] [{{.LessThan}}refspec{{.GreaterThan}}]",
	},
}

type PushCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd PushCmd) Name() string {
	return "push"
}

// Description returns a description of the command
func (cmd PushCmd) Description() string {
	return "Push to a dolt remote."
}

func (cmd PushCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(pushDocs, ap)
}

func (cmd PushCmd) ArgParser() *argparser.ArgParser {
	return cli.CreatePushArgParser()
}

// EventType returns the type of the event to log
func (cmd PushCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PUSH
}

// Exec executes the command
func (cmd PushCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, pushDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return handleStatusVErr(err)
	}
	if closeFunc != nil {
		defer closeFunc()
	}
	err = push(queryist, sqlCtx, apr)
	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
}

func push(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults) error {
	force := apr.Contains(cli.ForceFlag)
	setUpstream := apr.Contains(cli.SetUpstreamFlag)

	remoteName := "origin"
	remotes, err := getRemotes(queryist, sqlCtx)
	if err != nil {
		return fmt.Errorf("failed to get remotes: %w", err)
	}

	args := apr.Args
	if len(args) == 1 {
		if _, ok := remotes[args[0]]; ok {
			remoteName = args[0]
			args = []string{}
		}
	}
	_, remoteOK := remotes[remoteName]
	var refSpec string
	if remoteOK && len(args) == 1 {
		refSpec = args[0]
	} else if len(args) == 2 {
		remoteName = args[0]
		refSpec = args[1]
	}

	params := []interface{}{}

	sb := strings.Builder{}
	sb.WriteString("call dolt_push(")
	if force {
		sb.WriteString("'--force', ")
	}
	if setUpstream {
		sb.WriteString("'--set-upstream', ")
	}
	sb.WriteString("?")
	params = append(params, remoteName)
	if len(refSpec) > 0 {
		sb.WriteString(", ?")
		params = append(params, refSpec)
	}
	sb.WriteString(");")
	query := sb.String()

	rows, err := InterpolateAndRunQuery(queryist, sqlCtx, query, params...)
	cli.Printf("pavel >>> rows: %v\n", rows)
	if err != nil {
		cli.Printf("pavel >>> error: %v\n", err)
		return err
	}
	return err
}

func printInfoForPushError(err error, remote env.Remote, destRef, remoteRef ref.DoltRef) errhand.VerboseError {
	switch err {
	case doltdb.ErrUpToDate:
		cli.Println("Everything up-to-date")
	case doltdb.ErrIsAhead, actions.ErrCantFF, datas.ErrMergeNeeded:
		cli.Printf("To %s\n", remote.Url)
		cli.Printf("! [rejected]          %s -> %s (non-fast-forward)\n", destRef.String(), remoteRef.String())
		cli.Printf("error: failed to push some refs to '%s'\n", remote.Url)
		cli.Println("hint: Updates were rejected because the tip of your current branch is behind")
		cli.Println("hint: its remote counterpart. Integrate the remote changes (e.g.")
		cli.Println("hint: 'dolt pull ...') before pushing again.")
		return errhand.BuildDError("").Build()
	case actions.ErrUnknownPushErr:
		status, ok := status.FromError(err)
		if ok && status.Code() == codes.PermissionDenied {
			cli.Println("hint: have you logged into DoltHub using 'dolt login'?")
			cli.Println("hint: check that user.email in 'dolt config --list' has write perms to DoltHub repo")
		}
		if rpcErr, ok := err.(*remotestorage.RpcError); ok {
			return errhand.BuildDError("error: push failed").AddCause(err).AddDetails(rpcErr.FullDetails()).Build()
		} else {
			return errhand.BuildDError("error: push failed").AddCause(err).Build()
		}
	default:
		return errhand.BuildDError("error: push failed").AddCause(err).Build()
	}
	return nil
}

func pullerProgFunc(ctx context.Context, statsCh chan pull.Stats, language progLanguage) {
	p := cli.NewEphemeralPrinter()

	for {
		select {
		case <-ctx.Done():
			return
		case stats, ok := <-statsCh:
			if !ok {
				return
			}
			if language == downloadLanguage {
				p.Printf("Downloaded %s chunks, %s @ %s/s.",
					humanize.Comma(int64(stats.FetchedSourceChunks)),
					humanize.Bytes(stats.FetchedSourceBytes),
					humanize.SIWithDigits(stats.FetchedSourceBytesPerSec, 2, "B"),
				)
			} else {
				p.Printf("Uploaded %s of %s @ %s/s.",
					humanize.Bytes(stats.FinishedSendBytes),
					humanize.Bytes(stats.BufferedSendBytes),
					humanize.SIWithDigits(stats.SendBytesPerSec, 2, "B"),
				)
			}
			p.Display()
		}
	}
}

// progLanguage is the language to use when displaying progress for a pull from a src db to a sink db.
type progLanguage int

const (
	defaultLanguage progLanguage = iota
	downloadLanguage
)

func buildProgStarter(language progLanguage) actions.ProgStarter {
	return func(ctx context.Context) (*sync.WaitGroup, chan pull.Stats) {
		statsCh := make(chan pull.Stats, 128)
		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			pullerProgFunc(ctx, statsCh, language)
		}()

		return wg, statsCh
	}
}

func stopProgFuncs(cancel context.CancelFunc, wg *sync.WaitGroup, statsCh chan pull.Stats) {
	cancel()
	close(statsCh)
	wg.Wait()
}

func getRemotes(queryist cli.Queryist, sqlCtx *sql.Context) (map[string]remoteInfo, error) {
	rows, err := GetRowsForSql(queryist, sqlCtx, "select * from dolt_remotes")
	if err != nil {
		return nil, fmt.Errorf("failed to read dolt remotes: %w", err)
	}

	remotes := map[string]remoteInfo{}
	for _, row := range rows {
		name := row[0].(string)
		url := row[1].(string)
		fetchSpecs, err := getJsonDocumentCol(sqlCtx, row[2])
		if err != nil {
			return nil, fmt.Errorf("failed to read fetch specs for remote %s: %w", name, err)
		}
		params, err := getJsonDocumentCol(sqlCtx, row[3])
		if err != nil {
			return nil, fmt.Errorf("failed to read params for remote %s: %w", name, err)
		}

		remote := remoteInfo{
			Name:       name,
			Url:        url,
			FetchSpecs: fetchSpecs,
			Params:     params,
		}
		remotes[name] = remote
	}
	return remotes, nil
}
