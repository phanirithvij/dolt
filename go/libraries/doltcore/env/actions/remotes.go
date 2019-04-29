package actions

import (
	"context"
	"errors"

	"github.com/attic-labs/noms/go/datas"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
)

var ErrCantFF = errors.New("can't fast forward merge")

func Push(ctx context.Context, branch, remoteBranch string, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, progChan chan datas.PullProgress) error {
	canFF, err := srcDB.CanFastForward(ctx, remoteBranch, commit)

	if err != nil {
		return err
	} else if !canFF {
		return ErrCantFF
	}

	err = pantoerr.PanicToErrorNil("error pulling chunks", func() {
		destDB.PullChunks(ctx, srcDB, commit, progChan)
	})

	if err != nil {
		return err
	}

	err = destDB.FastForward(ctx, branch, commit)

	if err != nil {
		return err
	}

	err = srcDB.FastForward(ctx, remoteBranch, commit)

	return err
}

func Fetch(ctx context.Context, branch string, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, progChan chan datas.PullProgress) error {
	err := pantoerr.PanicToErrorNil("error pulling chunks", func() {
		destDB.PullChunks(ctx, srcDB, commit, progChan)
	})

	if err != nil {
		return err
	}

	return destDB.FastForward(ctx, branch, commit)
}
