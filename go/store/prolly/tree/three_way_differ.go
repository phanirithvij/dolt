// Copyright 2023 Dolthub, Inc.
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

package tree

import (
	"bytes"
	"context"
	"github.com/dolthub/dolt/go/store/val"
)

// ThreeWayDiffer is an iterator that gives an increased level of granularity
// of diffs between three root values. See diffOp for the classes of diffs.
type ThreeWayDiffer[K, V ~[]byte, O Ordering[K]] struct {
	// TODO: explain why we keep the left rows map but use an Diff iterator for the right side
	left          StaticMap[K, V, O]
	rIter         Differ[K, O]
	resolveCb     resolveCb
	keyless       bool
	order         Ordering[K]
	valueComparer val.TupleDesc
}

type resolveCb func(val.Tuple, val.Tuple, val.Tuple) (val.Tuple, bool)

func NewThreeWayDiffer[K, V ~[]byte, O Ordering[K]](
	ctx context.Context,
	ns NodeStore,
	left, right, base StaticMap[K, V, O],
	resolveCb resolveCb,
	keyless bool,
	keyComparer O,
	valueComparer val.TupleDesc,
) (*ThreeWayDiffer[K, V, O], error) {
	// Because we (currently) always merge from right (THEIRS) to left (OURS), we don't actually need
	// to compute every single row diff for the left side (OURS) – those have already been applied to
	// the target side and don't really need to be messed with again.
	// However we DO need the left side row (OURS) in keyComparer to calculate exactly how a row has been changed,
	// for example, was the change on the right side (THEIRS) divergent or convergent with the data currently
	// on the left side (OURS).
	//
	// So, instead of using the two iterators and marching them forward until we find matches... just use the
	// the right (THEIRS) iterator and for each row, do a lookup into the table on the left side (OURS) and fill
	// in the data there.
	//
	// TODO: The callback function for cell-wise merging may need to be extracted to a different part of the code?
	//
	// TODO: This changes makes this type a bit more specific to our merge logic and a bit less of a general purpose
	//       three-way differ. Merge is currently the only place where we use this type, so that's fine for now.

	rd, err := DifferFromRoots[K](ctx, ns, ns, base.Root, right.Root, keyComparer)
	if err != nil {
		return nil, err
	}

	return &ThreeWayDiffer[K, V, O]{
		left:          left,
		rIter:         rd,
		resolveCb:     resolveCb,
		keyless:       keyless,
		order:         keyComparer,
		valueComparer: valueComparer,
	}, nil
}

type threeWayDiffState uint8

const (
	dsUnknown threeWayDiffState = iota
	dsInit
	dsDiffFinalize
	dsCompare
	dsNewLeft
	dsNewRight
	dsMatch
	dsMatchFinalize
)

func (d *ThreeWayDiffer[K, V, O]) Next(ctx context.Context) (ThreeWayDiff, error) {
	rightDiff, err := d.rIter.Next(ctx)
	if err != nil {
		return ThreeWayDiff{}, err
	}

	// We've got the right diff with the key, and to/from tuples
	// Now we just need to do a lookup by key into the left side and fill that in
	var leftValue V
	d.left.Get(ctx, K(rightDiff.Key), func(k K, v V) error {
		leftValue = v
		return nil
	})

	leftHas, err := d.left.Has(ctx, K(rightDiff.Key))
	if err != nil {
		return ThreeWayDiff{}, err
	}

	ancestorValue := rightDiff.From

	noLeftDiff := false
	var leftDiffType DiffType
	if ancestorValue == nil && leftHas {
		leftDiffType = AddedDiff
	} else if ancestorValue != nil && !leftHas {
		leftDiffType = RemovedDiff
	} else {
		// Could be modified, or no diff
		if leftValue == nil && ancestorValue == nil {
			noLeftDiff = true
		} else if d.valueComparer.Compare(val.Tuple(leftValue), val.Tuple(ancestorValue)) == 0 {
			noLeftDiff = true
		} else {
			leftDiffType = ModifiedDiff
		}
	}

	// If the left side has the key and right side is not nil, then the right side is adding a new row
	// TODO: Is this condition right?
	//       How about deletes from the right side? Those aren't caught here, right?
	if noLeftDiff { // && !leftHas && rightDiff.To != nil {
		return d.newRightEdit(rightDiff.Key, rightDiff.From, rightDiff.To, rightDiff.Type), nil
	} else {
		if leftValue == nil && rightDiff.To == nil {
			return d.newConvergentEdit(rightDiff.Key, Item(leftValue), leftDiffType), nil
		} else if leftValue == nil || rightDiff.To == nil {
			return d.newDivergentDeleteConflict(rightDiff.Key, rightDiff.From, Item(leftValue), rightDiff.To), nil
		} else if leftDiffType == rightDiff.Type && bytes.Equal(leftValue, rightDiff.To) {
			return d.newConvergentEdit(rightDiff.Key, Item(leftValue), leftDiffType), nil
		} else {
			// Cell-wise merge case
			resolved, ok := d.resolveCb(val.Tuple(leftValue), val.Tuple(rightDiff.To), val.Tuple(rightDiff.From))
			if !ok {
				return d.newDivergentClashConflict(rightDiff.Key, rightDiff.From, Item(leftValue), rightDiff.To), nil
			} else {
				return d.newDivergentResolved(rightDiff.Key, Item(leftValue), rightDiff.To, Item(resolved)), nil
			}
		}
	}

	panic("unknown threeWayDiffState!")
}

//func (d *ThreeWayDiffer[K, V, O]) NextOLD(ctx context.Context) (ThreeWayDiff, error) {
//	var err error
//	var res ThreeWayDiff
//	nextState := dsInit
//	for {
//		// The regular flow will be:
//		// - dsInit: get the first diff in each iterator if this is the first Next
//		// - dsDiffFinalize: short-circuit comparing if one iterator is exhausted
//		// - dsCompare: compare keys for the leading diffs, to determine whether
//		//   the diffs are independent, or require further disambiguation.
//		// - dsNewLeft: an edit was made to the left root value for a key not edited
//		//   on the right.
//		// - dsNewRight: ditto above, edit to key only on right.
//		// - dsMatch: edits made to the same key in left and right roots, either
//		//   resolve non-overlapping field changes or indicate schema/value conflict.
//		// - dsMatchFinalize: increment both iters after performing match disambiguation.
//		switch nextState {
//		case dsInit:
//			//if !d.lDone {
//			//	if d.lDiff.Key == nil {
//			//		d.lDiff, err = d.lIter.Next(ctx)
//			//		if errors.Is(err, io.EOF) {
//			//			d.lDone = true
//			//		} else if err != nil {
//			//			return ThreeWayDiff{}, err
//			//		}
//			//	}
//			//}
//			if !d.rDone {
//				if d.rDiff.Key == nil {
//					d.rDiff, err = d.rIter.Next(ctx)
//					if errors.Is(err, io.EOF) {
//						d.rDone = true
//					} else if err != nil {
//						return ThreeWayDiff{}, err
//					}
//				}
//			}
//			nextState = dsDiffFinalize
//		case dsDiffFinalize:
//			if d.rDone {
//				return ThreeWayDiff{}, io.EOF
//			} else {
//				// TODO: Do we even need a dsCompare state anymore?
//				nextState = dsCompare
//			}
//		case dsCompare:
//			cmp := d.lIter.order.Compare(K(d.lDiff.Key), K(d.rDiff.Key))
//			switch {
//			case cmp < 0:
//				nextState = dsNewLeft
//			case cmp == 0:
//				nextState = dsMatch
//			case cmp > 0:
//				nextState = dsNewRight
//			default:
//			}
//		case dsNewLeft:
//			res = d.newLeftEdit(d.lDiff.Key, d.lDiff.To, d.lDiff.Type)
//			d.lDiff, err = d.lIter.Next(ctx)
//			if errors.Is(err, io.EOF) {
//				d.lDone = true
//			} else if err != nil {
//				return ThreeWayDiff{}, err
//			}
//			return res, nil
//		case dsNewRight:
//			res = d.newRightEdit(d.rDiff.Key, d.rDiff.From, d.rDiff.To, d.rDiff.Type)
//			d.rDiff, err = d.rIter.Next(ctx)
//			if errors.Is(err, io.EOF) {
//				d.rDone = true
//			} else if err != nil {
//				return ThreeWayDiff{}, err
//			}
//			return res, nil
//		case dsMatch:
//			if d.lDiff.To == nil && d.rDiff.To == nil {
//				res = d.newConvergentEdit(d.lDiff.Key, d.lDiff.To, d.lDiff.Type)
//			} else if d.lDiff.To == nil || d.rDiff.To == nil {
//				res = d.newDivergentDeleteConflict(d.lDiff.Key, d.lDiff.From, d.lDiff.To, d.rDiff.To)
//			} else if d.lDiff.Type == d.rDiff.Type && bytes.Equal(d.lDiff.To, d.rDiff.To) {
//				res = d.newConvergentEdit(d.lDiff.Key, d.lDiff.To, d.lDiff.Type)
//			} else {
//				resolved, ok := d.resolveCb(val.Tuple(d.lDiff.To), val.Tuple(d.rDiff.To), val.Tuple(d.lDiff.From))
//				if !ok {
//					res = d.newDivergentClashConflict(d.lDiff.Key, d.lDiff.From, d.lDiff.To, d.rDiff.To)
//				} else {
//					res = d.newDivergentResolved(d.lDiff.Key, d.lDiff.To, d.rDiff.To, Item(resolved))
//				}
//			}
//			nextState = dsMatchFinalize
//		case dsMatchFinalize:
//			d.lDiff, err = d.lIter.Next(ctx)
//			if errors.Is(err, io.EOF) {
//				d.lDone = true
//			} else if err != nil {
//				return ThreeWayDiff{}, err
//			}
//
//			d.rDiff, err = d.rIter.Next(ctx)
//			if errors.Is(err, io.EOF) {
//				d.rDone = true
//			} else if err != nil {
//				return ThreeWayDiff{}, err
//			}
//
//			return res, nil
//		default:
//			panic(fmt.Sprintf("unknown threeWayDiffState: %d", nextState))
//		}
//	}
//}

func (d *ThreeWayDiffer[K, V, O]) Close() error {
	return nil
}

//go:generate stringer -type=diffOp -linecomment

type DiffOp uint16

const (
	DiffOpLeftAdd                 DiffOp = iota // leftAdd
	DiffOpRightAdd                              // rightAdd
	DiffOpLeftDelete                            //leftDelete
	DiffOpRightDelete                           //rightDelete
	DiffOpLeftModify                            //leftModify
	DiffOpRightModify                           //rightModify
	DiffOpConvergentAdd                         //convergentAdd
	DiffOpConvergentDelete                      //convergentDelete
	DiffOpConvergentModify                      //convergentModify
	DiffOpDivergentModifyResolved               //divergenModifytResolved
	DiffOpDivergentDeleteConflict               //divergentDeleteConflict
	DiffOpDivergentModifyConflict               //divergentModifyConflict
)

// ThreeWayDiff is a generic object for encoding a three way diff.
type ThreeWayDiff struct {
	// Op indicates the type of diff
	Op DiffOp
	// a partial set of tuple values are set
	// depending on the diffOp
	Key, Base, Left, Right, Merged val.Tuple
}

func (d *ThreeWayDiffer[K, V, O]) newLeftEdit(key, left Item, typ DiffType) ThreeWayDiff {
	var op DiffOp
	switch typ {
	case AddedDiff:
		op = DiffOpLeftAdd
	case ModifiedDiff:
		op = DiffOpLeftModify
	case RemovedDiff:
		op = DiffOpLeftDelete
	default:
		panic("unknown diff type")
	}
	return ThreeWayDiff{
		Op:   op,
		Key:  val.Tuple(key),
		Left: val.Tuple(left),
	}
}

func (d *ThreeWayDiffer[K, V, O]) newRightEdit(key, base, right Item, typ DiffType) ThreeWayDiff {
	var op DiffOp
	switch typ {
	case AddedDiff:
		op = DiffOpRightAdd
	case ModifiedDiff:
		op = DiffOpRightModify
	case RemovedDiff:
		op = DiffOpRightDelete
	default:
		panic("unknown diff type")
	}
	return ThreeWayDiff{
		Op:    op,
		Key:   val.Tuple(key),
		Base:  val.Tuple(base),
		Right: val.Tuple(right),
	}
}

func (d *ThreeWayDiffer[K, V, O]) newConvergentEdit(key, left Item, typ DiffType) ThreeWayDiff {
	var op DiffOp
	switch typ {
	case AddedDiff:
		op = DiffOpConvergentAdd
	case ModifiedDiff:
		op = DiffOpConvergentModify
	case RemovedDiff:
		op = DiffOpConvergentDelete
	default:
		panic("unknown diff type")
	}
	return ThreeWayDiff{
		Op:   op,
		Key:  val.Tuple(key),
		Left: val.Tuple(left),
	}
}

func (d *ThreeWayDiffer[K, V, O]) newDivergentResolved(key, left, right, merged Item) ThreeWayDiff {
	return ThreeWayDiff{
		Op:     DiffOpDivergentModifyResolved,
		Key:    val.Tuple(key),
		Left:   val.Tuple(left),
		Right:  val.Tuple(right),
		Merged: val.Tuple(merged),
	}
}

func (d *ThreeWayDiffer[K, V, O]) newDivergentDeleteConflict(key, base, left, right Item) ThreeWayDiff {
	return ThreeWayDiff{
		Op:    DiffOpDivergentDeleteConflict,
		Key:   val.Tuple(key),
		Base:  val.Tuple(base),
		Left:  val.Tuple(left),
		Right: val.Tuple(right),
	}
}

func (d *ThreeWayDiffer[K, V, O]) newDivergentClashConflict(key, base, left, right Item) ThreeWayDiff {
	return ThreeWayDiff{
		Op:    DiffOpDivergentModifyConflict,
		Key:   val.Tuple(key),
		Base:  val.Tuple(base),
		Left:  val.Tuple(left),
		Right: val.Tuple(right),
	}
}
