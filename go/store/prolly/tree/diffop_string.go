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

// Code generated by "stringer -type=diffOp -linecomment"; DO NOT EDIT.

package tree

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[DiffOpLeftAdd-0]
	_ = x[DiffOpRightAdd-1]
	_ = x[DiffOpLeftDelete-2]
	_ = x[DiffOpRightDelete-3]
	_ = x[DiffOpLeftModify-4]
	_ = x[DiffOpRightModify-5]
	_ = x[DiffOpConvergentAdd-6]
	_ = x[DiffOpConvergentDelete-7]
	_ = x[DiffOpConvergentModify-8]
	_ = x[DiffOpDivergentModifyResolved-9]
	_ = x[DiffOpDivergentDeleteConflict-10]
	_ = x[DiffOpDivergentModifyConflict-11]
}

const _diffOp_name = "leftAddrightAddleftDeleterightDeleteleftModifyrightModifyconvergentAddconvergentDeleteconvergentModifydivergenModifytResolveddivergentDeleteConflictdivergentModifyConflict"

var _diffOp_index = [...]uint8{0, 7, 15, 25, 36, 46, 57, 70, 86, 102, 125, 148, 171}

func (i DiffOp) String() string {
	if i >= DiffOp(len(_diffOp_index)-1) {
		return "diffOp(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _diffOp_name[_diffOp_index[i]:_diffOp_index[i+1]]
}
