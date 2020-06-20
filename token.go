package sqlcomposer

import (
	"fmt"
	"strings"
)

type Sort struct {
	Name string
	Direction Direction
}

type OrderBy []Sort

func (ob OrderBy) IsEmpty() bool {
	return len(ob) == 0
}

// Implement token replacer
func (ob OrderBy) TokenReplace(ctx map[string]interface{}) string {
	var sb []string

	if ob.IsEmpty() {
		return ""
	}

	for _, s := range ob {
		sb = append(sb, fmt.Sprintf("%s %s", s.Name, s.Direction))
	}

	return fmt.Sprintf("ORDER BY %s", strings.Join(sb, ", "))
}

// TODO rename to Condition
type ConditionStmt struct {
	Clause string
	Arg    map[string]interface{}
}

func (fs ConditionStmt) IsEmpty() bool {
	return fs.Clause == ""
}

// Implement token replacer
func (fs ConditionStmt) TokenReplace(ctx map[string]interface{}) string {
	if !fs.IsEmpty() {
		return fmt.Sprintf("WHERE %s", fs.Clause)
	}

	return ""
}

type SqlLimit struct {
	Offset int64
	Size   int64
}

// Implement token replacer
func (limit SqlLimit) TokenReplace(ctx map[string]interface{}) string {
	return fmt.Sprintf("LIMIT %d, %d", limit.Offset, limit.Size)
}
