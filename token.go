package sqlcomposer

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

//TokenReplacer
type TokenReplacer interface {
	TokenReplace(ctx map[string]interface{}) string
}

type ParameterizedTokenReplacer interface {
	TokenReplaceWithParams(params string, token string) string
}

//
//OrderBy
//
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
	Clause      string
	Arg         map[string]interface{}
	ClauseSlice map[string]string
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

func removeCondition(s string, c string) (res string) {
	rc := strings.ReplaceAll(strings.ReplaceAll(c, "(", "\\("), ")", "\\)")

	l := regexp.MustCompile(fmt.Sprintf(`\s+(AND|OR)?\s+(\(%s\)|%s)`, rc, rc))
	r := regexp.MustCompile(fmt.Sprintf(`(\(%s\)|%s)\s+(AND|OR)?\s+`, rc, rc))

	replaced := l.ReplaceAllString(s, "")

	if replaced == s {
		replaced = r.ReplaceAllString(s, "")
	}

	if replaced != s {
		res = replaced
	} else {
		res = strings.ReplaceAll(s, c, "")
	}

	emptyRep := regexp.MustCompile(`\(\)`)
	hasEmpty := emptyRep.FindAllString(res, -1)

	if len(hasEmpty) > 0 {
		return removeCondition(res, "()")
	}

	return res
}

// Implement token replacer
func (fs ConditionStmt) TokenReplaceWithParams(params string, token string) string {
	if !fs.IsEmpty() {
		include, fields := processConditionsParameters(params)

		if len(fields) == 0 {
			return fmt.Sprintf("WHERE %s", fs.Clause)
		}

		clauses := fs.Clause

		excluded := make([]string, 0)

		for k, _ := range fs.ClauseSlice {
			in := false
			for _, f := range fields {
				if f == k {
					in = true
					break
				}
			}

			if !in {
				excluded = append(excluded, k)
			}
		}

		if include {
			for _, f := range excluded {
				if cs, ok := fs.ClauseSlice[f]; ok {
					clauses = removeCondition(clauses, cs)
				}
			}
		} else {
			for _, f := range fields {
				if cs, ok := fs.ClauseSlice[f]; ok {
					clauses = removeCondition(clauses, cs)
				}
			}
		}

		if len(clauses) == 0 {
			return ""
		}

		if token == "where" {
			return fmt.Sprintf("WHERE %s", clauses)
		}

		if token == "having" {
			return fmt.Sprintf("HAVING %s", clauses)
		}
	}

	return ""
}

func processConditionsParameters(p string) (include bool, fields []string) {
	// not include those fields
	if strings.HasPrefix(p, "!") {
		return false, strings.Split(p[1:], ",")
	}

	if p == "*" {
		return true, make([]string, 0)
	}

	return true, strings.Split(p, ",")
}

//
//SqlLimit
//
type SqlLimit struct {
	Offset int64
	Size   int64
}

// Implement token replacer
func (limit SqlLimit) TokenReplace(ctx map[string]interface{}) string {
	return fmt.Sprintf("LIMIT %d, %d", limit.Offset, limit.Size)
}

//
//SqlCompositionFieldGroup
//
type SqlCompositionFieldGroup []SqlCompositionField

// Implement token replacer
func (group SqlCompositionFieldGroup) TokenReplace(ctx map[string]interface{}) string {
	var res []string
	for _, field := range group {
		res = append(res, fmt.Sprintf("%s AS %s", field.Expr, field.Name))
	}

	return strings.Join(res, ", ")
}

//
// Token replace
//
func tokenReplace(s string, tks map[string]interface{}) (rs string, err error) {
	// collect all token placeholders on the string
	tps := CollectTokenPlaceholder(s)

	// no token need replace
	if len(tps) == 0 {
		return s, nil
	}

	rs = s
	for _, placeholder := range tps {
		if tr, ok := tks[placeholder[1]]; ok {
			// tr is string
			if rt := reflect.TypeOf(tr); rt.Kind() == reflect.String {
				rs = strings.Replace(rs, placeholder[0], tr.(string), 1)
			} else {
				// index 2 indicate the token params
				if len(placeholder) == 4 && len(placeholder[2]) == 0 {
					replacer, ok := tr.(TokenReplacer)

					if !ok {
						return rs, fmt.Errorf("placeholder %s in context must implemented TokenReplacer", placeholder[0])
					}

					rs = strings.Replace(rs, placeholder[0], replacer.TokenReplace(tks), 1)
				} else {
					replacer, ok := tr.(ParameterizedTokenReplacer)

					if !ok {
						return rs, fmt.Errorf("placeholder %s in context must implemented ParameterizedTokenReplacer", placeholder[0])
					}

					params := placeholder[2][1 : len(placeholder[2])-1]
					rs = strings.Replace(rs, placeholder[0], replacer.TokenReplaceWithParams(params, placeholder[1]), 1)
				}
			}
		} else {
			return rs, fmt.Errorf("placeholder [%s] not definition in context", placeholder[1])
		}
	}

	return tokenReplace(replaceSpaceString(rs), tks)
}

func replaceSpaceString(s string) string {
	return strings.Trim(strings.Replace(strings.Replace(s, "\n", " ", -1), "\t", " ", -1), " ")
}

//CollectTokenPlaceholder
func CollectTokenPlaceholder(s string) (tps [][]string) {
	r := regexp.MustCompile(`%([\w.]+)({([\w*!]+,?)*})?`)
	return r.FindAllStringSubmatch(s, -1)
}
