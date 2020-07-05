package sqlcomposer

import (
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Operator string
type LogicOperator string
type Direction string

const (
	Equal          Operator = "="
	NotEqual                = "<>"
	Greater                 = ">"
	Less                    = "<"
	GreaterOrEqual          = ">="
	LessOrEqual             = "<="
	StartsWith              = "starts_with"
	Contains                = "contains"
	EndsWith                = "ends_with"
	In                      = "in"
	NotIn                   = "not_in"
	Between                 = "between"
	NotBetween              = "not_between"
	IsNull                  = "is_null"
	IsNotNull               = "is_not_null"
)

const (
	AND LogicOperator = "AND"
	OR                = "OR"
)

const (
	ASC  Direction = "ASC"
	DESC           = "DESC"
)

type Sort struct {
	Name      string
	Direction Direction
}

type Filter struct {
	Val  interface{}
	Op   Operator
	Attr string
}

type FilterGroup struct {
	LogicOp LogicOperator
	Filters []*Filter
}

//
// Condition handlers
//

// Handle filters to filters statement
func Conditions(f *[]Filter, op LogicOperator) (stmt ConditionStmt, err error) {
	var (
		conditions []string
	)

	conditions = []string{}
	stmt.Arg = map[string]interface{}{}
	stmt.ClauseSlice = map[string]string{}

	for _, value := range *f {
		var str strings.Builder

		paramsAttr := strings.Replace(value.Attr, ".", "_", -1)
		paramsAttr = generateNewAttrName(paramsAttr, stmt.Arg)

		switch value.Op {
		case StartsWith:
			str.WriteString(fmt.Sprintf("%s LIKE :%s", value.Attr, paramsAttr))

			err = likeParamsProcess(value.Val, paramsAttr, value.Op, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}
		case Contains:
			str.WriteString(fmt.Sprintf("%s LIKE :%s", value.Attr, paramsAttr))

			err = likeParamsProcess(value.Val, paramsAttr, value.Op, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}
		case EndsWith:
			str.WriteString(fmt.Sprintf("%s LIKE :%s", value.Attr, paramsAttr))

			err = likeParamsProcess(value.Val, paramsAttr, value.Op, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}

			break
		case In:
			str.WriteString(fmt.Sprintf("%s IN(:%s)", value.Attr, paramsAttr))
			stmt.Arg[paramsAttr] = value.Val
			break
		case NotIn:
			str.WriteString(fmt.Sprintf("%s NOT IN(:%s)", value.Attr, paramsAttr))
			stmt.Arg[paramsAttr] = value.Val
			break
		case Between:
			str.WriteString(fmt.Sprintf("%s >= :%s AND %s <= :%s",
				value.Attr, paramsAttr+"_1", value.Attr, paramsAttr+"_2"))

			err = betweenParamsProcess(value.Val, paramsAttr, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}

			break
		case NotBetween:
			str.WriteString(fmt.Sprintf("%s <= :%s AND %s >= :%s",
				value.Attr, paramsAttr+"_1", value.Attr, paramsAttr+"_2"))

			err = betweenParamsProcess(value.Val, paramsAttr, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}
			break
		case IsNull:
			str.WriteString(fmt.Sprintf("%s IS NULL", value.Attr))
			break
		case IsNotNull:
			str.WriteString(fmt.Sprintf("%s IS NOT NULL", value.Attr))
			break
		default:
			str.WriteString(fmt.Sprintf("%s %s :%s", value.Attr, value.Op, paramsAttr))
			stmt.Arg[paramsAttr] = value.Val
		}

		conditions = append(conditions, str.String())
		// put string to clause slice
		stmt.ClauseSlice[paramsAttr] = str.String()
	}

	stmt.Clause = strings.Join(conditions, fmt.Sprintf(" %s ", op))
	return stmt, nil
}

func generateNewAttrName(s string, args map[string]interface{}) string {
	var (
		i  int64
		ns string
	)
	ns = s
	ss := strings.Split(s, "_")

	if d, err := strconv.ParseInt(ss[len(ss)-1], 0, 64); err == nil {
		ns = strings.Join(ss[0:len(ss)-1], "_")
		i = d
	} else {
		i = 0
	}

	if _, ok := args[s]; ok {
		i ++
		return generateNewAttrName(fmt.Sprintf("%s_%d", ns, i), args)
	}

	return s
}

func numberSuffixMatch(s string) (prefix, suffix string) {
	r := regexp.MustCompile(`(\w+_?)+_(\d)+$`)
	ms := r.FindAllStringSubmatch(s, -1)

	if len(ms) == 0 {
		return "", ""
	}

	return ms[0][1], ms[0][2]
}

func WhereOr(f *[]Filter) (stmt ConditionStmt, err error) {
	return Conditions(f, OR)
}

func WhereAnd(f *[]Filter) (stmt ConditionStmt, err error) {
	return Conditions(f, AND)
}

func CombineOr(stmts ...ConditionStmt) (stmt ConditionStmt) {
	return Combine(OR, stmts...)
}

func CombineAnd(stmts ...ConditionStmt) (stmt ConditionStmt) {
	return Combine(AND, stmts...)
}

// Combine two or more filter statement to one
func Combine(op LogicOperator, stmts ...ConditionStmt) (stmt ConditionStmt) {
	var clauses []string
	stmt.Arg = map[string]interface{}{}
	stmt.ClauseSlice = map[string]string{}

	for _, s := range stmts {
		if s.Clause != "" {
			c := s.Clause

			keys := make([]string, 0, len(s.Arg))

			for k := range s.Arg {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				nk := generateNewAttrName(k, stmt.Arg)
				// replace to new placeholder

				c = strings.Replace(c, ":"+k, ":"+nk, 1)

				ps, _ := numberSuffixMatch(k)
				if ps != "" {
					if _, ok := s.ClauseSlice[ps]; ok {
						s.ClauseSlice[ps] = strings.Replace(s.ClauseSlice[ps], ":"+k, ":"+nk, 1)
					}
				} else {
					if _, ok := s.ClauseSlice[k]; ok {
						s.ClauseSlice[k] = strings.Replace(s.ClauseSlice[k], ":"+k, ":"+nk, 1)
					}
				}

				stmt.Arg[nk] = s.Arg[k]
			}

			for k, cs := range s.ClauseSlice {
				if _, ok := stmt.ClauseSlice[k]; ok {
					// replace new name
					stmt.ClauseSlice[k] = fmt.Sprintf("%s %s %s", stmt.ClauseSlice[k], op, cs)
				} else {
					stmt.ClauseSlice[k] = cs
				}
			}

			clauses = append(clauses, fmt.Sprintf("(%s)", c))
		}
	}

	stmt.Clause = strings.Join(clauses, fmt.Sprintf(" %s ", op))

	return stmt
}

// Helper func for process the between params
func betweenParamsProcess(v interface{}, attr string, params map[string]interface{}) error {
	s := reflect.ValueOf(v)

	if s.Kind() != reflect.Slice {
		return errors.New("between operator value must be slice type")
	}

	if s.Len() != 2 {
		return errors.New("between operator required two value")
	}

	k := s.Index(0).Kind()

	if k == reflect.Int || k == reflect.Int8 || k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64 {
		params[attr+"_1"] = s.Index(0).Int()
		params[attr+"_2"] = s.Index(1).Int()
	}

	if k == reflect.Float32 || k == reflect.Float64 {
		params[attr+"_1"] = s.Index(0).Float()
		params[attr+"_2"] = s.Index(1).Float()
	}

	if k == reflect.String {
		params[attr+"_1"] = s.Index(0).String()
		params[attr+"_2"] = s.Index(1).String()
	}

	if k == reflect.Interface {
		params[attr+"_1"] = s.Index(0).Elem().String()
		params[attr+"_2"] = s.Index(1).Elem().String()

		if params[attr+"_1"] == "<int Value>" {
			params[attr+"_1"] = s.Index(0).Elem().Int()
		}

		if params[attr+"_2"] == "<int Value>" {
			params[attr+"_2"] = s.Index(1).Elem().Int()
		}

		if params[attr+"_1"] == "<float64 Value>" {
			params[attr+"_1"] = s.Index(0).Elem().Float()
		}

		if params[attr+"_2"] == "<float64 Value>" {
			params[attr+"_2"] = s.Index(1).Elem().Float()
		}
	}

	return nil
}

// Helper func for process the like params
func likeParamsProcess(v interface{}, attr string, op Operator, params map[string]interface{}) error {
	s := reflect.ValueOf(v)
	if s.Kind() != reflect.String {
		return errors.New("like operator value must be string type")
	}

	if op == StartsWith {
		params[attr] = v.(string) + "%"
	}

	if op == EndsWith {
		params[attr] = "%" + v.(string)
	}

	if op == Contains {
		params[attr] = "%" + v.(string) + "%"
	}

	return nil
}

//
// Filter pipeline
//
type Expander interface {
	Expand(origFilter Filter) (ConditionStmt, error)
}

type FilterPipeline struct {
	Attr      string
	CombineOp LogicOperator
	Expander  Expander
}

// High order filter handler that can with pipelines, pipeline definition could implement custom behaviors to process
// complex filter logic
func FilterToWhereAnd(filters *[]Filter, pipelines ...FilterPipeline) (stmt ConditionStmt, err error) {
	var restFilters []Filter

	for _, f := range *filters {
		contains := false
		for _, p := range pipelines {
			if f.Attr == p.Attr {
				contains = true
			}
		}

		if !contains {
			restFilters = append(restFilters, f)
		}
	}

	stmt, err = WhereAnd(&restFilters)

	for _, f := range *filters {
		for _, p := range pipelines {
			if f.Attr == p.Attr {
				subFilterStmt, err := p.Expander.Expand(f)

				if err != nil {
					return stmt, err
				}

				stmt = Combine(p.CombineOp, subFilterStmt, stmt)
			}
		}
	}

	return stmt, err
}
