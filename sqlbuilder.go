package sqlcomposer

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"regexp"
	"strings"
)

type TokenReplacer interface {
	TokenReplace(ctx map[string]interface{}) string
}

type SqlCompositionFields map[string]SqlCompositionFieldGroup

type SqlCompositionFieldGroup []SqlCompositionField

func (group SqlCompositionFieldGroup) TokenReplace(ctx map[string]interface{}) string {
	var res []string
	for _, field := range group {
		res = append(res, fmt.Sprintf("%s AS %s", field.Expr, field.Name))
	}

	return strings.Join(res, ", ")
}

type SqlCompositionField struct {
	Name string `yaml:"name"`
	Expr string `yaml:"expr"`
	Type string `yaml:"type,omitempty"`
}

type TokenParam struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type TokenDefinition struct {
	Params []TokenParam `yaml:"params,omitempty"`
}

type FilterPipelineParam struct {
	Name  string      `yaml:"name"`
	Value interface{} `yaml:"value"`
}

type FilterPipelineParams []FilterPipelineParam

func (p *FilterPipelineParams) Get(key string) interface{} {
	for _, v := range *p {
		if key == v.Name {
			return v.Value
		}
	}
	return nil
}

type FilterPipelineDefinition struct {
	Type   string               `yaml:"type"`
	Params FilterPipelineParams `yaml:"params,omitempty"`
}

type SqlApiDoc struct {
	Info struct {
		Name    string `yaml:"name"`
		Version string `yaml:"version"`
	} `yaml:"info"`
	Composition struct {
		Fields            SqlCompositionFields                `yaml:"fields"`
		Tokens            map[string]TokenDefinition          `yaml:"tokens,omitempty"`
		FilterPipelines   map[string]FilterPipelineDefinition `yaml:"filterPipelines,omitempty"`
		DefaultConditions []Filter                            `yaml:"defaultConditions,omitempty"`
		Subject           map[string]string                   `yaml:"subject"`
	} `yaml:"composition"`
}

type ExpanderGenerator func(params FilterPipelineParams) Expander

// SqlBuilder be responsible for build sql from yaml config
type SqlBuilder struct {
	DB         *sqlx.DB
	Doc        *SqlApiDoc
	Conditions *ConditionStmt
	limit      *SqlLimit
	tokens     map[string]interface{}
	pipelines  map[string]ExpanderGenerator
}

func NewSqlBuilder(db *sqlx.DB, yamlFile []byte) (*SqlBuilder, error) {
	doc := SqlApiDoc{}

	err := yaml.Unmarshal(yamlFile, &doc)

	if err != nil {
		return nil, errors.Wrap(err, "Construct SqlBuilder failure")
	}

	filterStmt := new(ConditionStmt)

	if doc.Composition.DefaultConditions != nil {
		*filterStmt, err = WhereAnd(&doc.Composition.DefaultConditions)
		if err != nil {
			return nil, errors.Wrap(err, "default conditions process failure")
		}
	}

	return &SqlBuilder{
		DB:         db,
		Doc:        &doc,
		Conditions: filterStmt,
		limit:      &SqlLimit{0, 10},
		tokens:     make(map[string]interface{}),
		pipelines:  make(map[string]ExpanderGenerator),
	}, nil
}

func (sc *SqlBuilder) RegisterToken(name string, gen func(params []TokenParam) TokenReplacer) {
	if td, ok := sc.Doc.Composition.Tokens[name]; ok {
		sc.tokens[name] = gen(td.Params)
	}
}

func (sc *SqlBuilder) RegisterPipelineType(t string) error {
	if _, ok := sc.pipelines[t]; !ok {
		sc.pipelines[t] = GenerateExpander(t)
		return nil
	}

	return fmt.Errorf("%s pipline type is registered", t)
}

func (sc *SqlBuilder) AndConditions(c *ConditionStmt) *SqlBuilder {
	combined := Combine(AND, *sc.Conditions, *c)
	sc.Conditions = &combined
	return sc
}

func (sc *SqlBuilder) OrConditions(c *ConditionStmt) *SqlBuilder {
	combined := Combine(OR, *sc.Conditions, *c)
	sc.Conditions = &combined
	return sc
}

func (sc *SqlBuilder) SetConditions(c *ConditionStmt) *SqlBuilder {
	sc.Conditions = c
	return sc
}

func (sc *SqlBuilder) AddFilters(f []Filter, operator LogicOperator) error {
	condition, err := sc.applyPipelines(f, operator)

	if err != nil {
		return errors.Wrap(err, "add filters to SqlBuilder failure")
	}

	combined := CombineAnd(*sc.Conditions, condition)
	sc.Conditions = &combined

	return nil
}

func (sc *SqlBuilder) applyPipelines(filters []Filter, operator LogicOperator) (stmt ConditionStmt, err error) {
	var restFilters []Filter

	for _, f := range filters {
		contains := false
		for k := range sc.Doc.Composition.FilterPipelines {
			if f.Attr == k {
				contains = true
			}
		}

		if !contains {
			restFilters = append(restFilters, f)
		}
	}

	stmt, err = Conditions(&restFilters, operator)

	if len(restFilters) == len(filters) {
		return stmt, err
	}

	for _, f := range filters {
		for attr, p := range sc.Doc.Composition.FilterPipelines {
			if f.Attr != attr {
				continue
			}
			if gen, ok := sc.pipelines[p.Type]; ok {
				expander := gen(p.Params)
				subStmt, err := expander.Expand(f)

				if err != nil {
					return stmt, fmt.Errorf("%s attr expend failure", attr)
				}

				if err != nil {
					return stmt, err
				}

				stmt = CombineAnd(subStmt, stmt)
			} else {
				return stmt, fmt.Errorf("%s pipline type not registered", p.Type)
			}
		}
	}

	return stmt, err
}

func (sc *SqlBuilder) Limit(offset int64, size int64) *SqlBuilder {
	sc.limit.Offset = offset
	sc.limit.Size = size
	return sc
}

func (sc *SqlBuilder) compose(s string) (string, error) {
	ctx := map[string]interface{}{
		"where": *sc.Conditions,
		"limit": *sc.limit,
	}

	// fields context process
	for k, g := range sc.Doc.Composition.Fields {
		ctx["fields."+k] = g
	}

	for k, v := range sc.tokens {
		ctx[k] = v
	}

	return tokenReplace(s, ctx)
}

// Build query statement
func (sc *SqlBuilder) Rebind(key string) (string, []interface{}, error) {
	if s, ok := sc.Doc.Composition.Subject[key]; ok {
		subject, err := sc.compose(s)

		if err != nil {
			return "", nil, errors.Wrap(err, "sql compose failure")
		}

		query, args, err := sqlx.Named(subject, sc.Conditions.Arg)

		if err != nil {
			return query, nil, errors.Wrap(err, "Named failure")
		}

		reg := regexp.MustCompile(`IN\(:\w+\)`)
		if reg.MatchString(subject) {
			query, args, err = sqlx.In(query, args...)

			if err != nil {
				return query, nil, errors.Wrap(err, "IN sql rebind failure")
			}
		}

		query = sc.DB.Rebind(query)
		return query, args, nil
	}

	return "", nil, fmt.Errorf("key name %s not exists in composition doc", key)
}

// Result row type convert
// TODO support custom type
func (sc *SqlBuilder) RowConvert(row *map[string]interface{}) {
	fs := SqlCompositionFieldGroup{}

	for _, fields := range sc.Doc.Composition.Fields {
		for _, f := range fields {
			fs = append(fs, f)
		}
	}

	for _, f := range fs {
		if val, ok := (*row)[f.Name]; ok {
			switch f.Type {
			case "string":
				(*row)[f.Name] = string(val.([]byte))
				continue
			}
		}
	}
}
