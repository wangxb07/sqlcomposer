package sqlcomposer

import "reflect"

func GenerateExpander(t string) ExpanderGenerator {
	switch t {
	case "fulltext":
		return func(ps FilterPipelineParams) Expander {
			paramFields := ps.Get("fields")
			rv := reflect.ValueOf(paramFields)

			if rv.Kind() == reflect.Slice {
				fields := make([]string, rv.Len())

				for i := 0; i < rv.Len(); i++ {
					fields[i] = rv.Index(i).Elem().String()
				}

				return &FulltextSearchExpander{
					Fields: fields,
				}
			}

			return nil
		}
	}

	return nil
}

type FulltextSearchExpander struct {
	Fields []string
}

func (e *FulltextSearchExpander) Expand(origFilter Filter) (ConditionStmt, error) {
	var filters []Filter
	filters = []Filter{}

	for _, field := range e.Fields {
		filters = append(filters, Filter{
			Attr: field,
			Op:   origFilter.Op,
			Val:  origFilter.Val,
		})
	}

	return WhereOr(&filters)
}
