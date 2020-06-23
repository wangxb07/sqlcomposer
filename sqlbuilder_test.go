package sqlcomposer

import (
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestNewSqlBuilder(t *testing.T) {
	var sqlComposition = `
info:
  name: example
  version: 1.0.0
composition:
  fields:
    base:
      - name: name
        expr: users.name
        type: string
      - name: age
        expr: users.age
    statistic:
      - name: consume_times
        expr: COUNT(orders.id)
      - name: consume_total
        expr: SUM(orders.total_amount)
  subject: 
    list: "SELECT %fields.base, %fields.statistic FROM users LEFT JOIN orders ON orders.uid = users.uid %where{!consume_total} GROUP BY users.uid %having{consume_total} %limit"
    total: "SELECT count(users.uid) FROM users LEFT JOIN order ON order.uid = users.uid %where GROUP BY users.uid"`

	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)

		sb, err := NewSqlBuilder(db, []byte(sqlComposition))

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "example", sb.Doc.Info.Name)
		assert.Equal(t, "1.0.0", sb.Doc.Info.Version)
		assert.Equal(t, "consume_times", sb.Doc.Composition.Fields["statistic"][0].Name)
		assert.Equal(t, "COUNT(orders.id)", sb.Doc.Composition.Fields["statistic"][0].Expr)

		where, err := WhereAnd(&[]Filter{
			{Val: "Barry", Op: Contains, Attr: "users.name"},
		})

		if err != nil {
			t.Fatal(err)
		}

		q, a, err := sb.AndConditions(&where).Limit(0, 10).Rebind("list")

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"WHERE users.name LIKE ? GROUP BY users.uid  LIMIT 0, 10", q)

		rows, err := db.Queryx(q, a...)

		assert.Equal(t, true, rows.Next())
		row := make(map[string]interface{})
		err = rows.MapScan(row)

		if err != nil {
			t.Fatal(err)
		}

		sb.RowConvert(&row)

		assert.Equal(t, "Barry", row["name"])
		assert.Equal(t, int64(24), row["age"])
		assert.Equal(t, int64(1), row["consume_times"])
		assert.Equal(t, 28.8, row["consume_total"])

		_ = sb.AddFilters([]Filter{
			{Val: "28", Op: Greater, Attr: "consume_total"},
		}, AND)

		q, a, err = sb.Rebind("list")

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"WHERE users.name LIKE ? GROUP BY users.uid HAVING consume_total > ? LIMIT 0, 10", q)
	})
}

type attrsTokenReplacer struct {
	Attrs map[string]string
	DB    *sqlx.DB
}

func (atr attrsTokenReplacer) TokenReplace(ctx map[string]interface{}) string {
	return "LEFT JOIN fty_obj_attr AS prod_material ON prod_material.attr_sid = '87c53961debe28ecaf55dfc5af1c9039' AND prod_material.obj_sid = fty_product.sid LEFT JOIN fty_obj_attr AS prod_weight ON prod_weight.attr_sid = 'af37d15ade63f26ee566fcd9692c63d4' AND prod_weight.obj_sid = fty_product.sid"
}

type attrsFieldsTokenReplacer struct {
	Attrs map[string]string
}

func (atr attrsFieldsTokenReplacer) TokenReplace(ctx map[string]interface{}) string {
	return "prod_material.attr_value AS product_material,prod_weight.attr_value AS product_weight"
}

func TestSqlBuilder_RegisterToken(t *testing.T) {
	var sqlWithToken = `
info:
  name: example
  version: 1.0.0
composition:
  tokens:
    attrs:
      params:
        - name: prod-weight
          value: product_weight
        - name: prod-material
          value: product_material
    attrs_fields:
      params:
        - name: prod-weight
          value: product_weight
        - name: prod-material
          value: product_material
  fields:
    base:
      - name: name
        expr: users.name
      - name: age
        expr: users.age
    statistic:
      - name: consume_times
        expr: COUNT(orders.id)
      - name: consume_total
        expr: SUM(orders.total_amount)
  subject: 
    list: "SELECT %fields.base, %fields.statistic, %attrs_fields FROM users LEFT JOIN orders ON orders.uid = users.uid LEFT JOIN fty_product ON orders.product_sid = fty_product.sid %attrs %where GROUP BY users.uid %limit"
    total: "SELECT count(users.uid) FROM users LEFT JOIN orders ON orders.uid = users.uid LEFT JOIN fty_product ON orders.product_sid = fty_product.sid %attrs %where GROUP BY users.uid"`

	var sqlNotWithToken = `
info:
  name: example
  version: 1.0.0
composition:
  fields:
    base:
      - name: name
        expr: users.name
      - name: age
        expr: users.age
    statistic:
      - name: consume_times
        expr: COUNT(orders.id)
      - name: consume_total
        expr: SUM(orders.total_amount)
  subject: 
    list: "SELECT %fields.base, %fields.statistic FROM users LEFT JOIN orders ON orders.uid = users.uid LEFT JOIN fty_product ON orders.product_sid = fty_product.sid %where GROUP BY users.uid %limit"
    total: "SELECT count(users.uid) FROM users LEFT JOIN orders ON orders.uid = users.uid LEFT JOIN fty_product ON orders.product_sid = fty_product.sid %where GROUP BY users.uid"`

	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)

		sb, err := NewSqlBuilder(db, []byte(sqlWithToken))

		keys := make([]string, len(sb.Doc.Composition.Tokens))

		i := 0
		for k := range sb.Doc.Composition.Tokens {
			keys[i] = k
			i++
		}
		sort.Strings(keys)
		assert.Equal(t, []string{"attrs", "attrs_fields"}, keys)

		if err != nil {
			t.Fatal(err)
		}

		where, err := WhereAnd(&[]Filter{
			{Val: "Barry", Op: Contains, Attr: "users.name"},
		})

		if err != nil {
			t.Fatal(err)
		}

		q, _, err := sb.AndConditions(&where).Limit(0, 10).Rebind("list")

		assert.Error(t, err)

		sb.RegisterToken("attrs", func(params []TokenParam) TokenReplacer {
			attrs := map[string]string{}
			for _, p := range params {
				attrs[p.Name] = p.Value
			}

			return &attrsTokenReplacer{
				Attrs: attrs,
				DB:    db,
			}
		})

		sb.RegisterToken("attrs_fields", func(params []TokenParam) TokenReplacer {
			attrs := map[string]string{}
			for _, p := range params {
				attrs[p.Name] = p.Value
			}

			return &attrsFieldsTokenReplacer{
				Attrs: attrs,
			}
		})

		q, _, err = sb.Rebind("list")

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total, prod_material.attr_value AS product_material,"+
			"prod_weight.attr_value AS product_weight "+
			"FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"LEFT JOIN fty_product ON orders.product_sid = fty_product.sid "+
			"LEFT JOIN fty_obj_attr AS prod_material ON prod_material.attr_sid = '87c53961debe28ecaf55dfc5af1c9039' "+
			"AND prod_material.obj_sid = fty_product.sid LEFT JOIN fty_obj_attr AS prod_weight "+
			"ON prod_weight.attr_sid = 'af37d15ade63f26ee566fcd9692c63d4' AND prod_weight.obj_sid = fty_product.sid "+
			"WHERE (users.name LIKE ?) GROUP BY users.uid LIMIT 0, 10", q)

		q, _, err = sb.Rebind("total")

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "SELECT count(users.uid) "+
			"FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"LEFT JOIN fty_product ON orders.product_sid = fty_product.sid "+
			"LEFT JOIN fty_obj_attr AS prod_material ON prod_material.attr_sid = '87c53961debe28ecaf55dfc5af1c9039' "+
			"AND prod_material.obj_sid = fty_product.sid LEFT JOIN fty_obj_attr AS prod_weight "+
			"ON prod_weight.attr_sid = 'af37d15ade63f26ee566fcd9692c63d4' AND prod_weight.obj_sid = fty_product.sid "+
			"WHERE (users.name LIKE ?) GROUP BY users.uid", q)


		// register token but not in compose
		sb, err = NewSqlBuilder(db, []byte(sqlNotWithToken))

		sb.RegisterToken("attrs", func(params []TokenParam) TokenReplacer {
			attrs := map[string]string{}
			for _, p := range params {
				attrs[p.Name] = p.Value
			}

			return &attrsTokenReplacer{
				Attrs: attrs,
				DB:    db,
			}
		})

		sb.RegisterToken("attrs_fields", func(params []TokenParam) TokenReplacer {
			attrs := map[string]string{}
			for _, p := range params {
				attrs[p.Name] = p.Value
			}

			return &attrsFieldsTokenReplacer{
				Attrs: attrs,
			}
		})

		q, _, err = sb.Rebind("list")

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total "+
			"FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"LEFT JOIN fty_product ON orders.product_sid = fty_product.sid  "+
			"GROUP BY users.uid LIMIT 0, 10", q)
	})
}

func TestNewSqlBuilder_DefaultConditions(t *testing.T) {
	var sqlComposition = `
info:
  name: example
  version: 1.0.0
composition:
  fields:
    base:
      - name: name
        expr: users.name
      - name: age
        expr: users.age
      - name: order_status
        expr: orders.status
    statistic:
      - name: consume_times
        expr: COUNT(orders.id)
      - name: consume_total
        expr: SUM(orders.total_amount)
  defaultConditions:
    - attr: users.name
      op: contains
      val: Barry
    - attr: order_status
      op: in
      val: [4,5,6,8]
  subject: 
    list: "SELECT %fields.base, %fields.statistic FROM users LEFT JOIN orders ON orders.uid = users.uid %where GROUP BY users.uid %limit"
    total: "SELECT count(users.uid) FROM users LEFT JOIN order ON order.uid = users.uid %where GROUP BY users.uid"`

	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)

		sb, err := NewSqlBuilder(db, []byte(sqlComposition))

		if err != nil {
			t.Fatal(err)
		}

		q, _, err := sb.Limit(0, 10).Rebind("list")

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, orders.status AS order_status, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"WHERE users.name LIKE ? AND order_status IN(?, ?, ?, ?) GROUP BY users.uid LIMIT 0, 10", q)
	})
}

func TestSqlBuilder_AddFilters(t *testing.T) {
	var sqlComposition = `
info:
  name: example
  version: 1.0.0
composition:
  fields:
    base:
      - name: name
        expr: users.name
      - name: age
        expr: users.age
      - name: order_status
        expr: orders.status
    statistic:
      - name: consume_times
        expr: COUNT(orders.id)
      - name: consume_total
        expr: SUM(orders.total_amount)
  subject: 
    list: "SELECT %fields.base, %fields.statistic FROM users LEFT JOIN orders ON orders.uid = users.uid %where GROUP BY users.uid %limit"
    total: "SELECT count(users.uid) FROM users LEFT JOIN order ON order.uid = users.uid %where GROUP BY users.uid"`

	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)

		sb, err := NewSqlBuilder(db, []byte(sqlComposition))

		if err != nil {
			t.Fatal(err)
		}

		err = sb.AddFilters([]Filter{
			{Val: "barry", Op: Contains, Attr: "users.name"},
			{Val: []int{1, 2, 4, 8}, Op: In, Attr: "order_status"},
		}, OR)

		err = sb.AddFilters([]Filter{
			{Val: 10, Op: Greater, Attr: "users.age"},
		}, OR)

		if err != nil {
			t.Fatal(err)
		}

		q, _, err := sb.Rebind("list")

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, orders.status AS order_status, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"WHERE ((users.name LIKE ? OR order_status IN(?, ?, ?, ?))) AND (users.age > ?) GROUP BY users.uid LIMIT 0, 10", q)
	})
}

func TestSqlBuilder_RegisterFilterPipeline(t *testing.T) {
	var sqlComposition = `
info:
  name: example
  version: 1.0.0
composition:
  filterPipelines:
    attrs_fulltext:
      type: fulltext
      params:
        - name: fields
          value: 
            - product_spec
            - product_unit_weight
            - product_material
  fields:
    base:
      - name: name
        expr: users.name
      - name: age
        expr: users.age
      - name: order_status
        expr: orders.status
    statistic:
      - name: consume_times
        expr: COUNT(orders.id)
      - name: consume_total
        expr: SUM(orders.total_amount)
  subject: 
    list: "SELECT %fields.base, %fields.statistic FROM users LEFT JOIN orders ON orders.uid = users.uid %where GROUP BY users.uid %limit"
    total: "SELECT count(users.uid) FROM users LEFT JOIN order ON order.uid = users.uid %where GROUP BY users.uid"`

	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)

		sb, err := NewSqlBuilder(db, []byte(sqlComposition))

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, map[string]FilterPipelineDefinition{
			"attrs_fulltext": {
				Type: "fulltext",
				Params: []FilterPipelineParam{
					{Name: "fields", Value: []interface{}{"product_spec", "product_unit_weight", "product_material"}},
				},
			},
		}, sb.Doc.Composition.FilterPipelines)

		err = sb.RegisterPipelineType("fulltext")

		if err != nil {
			t.Fatal(err)
		}

		err = sb.AddFilters([]Filter{
			{Val: "barry", Op: Contains, Attr: "users.name"},
			{Val: []int{1, 2, 4, 8}, Op: In, Attr: "order_status"},
			{Val: "X11", Op: Contains, Attr: "attrs_fulltext"},
		}, AND)

		q, _, err := sb.Rebind("list")

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, orders.status AS order_status, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"WHERE ((product_spec LIKE ? OR product_unit_weight LIKE ? OR product_material LIKE ?) AND (users.name LIKE ? AND order_status IN(?, ?, ?, ?))) GROUP BY users.uid LIMIT 0, 10", q)
	})
}

func TestSqlBuilder_OrderBy(t *testing.T) {
	var sqlComposition = `
info:
  name: example
  version: 1.0.0
composition:
  fields:
    base:
      - name: name
        expr: users.name
        type: string
      - name: age
        expr: users.age
    statistic:
      - name: consume_times
        expr: COUNT(orders.id)
      - name: consume_total
        expr: SUM(orders.total_amount)
  subject: 
    list: "SELECT %fields.base, %fields.statistic FROM users LEFT JOIN orders ON orders.uid = users.uid %where GROUP BY users.uid %order_by %limit"
    total: "SELECT count(users.uid) FROM users LEFT JOIN order ON order.uid = users.uid %where{!consume_total} %having{consume_total} GROUP BY users.uid"`

	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)

		sb, err := NewSqlBuilder(db, []byte(sqlComposition))

		if err != nil {
			t.Fatal(err)
		}

		if err != nil {
			t.Fatal(err)
		}

		q, a, err := sb.OrderBy(&OrderBy{
			{Name: "age", Direction: ASC},
		}).Limit(0, 10).Rebind("list")

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total FROM users LEFT JOIN orders ON orders.uid = users.uid"+
			"  GROUP BY users.uid ORDER BY age ASC LIMIT 0, 10", q)

		rows, err := db.Queryx(q, a...)

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, true, rows.Next())
		row := make(map[string]interface{})
		err = rows.MapScan(row)

		if err != nil {
			t.Fatal(err)
		}

		sb.RowConvert(&row)

		assert.Equal(t, "Scott", row["name"])
		assert.Equal(t, int64(20), row["age"])
		assert.Equal(t, int64(2), row["consume_times"])
		assert.Equal(t, 192.89999999999998, row["consume_total"])
	})
}
