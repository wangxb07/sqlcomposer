package sqlcomposer

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

var db *sqlx.DB

type Schema struct {
	create string
	drop   string
}

func (s Schema) Sqlite3() (string, string) {
	return strings.Replace(s.create, `now()`, `CURRENT_TIMESTAMP`, -1), s.drop
}

func MultiExec(e sqlx.Execer, query string) {
	stmts := strings.Split(query, ";\n")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		_, err := e.Exec(s)
		if err != nil {
			fmt.Println(err, s)
		}
	}
}

func RunWithSchema(schema Schema, t *testing.T, test func(db *sqlx.DB, t *testing.T)) {
	runner := func(db *sqlx.DB, t *testing.T, create, drop string) {
		defer func() {
			MultiExec(db, drop)
		}()

		MultiExec(db, create)
		test(db, t)
	}
	create, drop := schema.Sqlite3()

	runner(db, t, create, drop)
}

var defaultSchema = Schema{
	create: `
CREATE TABLE fty_dictionary_type (
	sid text,
	code text,
	name text,
	status integer,
	description text,
	create_time timestamp default now(),
	update_time timestamp default now(),
	is_delete integer default 0,
	parent_code text
);
CREATE TABLE fty_obj_attr (
	sid text,
	attr_sid text,
	obj_sid text,
	attr_value text
);
CREATE TABLE fty_product (
	sid text,
	name text
);
CREATE TABLE users (
	uid integer,
	name text,
	age integer
);
CREATE TABLE orders (
	id integer,
	uid integer,
	order_no text,
	total_amount float,
	status integer,
    product_sid text
);
`,
	drop: `
drop table fty_dictionary_type;
drop table fty_obj_attr;
drop table fty_product;
drop table users;
drop table orders;
`,
}

func init() {
	sqdsn := os.Getenv("SQLITE_DSN")

	if sqdsn == "" {
		sqdsn = ":memory:"
	}
	db = sqlx.MustConnect("sqlite3", sqdsn)
}

func loadDefaultFixture(db *sqlx.DB, t *testing.T) {
	tx := db.MustBegin()
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('87c53961debe28ecaf55dfc5af1c9039','prod-material','产品材质',1,'材质','2019-11-04 11:01:35','2019-11-04 11:07:53',0)"))
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('af37d15ade63f26ee566fcd9692c63d4','prod-weight','产品单重',1,'单重（g）','2019-11-04 11:04:39','2019-12-14 00:20:31',0)"))
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('416d89cf8c45218b634dd3be2ffef0f8','prod-hardness','产品硬度',1,'硬度','2019-11-04 11:04:39','2019-12-14 00:20:31',0)"))
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('7fcba4153718271241b71a6bb941b454','prod-perform-level','产品性能等级',1,'性能等级','2019-11-04 11:04:39','2019-12-14 00:20:31',0)"))
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('ff6d4b42ce74bb424257e15f02f6ec63','prod-surface-treat','产品表面处理',1,'表面处理','2019-11-04 11:04:39','2019-12-14 00:20:31',0)"))

	tx.MustExec(tx.Rebind("INSERT INTO users (uid, name, age) VALUES (?, ?, ?)"), 1, "Scott", "20")
	tx.MustExec(tx.Rebind("INSERT INTO users (uid, name, age) VALUES (?, ?, ?)"), 2, "Barry", "24")
	tx.MustExec(tx.Rebind("INSERT INTO users (uid, name, age) VALUES (?, ?, ?)"), 3, "Zoe", "24")

	tx.MustExec(tx.Rebind("INSERT INTO orders (id, uid, order_no, total_amount) VALUES (?, ?, ?, ?)"), 1, 1, "001", 101.1)
	tx.MustExec(tx.Rebind("INSERT INTO orders (id, uid, order_no, total_amount) VALUES (?, ?, ?, ?)"), 2, 1, "002", 91.8)
	tx.MustExec(tx.Rebind("INSERT INTO orders (id, uid, order_no, total_amount) VALUES (?, ?, ?, ?)"), 3, 2, "003", 28.8)
	tx.MustExec(tx.Rebind("INSERT INTO orders (id, uid, order_no, total_amount) VALUES (?, ?, ?, ?)"), 4, 3, "004", 18.9)
	_ = tx.Commit()
}

func TestCombine(t *testing.T) {
	f1 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
	}
	f2 := &[]Filter{
		{Val: "barry", Op: Equal, Attr: "nickname"},
		{Val: 10, Op: Equal, Attr: "age"},
		{Val: []string{"pet", "movie"}, Op: In, Attr: "fav"},
	}

	s1, _ := WhereAnd(f1)
	s2, _ := WhereAnd(f2)

	combined := CombineOr(s1, s2)
	assert.Equal(t, "(name = :name) OR (nickname = :nickname AND age = :age AND fav IN(:fav))", combined.Clause)
	assert.Equal(t, map[string]interface{}{
		"nickname": "barry",
		"name":     "wang",
		"age":      10,
		"fav":      []string{"pet", "movie"},
	}, combined.Arg)

	assert.Equal(t, map[string]string{
		"name":     "name = :name",
		"nickname": "nickname = :nickname",
		"age":      "age = :age",
		"fav":      "fav IN(:fav)",
	}, combined.ClauseSlice)

	f3 := &[]Filter{
		{Val: []int{10, 15}, Op: Between, Attr: "age"},
		{Val: nil, Op: IsNotNull, Attr: "class"},
	}

	s3, _ := WhereAnd(f3)

	combined = CombineAnd(combined, s3)
	assert.Equal(t, "((name = :name) OR (nickname = :nickname AND age = :age AND fav IN(:fav))) AND (age >= :age_1 AND age <= :age_2 AND class IS NOT NULL)", combined.Clause)
	assert.Equal(t, map[string]interface{}{
		"nickname": "barry",
		"name":     "wang",
		"age":      10,
		"age_1":    int64(10),
		"age_2":    int64(15),
		"fav":      []string{"pet", "movie"},
	}, combined.Arg)
	assert.Equal(t, map[string]string{
		"name":     "name = :name",
		"nickname": "nickname = :nickname",
		"age":      "age = :age AND age >= :age_1 AND age <= :age_2",
		"class":    "class IS NOT NULL",
		"fav":      "fav IN(:fav)",
	}, combined.ClauseSlice)

	// Empty combine
	filterEmpty := &[]Filter{}

	s4, _ := WhereOr(filterEmpty)

	emptyCombined := CombineAnd(s3, s4)
	assert.Equal(t, "(age >= :age_1 AND age <= :age_2 AND class IS NOT NULL)", emptyCombined.Clause)
	assert.Equal(t, map[string]interface{}{
		"age_1": int64(10),
		"age_2": int64(15),
	}, emptyCombined.Arg)

	// repeated args key test
	s5, _ := WhereAnd(&[]Filter{
		{Val: "1", Op: NotEqual, Attr: "order_type"},
	})

	s6, _ := WhereAnd(&[]Filter{
		{Val: "2", Op: Equal, Attr: "order_type"},
	})

	ra := CombineAnd(s5, s6)
	assert.Equal(t, "(order_type <> :order_type) AND (order_type = :order_type_1)", ra.Clause)
	assert.Equal(t, map[string]interface{}{
		"order_type":   "1",
		"order_type_1": "2",
	}, ra.Arg)
	assert.Equal(t, map[string]string {
		"order_type":   "order_type <> :order_type AND order_type = :order_type_1",
	}, ra.ClauseSlice)

	// repeated args key and between test
	s7, _ := WhereAnd(&[]Filter{
		{Val: []int{10, 15}, Op: Between, Attr: "age"},
	})

	s8, _ := WhereAnd(&[]Filter{
		{Val: []int{15, 50}, Op: Between, Attr: "age"},
	})

	ra = CombineAnd(s7, s8)
	assert.Equal(t, "(age >= :age_1 AND age <= :age_2) AND (age >= :age_3 AND age <= :age_4)", ra.Clause)
	assert.Equal(t, map[string]interface{}{
		"age_1": int64(10),
		"age_2": int64(15),
		"age_3": int64(15),
		"age_4": int64(50),
	}, ra.Arg)
	assert.Equal(t, map[string]string {
		"age":   "age >= :age_1 AND age <= :age_2 AND age >= :age_3 AND age <= :age_4",
	}, ra.ClauseSlice)
}

func TestBuildWhereAnd(t *testing.T) {
	f0 := &[]Filter{
		{Val: "", Op: Equal, Attr: "name"},
	}

	s0, _ := WhereAnd(f0)

	assert.Equal(t, "name = :name", s0.Clause)
	assert.Equal(t, map[string]interface{}{"name": ""}, s0.Arg)

	f1 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "tb.name"},
	}

	s1, _ := WhereAnd(f1)

	assert.Equal(t, "tb.name = :tb_name", s1.Clause)
	assert.Equal(t, map[string]interface{}{"tb_name": "wang"}, s1.Arg)

	f2 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: 10, Op: Equal, Attr: "age"},
		{Val: []string{"pet", "movie"}, Op: In, Attr: "fav"},
	}

	s2, _ := WhereAnd(f2)
	assert.Equal(t, "name = :name AND age = :age AND fav IN(:fav)", s2.Clause)
	assert.Equal(t, map[string]interface{}{
		"name": "wang",
		"age":  10,
		"fav":  []string{"pet", "movie"},
	}, s2.Arg)

	f3 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: []int{10, 15}, Op: Between, Attr: "age"},
		{Val: nil, Op: IsNotNull, Attr: "class"},
	}

	s3, _ := WhereAnd(f3)
	assert.Equal(t, "name = :name AND age >= :age_1 AND age <= :age_2 AND class IS NOT NULL", s3.Clause)
	assert.Equal(t, map[string]interface{}{
		"name":  "wang",
		"age_1": int64(10),
		"age_2": int64(15),
	}, s3.Arg)

	f4 := &[]Filter{
		{Val: "xian", Op: Contains, Attr: "name"},
		{Val: "wang", Op: StartsWith, Attr: "nickname"},
		{Val: "bin", Op: EndsWith, Attr: "nickname"},
		{Val: "barry", Op: EndsWith, Attr: "firstName"},
	}

	s4, err := WhereAnd(f4)

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "name LIKE :name AND nickname LIKE :nickname AND nickname LIKE :nickname_1 AND firstName LIKE :firstName", s4.Clause)
	assert.Equal(t, map[string]interface{}{
		"name":       "%xian%",
		"nickname":   "wang%",
		"nickname_1": "%bin",
		"firstName":  "%barry",
	}, s4.Arg)
	assert.Equal(t, "name LIKE :name", s4.ClauseSlice["name"])
	assert.Equal(t, "nickname LIKE :nickname", s4.ClauseSlice["nickname"])
	assert.Equal(t, "nickname LIKE :nickname_1", s4.ClauseSlice["nickname_1"])
	assert.Equal(t, "firstName LIKE :firstName", s4.ClauseSlice["firstName"])

	f5 := &[]Filter{
		{Val: "中文", Op: Contains, Attr: "cust_name"},
	}

	s5, err := WhereAnd(f5)

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "cust_name LIKE :cust_name", s5.Clause)
	assert.Equal(t, map[string]interface{}{
		"cust_name": "%中文%",
	}, s5.Arg)

	s6, err := WhereAnd(&[]Filter{
		{Val: []interface{}{"2020-06-01", "2020-06-20"}, Op: Between, Attr: "lang"},
	})

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "lang >= :lang_1 AND lang <= :lang_2", s6.Clause)
	assert.Equal(t, map[string]interface{}{
		"lang_1": "2020-06-01",
		"lang_2": "2020-06-20",
	}, s6.Arg)

	s7, err := WhereAnd(&[]Filter{
		{Val: []interface{}{128, 200}, Op: Between, Attr: "lang"},
	})

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "lang >= :lang_1 AND lang <= :lang_2", s7.Clause)
	assert.Equal(t, map[string]interface{}{
		"lang_1": int64(128),
		"lang_2": int64(200),
	}, s7.Arg)

	s8, err := WhereAnd(&[]Filter{
		{Val: []interface{}{128.0, 200.1}, Op: Between, Attr: "lang"},
	})

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "lang >= :lang_1 AND lang <= :lang_2", s8.Clause)
	assert.Equal(t, map[string]interface{}{
		"lang_1": float64(128.0),
		"lang_2": float64(200.1),
	}, s8.Arg)

	s9, err := WhereAnd(&[]Filter{
		{Val: []interface{}{128, 200.1}, Op: Between, Attr: "height"},
		{Val: []interface{}{"1028", "2000"}, Op: Between, Attr: "lang"},
	})

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "height >= :height_1 AND height <= :height_2 AND lang >= :lang_1 AND lang <= :lang_2", s9.Clause)
	assert.Equal(t, map[string]interface{}{
		"height_1": int64(128),
		"height_2": float64(200.1),
		"lang_1":   "1028",
		"lang_2":   "2000",
	}, s9.Arg)
	assert.Equal(t, "height >= :height_1 AND height <= :height_2", s9.ClauseSlice["height"])
	assert.Equal(t, "lang >= :lang_1 AND lang <= :lang_2", s9.ClauseSlice["lang"])
}

func TestBuildWhereOr(t *testing.T) {
	fe := &[]Filter{
		{Val: "", Op: Equal, Attr: "fty_plan_package.batch_no"},
		{Val: "", Op: IsNull, Attr: "fty_plan_package.batch_no"},
	}

	se, _ := WhereOr(fe)
	assert.Equal(t, "fty_plan_package.batch_no = :fty_plan_package_batch_no OR fty_plan_package.batch_no IS NULL", se.Clause)

	f1 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
	}

	s1, _ := WhereOr(f1)

	assert.Equal(t, "name = :name", s1.Clause)
	assert.Equal(t, map[string]interface{}{"name": "wang"}, s1.Arg)

	f2 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: 10, Op: Equal, Attr: "age"},
		{Val: []string{"pet", "movie"}, Op: In, Attr: "fav"},
	}

	s2, _ := WhereOr(f2)
	assert.Equal(t, "name = :name OR age = :age OR fav IN(:fav)", s2.Clause)
	assert.Equal(t, map[string]interface{}{
		"name": "wang",
		"age":  10,
		"fav":  []string{"pet", "movie"},
	}, s2.Arg)

	f3 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: []int{10, 15}, Op: Between, Attr: "age"},
		{Val: nil, Op: IsNotNull, Attr: "class"},
	}

	s3, _ := WhereOr(f3)
	assert.Equal(t, "name = :name OR age >= :age_1 AND age <= :age_2 OR class IS NOT NULL", s3.Clause)
	assert.Equal(t, map[string]interface{}{
		"name":  "wang",
		"age_1": int64(10),
		"age_2": int64(15),
	}, s3.Arg)

	f4 := &[]Filter{
		{Val: "xian", Op: Contains, Attr: "name"},
		{Val: "wang", Op: StartsWith, Attr: "nickname"},
		{Val: "barry", Op: EndsWith, Attr: "firstName"},
	}

	s4, err := WhereOr(f4)

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "name LIKE :name OR nickname LIKE :nickname OR firstName LIKE :firstName", s4.Clause)
	assert.Equal(t, map[string]interface{}{
		"name":      "%xian%",
		"nickname":  "wang%",
		"firstName": "%barry",
	}, s4.Arg)

	f5 := &[]Filter{
		{Val: "中文", Op: Contains, Attr: "cust_name"},
	}

	s5, err := WhereOr(f5)

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "cust_name LIKE :cust_name", s5.Clause)
	assert.Equal(t, map[string]interface{}{
		"cust_name": "%中文%",
	}, s5.Arg)
}

func TestFilterToWhereAnd(t *testing.T) {
	p1 := FilterPipeline{
		Attr:      "name",
		CombineOp: AND,
		Expander: &FulltextSearchExpander{
			Fields: []string{
				"first_name",
				"nick_name",
			},
		},
	}

	f1 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: 10, Op: Equal, Attr: "age"},
		{Val: []string{"pet", "movie"}, Op: In, Attr: "fav"},
	}

	stmt, _ := FilterToWhereAnd(f1, p1)

	assert.Equal(t, "(first_name = :first_name OR nick_name = :nick_name) AND (age = :age AND fav IN(:fav))", stmt.Clause)
	assert.Equal(t, map[string]interface{}{
		"first_name": "wang",
		"nick_name":  "wang",
		"age":        10,
		"fav":        []string{"pet", "movie"},
	}, stmt.Arg)
}

func TestTokenReplace(t *testing.T) {
	str := "SELECT * FROM tb %foo %where %limit"

	where, _ := WhereAnd(&[]Filter{
		{Val: "中文", Op: Contains, Attr: "cust_name"},
	})

	ctx := map[string]interface{}{
		"where": where,
		"limit": SqlLimit{
			Offset: 0,
			Size:   10,
		},
		"foo": "LEFT JOIN ltb ON ltb.fid = tb.id",
	}

	r, _ := tokenReplace(str, ctx)
	assert.Equal(t, "SELECT * FROM tb LEFT JOIN ltb ON ltb.fid = tb.id WHERE cust_name LIKE :cust_name LIMIT 0, 10", r)
}

func Test_generateNewAttrName(t *testing.T) {
	args := map[string]interface{}{
		"order_type":   "1",
		"order_status": "2",
	}

	newName := generateNewAttrName("order_type", args)
	assert.Equal(t, "order_type_1", newName)

	args[newName] = "3"
	newName = generateNewAttrName("order_type", args)
	assert.Equal(t, "order_type_2", newName)

	args[newName] = "4"
	newName = generateNewAttrName("order_type", args)
	assert.Equal(t, "order_type_3", newName)

	newName = generateNewAttrName("order_type", args)
	assert.Equal(t, "order_type_3", newName)

	args = map[string]interface{}{
		"age_1": "1",
		"age_2": "2",
	}

	newName = generateNewAttrName("age_1", args)
	assert.Equal(t, "age_3", newName)
	args[newName] = "3"
	newName = generateNewAttrName("age_2", args)
	assert.Equal(t, "age_4", newName)
}

func TestCollectTokenPlaceholder(t *testing.T) {
	var (
		tks [][]string
		sc  string
	)

	sc = "SELECT * FROM tb %foo %where %limit"
	tks = CollectTokenPlaceholder(sc)

	assert.Equal(t, "%foo", tks[0][0])
	assert.Equal(t, "%where", tks[1][0])
	assert.Equal(t, "%limit", tks[2][0])

	// with suffix
	sc = "SELECT %fields.base FROM tb %where %limit"
	tks = CollectTokenPlaceholder(sc)

	assert.Equal(t, "%fields.base", tks[0][0])
	assert.Equal(t, "fields.base", tks[0][1])
	assert.Equal(t, "", tks[1][2])

	// with params
	sc = "SELECT %fields.base FROM tb %where{name,age} %limit"
	tks = CollectTokenPlaceholder(sc)
	assert.Equal(t, "%where{name,age}", tks[1][0])
	assert.Equal(t, "{name,age}", tks[1][2])

	sc = "SELECT %fields.base FROM tb %where{!name,age} %limit"
	tks = CollectTokenPlaceholder(sc)
	assert.Equal(t, "%where{!name,age}", tks[1][0])
	assert.Equal(t, "{!name,age}", tks[1][2])

	sc = "SELECT %fields.base FROM tb %where{*} %limit"
	tks = CollectTokenPlaceholder(sc)
	assert.Equal(t, "%where{*}", tks[1][0])
	assert.Equal(t, "{*}", tks[1][2])

	sc = "SELECT %fields.base FROM tb %where{!name,age} %having{name,age} %limit"
	tks = CollectTokenPlaceholder(sc)
	assert.Equal(t, "%where{!name,age}", tks[1][0])
	assert.Equal(t, "%having{name,age}", tks[2][0])
	assert.Equal(t, "{!name,age}", tks[1][2])
	assert.Equal(t, "{name,age}", tks[2][2])
}
