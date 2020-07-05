package sqlcomposer

import (
	"reflect"
	"testing"
)

func Test_tokenReplace(t *testing.T) {
	w1, _ := WhereAnd(&[]Filter{
		{Val: "中文", Op: Contains, Attr: "cust_name"},
	})

	w2, _ := WhereAnd(&[]Filter{
		{Val: []interface{}{128, 200.1}, Op: Between, Attr: "height"},
		{Val: []interface{}{"1028", "2000"}, Op: Between, Attr: "lang"},
	})

	w3, _ := WhereAnd(&[]Filter{
	})

	w4, _ := WhereAnd(&[]Filter{
		{Val: []interface{}{128, 200.1}, Op: Between, Attr: "height"},
	})

	w5, _ := WhereAnd(&[]Filter{
		{Val: []interface{}{128, 200.1}, Op: Between, Attr: "height"},
		{Val: []interface{}{"1028", "2000"}, Op: Between, Attr: "lang"},
		{Val: "wang", Op: Equal, Attr: "name"},
	})

	type args struct {
		s   string
		ctx map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantRs  string
		wantErr bool
	}{
		{
			name: "test simple 1",
			args: args{
				s: "SELECT * FROM tb %foo %where %limit",
				ctx: map[string]interface{}{
					"where": w1,
					"limit": SqlLimit{
						Offset: 0,
						Size:   10,
					},
					"foo": "LEFT JOIN ltb ON ltb.fid = tb.id",
				},
			},
			wantRs: "SELECT * FROM tb LEFT JOIN ltb ON ltb.fid = tb.id WHERE cust_name LIKE :cust_name LIMIT 0, 10",
		},
		{
			name: "test ParameterizedTokenReplacer NOT",
			args: args{
				s: "SELECT * FROM tb %where{!height} %limit",
				ctx: map[string]interface{}{
					"where": w2,
					"limit": SqlLimit{
						Offset: 0,
						Size:   10,
					},
				},
			},
			wantRs: "SELECT * FROM tb WHERE lang >= :lang_1 AND lang <= :lang_2 LIMIT 0, 10",
		},
		{
			name: "test ParameterizedTokenReplacer NORMAL",
			args: args{
				s: "SELECT * FROM tb %where{height}",
				ctx: map[string]interface{}{
					"where": w2,
				},
			},
			wantRs: "SELECT * FROM tb WHERE height >= :height_1 AND height <= :height_2",
		},
		{
			name: "test ParameterizedTokenReplacer ALL",
			args: args{
				s: "SELECT * FROM tb %where{*}",
				ctx: map[string]interface{}{
					"where": w2,
				},
			},
			wantRs: "SELECT * FROM tb WHERE height >= :height_1 AND height <= :height_2 AND lang >= :lang_1 AND lang <= :lang_2",
		},
		{
			name: "test ParameterizedTokenReplacer NORMAL ALL",
			args: args{
				s: "SELECT * FROM tb %where{height,lang}",
				ctx: map[string]interface{}{
					"where": w2,
				},
			},
			wantRs: "SELECT * FROM tb WHERE height >= :height_1 AND height <= :height_2 AND lang >= :lang_1 AND lang <= :lang_2",
		},
		{
			name: "test ParameterizedTokenReplacer with having",
			args: args{
				s: "SELECT *, count(id) as lang FROM tb %where{!lang} %having{lang}",
				ctx: map[string]interface{}{
					"where": w2,
					"having": w2,
				},
			},
			wantRs: "SELECT *, count(id) as lang FROM tb WHERE height >= :height_1 AND height <= :height_2 HAVING lang >= :lang_1 AND lang <= :lang_2",
		},{
			name: "test ParameterizedTokenReplacer with having no condition",
			args: args{
				s: "SELECT *, count(id) as lang FROM tb %where{!lang} %having{lang}",
				ctx: map[string]interface{}{
					"where": w3,
					"having": w3,
				},
			},
			wantRs: "SELECT *, count(id) as lang FROM tb",
		},{
			name: "test ParameterizedTokenReplacer with having no lang condition",
			args: args{
				s: "SELECT *, count(id) as lang FROM tb %where{!lang} %having{lang}",
				ctx: map[string]interface{}{
					"where": w4,
					"having": w4,
				},
			},
			wantRs: "SELECT *, count(id) as lang FROM tb WHERE height >= :height_1 AND height <= :height_2",
		},{
			name: "test ParameterizedTokenReplacer with two excluded",
			args: args{
				s: "SELECT *, count(id) as lang FROM tb %where{!lang,name} %having{lang,name}",
				ctx: map[string]interface{}{
					"where": w5,
					"having": w5,
				},
			},
			wantRs: "SELECT *, count(id) as lang FROM tb WHERE height >= :height_1 AND height <= :height_2 HAVING lang >= :lang_1 AND lang <= :lang_2 AND name = :name",
		},{
			name: "test token value include token",
			args: args{
				s: "SELECT %fields FROM tb %where{!lang,name} %having{lang,name}",
				ctx: map[string]interface{}{
					"where": w5,
					"having": w5,
					"fields": "name, %other_fields",
					"other_fields": "age, sex, %counts",
					"counts": "count(id) as lang",
				},
			},
			wantRs: "SELECT name, age, sex, count(id) as lang FROM tb WHERE height >= :height_1 AND height <= :height_2 HAVING lang >= :lang_1 AND lang <= :lang_2 AND name = :name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRs, err := tokenReplace(tt.args.s, tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("tokenReplace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotRs != tt.wantRs {
				t.Errorf("tokenReplace() = %v, want %v", gotRs, tt.wantRs)
			}
		})
	}
}

func Test_processConditionsParameters(t *testing.T) {
	type args struct {
		p string
	}
	tests := []struct {
		name        string
		args        args
		wantInclude bool
		wantFields  []string
	}{
		{name: "NOT include", args: args{"!height"}, wantInclude: false, wantFields: []string{"height"}},
		{name: "include all", args: args{"*"}, wantInclude: true, wantFields: []string{}},
		{name: "include normal", args: args{"height,foo,bar"}, wantInclude: true, wantFields: []string{"height", "foo", "bar"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotInclude, gotFields := processConditionsParameters(tt.args.p)
			if gotInclude != tt.wantInclude {
				t.Errorf("processConditionsParameters() gotInclude = %v, want %v", gotInclude, tt.wantInclude)
			}
			if !reflect.DeepEqual(gotFields, tt.wantFields) {
				t.Errorf("processConditionsParameters() gotFields = %v, want %v", gotFields, tt.wantFields)
			}
		})
	}
}
