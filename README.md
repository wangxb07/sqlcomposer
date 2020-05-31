# Sqlcomposer
Build SQL by YAML configure

# Features
Base on sqlx
Support filter pipeline
Support custom tokens
Fast build a service for sql base analysis

#Examples

```yaml

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

```

``` golang
sb, err := NewSqlBuilder(db, []byte(sqlComposition))
if err != nil {
    t.Fatal(err)
}

err = sb.AddFilters([]Filter{
    {Val: "barry", Op: Contains, Attr: "users.name"},
		{Val: []int{1, 2, 4, 8}, Op: In, Attr: "order_status"},
}, AND)

q, args, err := sb.Rebind("list")

rows, err := db.Queryx(q, args)
...
```
