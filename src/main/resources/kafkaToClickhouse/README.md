## kafka message format
```declarative
{
  "event_date": "2024-06-01",
  "content_id": 123456,
  "view_cnt": 100,
  "gift_cnt": 5
}
```

## clickhouse table DDL
```declarative
CREATE TABLE sp_dw_db.user_user
(
event_date Date,
content_id UInt64,
view_cnt UInt64,
gift_cnt UInt64
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_date, content_id);
```
