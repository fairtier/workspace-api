-- The platform-owned duckdb.yaml connector attaches the warehouse as `lk`
-- (no USE), so models must fully-qualify lk.<namespace>.<table>.
select * from lk.marts.agg_daily_zone
