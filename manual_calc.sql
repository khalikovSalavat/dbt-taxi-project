with month_agg as (
  select
    taxi_id,
    format_date('%Y%m', trip_start_timestamp) as year_month,
    cast(date_trunc(trip_start_timestamp, month) as date) as month_start,
    sum(tips) as tips_sum
  from `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  where format_date('%Y%m', trip_start_timestamp) >= '201804'
  group by
    taxi_id,
    format_date('%Y%m', trip_start_timestamp),
    cast(date_trunc(trip_start_timestamp, month) as date)
),
ranks as (
  select distinct
    taxi_id,
    year_month,
    month_start,
    tips_sum,
    row_number() over (partition by year_month order by tips_sum desc, taxi_id) as top_num
  from month_agg
),
top_ranks as (
  select *, format_date('%Y%m', date_add(month_start, interval -1 month)) as prev_year_month
  from ranks
  where top_num <= 3
)
select
  cur.year_month,
  cur.taxi_id,
  cur.tips_sum,
  round(cur.tips_sum / prev.tips_sum * 100 - 100, 2) as tips_change
from top_ranks as cur
left join top_ranks as prev
  on cur.prev_year_month = prev.year_month
  and cur.top_num = prev.top_num
order by cur.year_month, cur.top_num, cur.taxi_id
;