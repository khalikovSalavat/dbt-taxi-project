{{
    config(
        alias='monthly_driver_tips_top',
        database='khalikov-chicago-taxi-trips',
        materialized='incremental',
        unique_key="concat('year_month', '_', 'taxi_id')",
        pre_hook="
            delete from {{ this }}
            where year_month = format_date('%Y%m', current_date);
        "
    )
}}

-- динамика tips_change рассчитывается относительно рейтинга по сумме чаевых
-- т.е. сравнивается наибольшая сумма за предыдущий месяц с наибольшей за текущий (без учета taxi_id)

with current_top as (
  select distinct
    year_month,
    month_start,
    taxi_id,
    tips_sum,
    row_number() over ( --возможно использование dense_rank в зависимости от требований
      order by tips_sum desc, taxi_id
    ) as top_num
  from {{ ref('monthly_driver_tips') }}
  where year_month = format_date('%Y%m', current_date)
)
{% if is_incremental() %}
  , prev_top as (
    select
      year_month,
      taxi_id,
      tips_sum,
      top_num
    from {{ this }}
    where month_start = format_date(date_add(current_date, interval -1 month))
  )
  select
    cur.taxi_id,
    cur.year_month,
    cur.month_start,
    cur.tips_sum,
    round(cur.tips_sum / prev.tips_sum * 100 - 100, 2) as tips_change
  from current_top as cur
  left join prev_top prev
    on cur.year_month = prev.year_month
    and cur.top_num = prev.top_num
  where cur.top_num <= 3
{% else %}
  select
    cur.taxi_id,
    cur.year_month,
    cur.month_start,
    cur.tips_sum,
    null as tips_change
  from current_top as cur
  where cur.top_num <= 3
{% endif %}
