{{
    config(
        alias='monthly_driver_tips',
        database='khalikov-chicago-taxi-trips',
        materialized='incremental',
        unique_key="concat('year_month', '_', 'taxi_id')",
        partition_by={
            'field': 'year_month',
            'data_type': 'string'
        },
        pre_hook="
            delete from {{ this }}
            where year_month = format_date('%Y%m', current_date);
        "
    )
}}

select
  taxi_id,
  format_date('%Y%m', trip_start_timestamp) as year_month,
  date_trunc(trip_start_timestamp, month) as month_start,
  sum(tips) as tips_sum
from {{ source('bigquery', 'taxi_trips') }}
where format_date('%Y%m', trip_start_timestamp) = format_date('%Y%m', current_date)
group by
  taxi_id,
  format_date('%Y%m', trip_start_timestamp),
  date_trunc(trip_start_timestamp, month)
