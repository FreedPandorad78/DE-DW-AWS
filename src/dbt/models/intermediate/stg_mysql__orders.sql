-- raw acumula los lotes extraídos: aquí queda la última versión de cada llave.
with latest as (
    {{ latest_batch_version(source('mysql_raw', 'mysql_orders'), 'order_id') }}
)

select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(order_date as timestamp) as order_date,
    nullif(trim(status), '') as status,
    nullif(trim(sales_channel), '') as sales_channel,
    nullif(trim(payment_method), '') as payment_method,
    cast(total as decimal(14, 2)) as total,
    _batch_id,
    _loaded_at,
    _source
from latest
