-- raw acumula los lotes extraídos: aquí queda la última versión de cada llave.
with latest as (
    {{ latest_batch_version(source('mysql_raw', 'mysql_customers'), 'customer_id') }}
)

select
    cast(customer_id as bigint) as customer_id,
    nullif(trim(name), '') as name,
    nullif(lower(trim(email)), '') as email,
    nullif(trim(phone), '') as phone,
    nullif(trim(city), '') as city,
    nullif(trim(department), '') as department,
    cast(registration_date as date) as registration_date,
    cast(updated_at as timestamp) as updated_at,
    _batch_id,
    _loaded_at,
    _source
from latest
