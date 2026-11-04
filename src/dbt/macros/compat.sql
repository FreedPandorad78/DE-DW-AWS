{# Funciones que cambian entre motores. El proyecto corre en Redshift (producción) y en DuckDB (pruebas locales). #}

{# Hora local de negocio de un timestamptz. AT TIME ZONE devuelve un timestamp sin zona en ambos motores. #}
{% macro local_timestamp(expression) -%}
({{ expression }} at time zone '{{ var("business_timezone") }}')
{%- endmacro %}

{# Texto de un campo anidado de un documento JSON guardado como varchar. Ejemplo: json_text('document_json', ['verification', 'purchase_confirmed']) #}
{% macro json_text(column, path_parts) -%}
{{ return(adapter.dispatch('json_text', 'ecommerce_dw')(column, path_parts)) }}
{%- endmacro %}

{% macro default__json_text(column, path_parts) -%}
json_extract_string({{ column }}, '$.{{ path_parts | join(".") }}')
{%- endmacro %}

{% macro redshift__json_text(column, path_parts) -%}
json_extract_path_text({{ column }}, '{{ path_parts | join("', '") }}')
{%- endmacro %}

{# Comparación segura ante nulos. Redshift documenta IS DISTINCT FROM como no soportado. #}
{% macro is_distinct_from(left, right) -%}
({{ left }} <> {{ right }} or ({{ left }} is null and {{ right }} is not null) or ({{ left }} is not null and {{ right }} is null))
{%- endmacro %}

{# Día de la semana ISO (1 = lunes ... 7 = domingo) usando dow (0 = domingo), que existe en ambos motores. #}
{% macro iso_day_of_week(date_expression) -%}
(case when extract(dow from {{ date_expression }}) = 0 then 7 else cast(extract(dow from {{ date_expression }}) as integer) end)
{%- endmacro %}

{# Última versión de cada llave entre los lotes acumulados en raw: el lote más reciente gana. #}
{% macro latest_batch_version(relation, key_column) -%}
select *
from (
    select
        raw_rows.*,
        row_number() over (
            partition by {{ key_column }}
            order by _batch_id desc, _loaded_at desc
        ) as _batch_rank
    from {{ relation }} as raw_rows
) as ranked_rows
where _batch_rank = 1
{%- endmacro %}

{# Texto a booleano. Redshift no permite CAST directo de texto a boolean ("cannot cast type text to boolean"). #}
{% macro to_boolean(expression) -%}
(case
    when lower(cast({{ expression }} as varchar)) in ('true', 't', '1') then true
    when lower(cast({{ expression }} as varchar)) in ('false', 'f', '0') then false
end)
{%- endmacro %}
