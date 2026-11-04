{# Filtro de cargas incrementales para la fila candidata (alias candidate). Carga las llaves nuevas, las filas cuya fecha de negocio cae en la ventana de reproceso (desde incremental_lookback_days antes de la fecha máxima ya cargada) y las filas cuyas llaves de dimensión resueltas cambiaron (tracked_columns), por ejemplo cuando una versión SCD2 nueva reemplaza a la anterior.
   candidates es el nombre del CTE del que sale candidate. La comparación con lo ya cargado usa una subconsulta NO correlacionada
   (IN con un join): Redshift no soporta subconsultas correlacionadas dentro de un OR. #}
{% macro incremental_candidate_filter(key_column, business_date_column, tracked_columns=[], candidates='candidates') %}
{% if is_incremental() %}
where candidate.{{ key_column }} not in (
    select existing.{{ key_column }} from {{ this }} as existing
)
or candidate.{{ business_date_column }} >= (
    select max(dates.date) - {{ var('incremental_lookback_days') }}
    from {{ this }} as existing
    inner join {{ ref('dim_date') }} as dates on dates.date_sk = existing.date_sk
)
{% if tracked_columns %}
or candidate.{{ key_column }} in (
    select existing.{{ key_column }}
    from {{ this }} as existing
    inner join {{ candidates }} as resolved on resolved.{{ key_column }} = existing.{{ key_column }}
    where {% for column in tracked_columns %}{{ is_distinct_from('existing.' ~ column, 'resolved.' ~ column) }}{{ "" if loop.last else " or " }}{% endfor %}
)
{% endif %}
{% endif %}
{% endmacro %}

{# Elimina de la tabla de hechos las filas cuya llave de origen ya no existe en la capa validada. #}
{% macro delete_orphaned_facts(sk_column, source_relation, source_key) %}
delete from {{ this }}
where {{ sk_column }} not in (
    select {{ dbt_utils.generate_surrogate_key(['cast(' ~ source_key ~ ' as varchar)']) }}
    from {{ source_relation }}
)
{% endmacro %}
