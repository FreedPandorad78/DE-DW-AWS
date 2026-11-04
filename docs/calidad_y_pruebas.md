# Calidad de datos y pruebas

## Reglas de calidad (capa intermedia)

Cada entidad pasa por `stg_*` (tipada, última versión por llave), `int_*__qa` (con `rejection_reason`) e `int_*__valid` (sin rechazados). Lo rechazado va a `quarantine` con su motivo y su lote de origen.

| Entidad | Motivos de rechazo |
|---|---|
| Clientes | `missing_name`, `invalid_email` |
| Productos | `missing_category`, `negative_price` |
| Pedidos | `orphan_customer`, `invalid_sales_channel`, `negative_total` |
| Líneas de pedido | `orphan_order`, `orphan_product`, `invalid_quantity`, `subtotal_mismatch` |
| Reseñas | `invalid_rating`, `missing_reviewed_at`, `orphan_customer`, `orphan_product` |
| Eventos de navegación | `invalid_event_type`, `missing_occurred_at`, `orphan_product`, `orphan_customer`, `invalid_sales_channel` |

## Pruebas (92 en total, todas en verde en Redshift)

- **Unicidad y campos obligatorios** de las llaves de staging, de las capas válidas, de las dimensiones y los hechos.
- **Valores aceptados** de `rejection_reason`.
- **Relaciones** entre hechos y dimensiones.
- **Reglas de negocio** (pruebas singulares): `dim_date` cubre exactamente el rango configurado; cada año tiene festivos; las fechas de las reseñas y eventos son el día local de Bogotá del documento original; la fecha de cada venta coincide con la del pedido en `raw`; el subtotal por fecha de negocio de `fct_sales` coincide con el origen validado.

## Conciliación de un indicador

`6_pruebas_calidad` compara cada hecho con su capa válida (filas, total y llaves duplicadas) y el subtotal de ventas por fecha. Además, `evidencias/dw/indicador_ventas.txt` concilia **ventas y líneas por mes** entre el origen válido y el modelo analítico con la consulta SQL incluida.

## Escenarios (sección 3.5 del enunciado)

| Escenario | Evidencia |
|---|---|
| Carga inicial | `evidencias/escenarios_dw/01_carga_inicial.txt` |
| Registros defectuosos | `evidencias/escenarios_dw/02_registros_defectuosos.txt` |
| Repetición de un lote exitoso | `evidencias/escenarios_dw/03_repeticion_lote_exitoso.txt` |
| Nuevo lote incremental | `evidencias/escenarios_dw/04_nuevo_lote_incremental.txt` |
| Cambio en un atributo historizado (SCD2) | `evidencias/escenarios_dw/05_cambio_atributo_historizado.txt` |
| Fallo controlado y reejecución | `06a_fallo_carga_redshift.txt` y `06b_fallo_dbt_marts.txt` |
