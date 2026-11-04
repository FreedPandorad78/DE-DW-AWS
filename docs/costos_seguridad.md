# Estimación de costos y controles de seguridad

## Costos (región us-east-2; precios de referencia, confirmar en la calculadora de AWS)

Es una **estimación**. La parte por hora depende de cuánto tiempo estuvo encendida la infraestructura; ajusta la columna de horas con los tiempos reales.

| Concepto | Precio de referencia | Supuesto | Costo (USD) |
|---|---|---|---|
| Redshift `ra3.large` (1 nodo) | 0,543 por hora, mientras no esté pausado | 2,5 horas encendido | 1,36 |
| DocumentDB serverless (0,5 DCU) | ≈ 0,041 por hora | 2,5 horas | 0,10 |
| RDS MySQL `db.t3.micro` | ≈ 0,017 por hora | 2,5 horas | 0,04 |
| EC2 `t3.medium` | 0,0416 por hora | 2,5 horas | 0,10 |
| IPv4 pública de la EC2 | 0,005 por hora | 2,5 horas | 0,01 |
| Disco EBS gp3 de 30 GB | 0,08 por GB al mes | 3 días | 0,24 |
| Almacenamiento de RDS (20 GB gp3) | ≈ 0,115 por GB al mes | 3 días | 0,23 |
| Secrets Manager | 0,40 por secreto al mes | 3 secretos, 3 días | 0,12 |
| S3, Redshift (almacenamiento) y transferencia | céntimos | menos de 0,1 GB | 0,01 |
| **Total estimado** | | | **≈ 2,2** |

Con todo encendido el costo es de unos USD 0,65 por hora (≈ USD 16 por día si se olvida). Pausado o detenido, solo se cobran los discos y los secretos.

### Qué se hizo para controlarlo

- `python infra/desplegar_dw.py --plan` muestra el costo por hora antes de crear nada.
- `./infra/energia_dw.sh pausar` pausa Redshift y detiene la EC2, RDS y DocumentDB entre sesiones.
- Redshift **aprovisionado** y no Serverless: su costo es predecible (ver `decisiones_tecnicas.md`).
- Servicios del tamaño mínimo (`db.t3.micro`, DocumentDB de 0,5 DCU, un solo nodo) y datos pequeños.
- `python infra/destruir_dw.py` elimina todo y verifica que no queda nada que siga cobrando.

## Seguridad: controles implementados (con evidencia en `evidencias/dw/seguridad_evidencia.txt`)

| Control | Cómo se cumple |
|---|---|
| Sin servicios de datos públicos | RDS, DocumentDB y Redshift con `PubliclyAccessible = false`; solo la EC2 los alcanza (grupo de seguridad por origen) |
| Acceso a la EC2 | SSH solo desde una IP (/32); la interfaz de Airflow escucha solo en `127.0.0.1` y se abre con túnel SSH |
| Secretos | Contraseñas aleatorias en Secrets Manager; ninguna en el repositorio ni en el `.env` de la EC2; dbt las recibe por variable de entorno en cada ejecución |
| Credenciales de AWS | Ninguna clave en la EC2: se usa el rol de la instancia con permisos mínimos, sin permisos sobre `*` |
| Metadatos de la instancia | IMDSv2 obligatorio, con límite de saltos 2 para los contenedores |
| Cifrado | RDS, DocumentDB, Redshift y discos de la EC2 cifrados; bucket con AES256; TLS en las conexiones a RDS y DocumentDB |
| Bucket | Privado, con los cuatro bloqueos de acceso público |
| Permisos de despliegue | La política de `user_cli` está limitada a la región, a recursos con prefijo `taller-dw` y a `PassRole` solo de los roles del proyecto |
| Repositorio | `docker/.env.example` solo trae marcadores; la búsqueda de llaves de acceso y claves privadas no encuentra nada |
