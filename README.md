# Argentina Ideal

## ¿De qué se trata?

Una empresa distribuidora "Argentina Ideal" necesita análisis financiero y operativo urgente ante un escenario macroeconómico crítico. El proyecto cubre desde la ingesta de datos hasta dashboards ejecutivos, todo dentro de Google Cloud Platform.

## Arquitectura

```
GCS Bucket (CSVs crudos)
    └── Script ETL (Python / Google Colab)
            └── BigQuery (dataset semi_raw)
                    ├── Venta
                    ├── Stock
                    └── Cliente
                            └── Looker Studio (dashboards)
```

## ¿Qué hace el ETL?

- Lista los archivos CSV desde un bucket de GCS organizado por carpetas de distribuidor
- Clasifica los archivos automáticamente por tipo (`venta`, `stock`, `cliente/deuda`)
- Carga cada archivo en la tabla BigQuery correspondiente con schema tipado
- Registra los archivos procesados en una tabla de metadatos para evitar reprocesamiento en sucesivas ejecuciones
- Maneja errores por archivo sin interrumpir el pipeline completo

## Análisis construidos

1. **Deuda vencida por cliente** — activos e inactivos por separado, agregado por distribuidora, visualizado como gráfico de barras apiladas
2. **Ventas en contado efectivo con deuda pendiente** — identifica clientes que requieren gestión de cobro, con montos totales
3. **Capacidad máxima de stock por distribuidor** — calcula los picos históricos de stock por SKU para proyectar targets de producción (+20% de reserva)

## Stack

- Python (google-cloud-bigquery, google-cloud-storage)
- Google Cloud Storage
- BigQuery
- Looker Studio
- Google Colab

## Nota

Los dashboards y reportes de Looker se construyeron dentro del entorno de GCP y no están incluidos en este repo. El script de ETL y las definiciones de schema son totalmente reproducibles con acceso a un proyecto de GCP con la misma estructura de bucket.
