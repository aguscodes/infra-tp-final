from google.cloud import bigquery
from google.cloud import storage
import os
from datetime import datetime


# Configuración inicial
PROJECT_ID = "usm-infra-grupo9"
BUCKET_NAME = "argentina_ideal"
FOLDER_PATH = "Distribuidor_001/"
DATASET_ID = "semi_raw"

# Tablas de destino
TABLAS_DESTINO = {
    'venta': f"{PROJECT_ID}.{DATASET_ID}.Venta",
    'stock': f"{PROJECT_ID}.{DATASET_ID}.Stock",
    'cliente': f"{PROJECT_ID}.{DATASET_ID}.Cliente"
}

# Inicializar clientes
client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client()

# Definición de funciones (deben estar ANTES de ser llamadas)

def list_csv_files(bucket_name, folder_path):
    """Lista todos los archivos CSV en el bucket y carpeta especificados"""
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path)
    return [blob.name for blob in blobs if blob.name.lower().endswith('.csv')]

def classify_files(csv_files):
    """Clasifica los archivos CSV en tres categorías: venta, stock, cliente"""
    classified = {
        'venta': [],
        'stock': [],
        'cliente': []
    }
    
    for file_path in csv_files:
        filename = os.path.basename(file_path).lower()
        
        if filename.startswith('venta'):
            classified['venta'].append(file_path)
        elif filename.startswith('stock'):
            classified['stock'].append(file_path)
        elif filename.startswith('cliente') or filename.startswith('deuda'):
            classified['cliente'].append(file_path)
    
    return classified

def get_processed_files(table_id):
    """Obtiene lista de archivos ya procesados desde metadatos de BigQuery"""
    query = f"""
    SELECT DISTINCT source_file
    FROM `{table_id}_metadata`
    """
    try:
        query_job = client.query(query)
        return {row.source_file for row in query_job}
    except Exception:
        return set()

def log_processed_file(table_id, file_path, rows_loaded):
    """Registra archivo procesado en tabla de metadatos"""
    metadata_table = f"{table_id}_metadata"
    
    schema = [
        bigquery.SchemaField("source_file", "STRING"),
        bigquery.SchemaField("process_date", "TIMESTAMP"),
        bigquery.SchemaField("rows_loaded", "INTEGER")
    ]
    
    try:
        client.get_table(metadata_table)
    except Exception:
        table = bigquery.Table(metadata_table, schema=schema)
        client.create_table(table)
    
    rows_to_insert = [{
        "source_file": file_path,
        "process_date": datetime.utcnow().isoformat(),
        "rows_loaded": rows_loaded
    }]
    
    errors = client.insert_rows_json(metadata_table, rows_to_insert)
    if errors:
        print(f"Error registrando metadatos: {errors}")

def load_files_to_table(file_list, table_id, table_schema):
    """Carga múltiples archivos CSV a una tabla BigQuery"""
    if not file_list:
        print(f"No hay archivos nuevos para cargar en {table_id}")
        return
    
    processed_files = get_processed_files(table_id)
    new_files = [f for f in file_list if f not in processed_files]
    
    if not new_files:
        print(f"Todos los archivos ya fueron procesados para {table_id}")
        return
    
    print(f"\nCargando {len(new_files)} archivos a {table_id}")
    
    for file_path in new_files:
        gcs_uri = f"gs://{BUCKET_NAME}/{file_path}"
        print(f"Procesando: {file_path}")
        
        try:
            job_config = bigquery.LoadJobConfig(
                schema=table_schema,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition='WRITE_APPEND'
            )
            
            load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
            load_job.result()
            
            log_processed_file(table_id, file_path, load_job.output_rows)
            print(f"Loaded {load_job.output_rows} rows from {file_path} to {table_id}")
            
        except Exception as e:
            print(f"Error al procesar {file_path}: {str(e)}")

# Esquemas para cada tipo de tabla
SCHEMAS = {
    'venta': [
        bigquery.SchemaField("codigo_sucursal", "INTEGER"),
        bigquery.SchemaField("codigo_cliente", "INTEGER"),
        bigquery.SchemaField("fecha_cierre_comercial", "DATE"),
        bigquery.SchemaField("SKU_codigo", "STRING"),
        bigquery.SchemaField("venta_unidades", "INTEGER"),
        bigquery.SchemaField("venta_importe", "FLOAT"),
        bigquery.SchemaField("condicion_venta", "STRING")
    ],
    'stock': [
        bigquery.SchemaField("codigo_sucursal", "INTEGER"),
        bigquery.SchemaField("fecha_cierre_comercial", "DATE"),
        bigquery.SchemaField("SKU_codigo", "STRING"),
        bigquery.SchemaField("SKU_descripcion", "STRING"),
        bigquery.SchemaField("Stock_unidades", "INTEGER"),
        bigquery.SchemaField("unidad", "STRING")
    ],
    'cliente': [
        bigquery.SchemaField("codigo_sucursal", "INTEGER"),
        bigquery.SchemaField("codigo_cliente", "INTEGER"),
        bigquery.SchemaField("ciudad", "STRING"),
        bigquery.SchemaField("provincia", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        bigquery.SchemaField("nombre_cliente", "STRING"),
        bigquery.SchemaField("cuit", "STRING"),
        bigquery.SchemaField("razon_social", "STRING"),
        bigquery.SchemaField("direccion", "STRING"),
        bigquery.SchemaField("dias_visita", "STRING"),
        bigquery.SchemaField("telefono", "STRING"),
        bigquery.SchemaField("fecha_alta", "DATE"),
        bigquery.SchemaField("fecha_baja", "STRING"),
        bigquery.SchemaField("lat", "FLOAT"),
        bigquery.SchemaField("long", "FLOAT"),
        bigquery.SchemaField("condicion_venta", "STRING"),
        bigquery.SchemaField("deuda_vencida", "FLOAT"),
        bigquery.SchemaField("tipo_negocio", "STRING")
    ]
}

# Bloque principal de ejecución
if __name__ == '__main__':
    # Paso 1: Listar todos los archivos CSV
    csv_files = list_csv_files(BUCKET_NAME, FOLDER_PATH)
    
    if not csv_files:
        print(f"No se encontraron archivos CSV en gs://{BUCKET_NAME}/{FOLDER_PATH}")
    else:
        # Paso 2: Clasificar los archivos
        classified_files = classify_files(csv_files)
        
        # Paso 3: Procesar cada categoría
        for file_type, files in classified_files.items():
            if files:
                print(f"\nProcesando archivos de tipo {file_type.upper()}:")
                load_files_to_table(
                    file_list=files,
                    table_id=TABLAS_DESTINO[file_type],
                    table_schema=SCHEMAS[file_type]
                )
            else:
                print(f"No se encontraron archivos de tipo {file_type}")
