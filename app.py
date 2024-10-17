from flask import Flask, request, jsonify
import pandas as pd
from google.cloud import bigquery
import logging

app = Flask(__name__)

# Configuración de BigQuery
PROJECT_ID = 'coral-velocity-438815-v4'
DATASET_ID = 'stg_rrhh'
BUCKET_NAME = 'pe-archivos-sensibles-2024'

# Configura el logging
logging.basicConfig(level=logging.INFO, filename='error.log',
                    format='%(asctime)s %(levelname)s:%(message)s')

# Definición de las estructuras
SCHEMAS = {
    'hired_employees.csv': [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("datetime", "STRING"),
        bigquery.SchemaField("department_id", "INTEGER"),
        bigquery.SchemaField("job_id", "INTEGER"),
    ],
    'departments.csv': [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("department", "STRING"),
    ],
    'jobs.csv': [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("job", "STRING"),
    ],
}

# Inicializa el cliente de BigQuery
client = bigquery.Client(project=PROJECT_ID)

def validate_data(df, schema):
    """Valida si el DataFrame cumple con la estructura requerida."""
    required_columns = [field.name for field in schema]
    if not all(col in df.columns for col in required_columns):
        return False
    if df.isnull().values.any():
        return False
    return True

@app.route('/upload', methods=['POST'])
def upload_files():
    files = request.files.getlist('files')

    if len(files) != 3:
        return jsonify({'error': 'Se requieren exactamente 3 archivos'}), 400

    for file in files:
        # Lee el CSV
        df = pd.read_csv(file)
        schema = SCHEMAS.get(file.filename)

        # Verifica que no se exceda el límite de filas
        if len(df) > 1000:
            logging.error(f'Error en {file.filename}: excede el límite de 1000 filas')
            continue

        # Valida las transacciones
        if not validate_data(df, schema):
            logging.error(f'Error en {file.filename}: datos inválidos')
            continue

        # Inserta en BigQuery
        table_id = f'{PROJECT_ID}.{DATASET_ID}.{file.filename[:-4]}'
        job_config = bigquery.LoadJobConfig(schema=schema)

        try:
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Espera a que el job finalice
        except Exception as e:
            logging.error(f'Error al insertar en {file.filename}: {e}')

    return jsonify({'message': 'Archivos procesados'}), 200

if __name__ == '__main__':
    app.run(debug=True)