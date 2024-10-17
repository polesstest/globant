import pandas as pd
from google.cloud import bigquery, storage
from flask import jsonify
import logging
from io import StringIO

# Inicializa los clientes de BigQuery y Cloud Storage
bq_client = bigquery.Client()
storage_client = storage.Client()

# Configuración de logging
logging.basicConfig(level=logging.INFO)

def validate_row(row: pd.Series, table_name: str) -> bool:
    """Valida si la fila cumple con los requisitos."""
    return not any(pd.isnull(row))

def insert_data_to_bq(table_name: str, data: pd.DataFrame):
    """Inserta datos en la tabla de BigQuery."""
    table_id = f"coral-velocity-438815-v4.stg_rrhh.{table_name}"
    job = bq_client.load_table_from_dataframe(data, table_id)
    job.result()  # Espera a que el trabajo finalice

def download_blob(bucket_name: str, source_blob_name: str) -> str:
    """Descarga el contenido de un blob desde Cloud Storage."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_text()

def upload_csvs(request):
    """Función principal para manejar la solicitud de carga de CSVs."""
    request_json = request.get_json()

    # Verifica que se proporcione el nombre del bucket
    if not request_json or 'bucket_name' not in request_json:
        return jsonify({"error": "Request must include 'bucket_name'."}), 400

    bucket_name = request_json['bucket_name']
    
    # Archivos por defecto
    hired_employees = 'hired_employees.csv'
    departments = 'departments.csv'
    jobs = 'jobs.csv'

    try:
        # Descarga los archivos CSV
        hired_employees_csv = download_blob(bucket_name, hired_employees)
        departments_csv = download_blob(bucket_name, departments)
        jobs_csv = download_blob(bucket_name, jobs)

        # Carga los CSV en DataFrames
        hired_employees_df = pd.read_csv(StringIO(hired_employees_csv))
        departments_df = pd.read_csv(StringIO(departments_csv))
        jobs_df = pd.read_csv(StringIO(jobs_csv))

        # Validación y registro de datos inválidos
        invalid_hired_employees = hired_employees_df[~hired_employees_df.apply(lambda row: validate_row(row, 'hired_employees'), axis=1)]
        invalid_departments = departments_df[~departments_df.apply(lambda row: validate_row(row, 'departments'), axis=1)]
        invalid_jobs = jobs_df[jobs_df.apply(lambda row: validate_row(row, 'jobs'), axis=1)]

        for invalid in [invalid_hired_employees, invalid_departments, invalid_jobs]:
            if not invalid.empty:
                logging.error(f"Transacciones inválidas: {invalid.to_dict(orient='records')}")

        # Filtra los datos válidos
        hired_employees_valid = hired_employees_df[hired_employees_df.apply(lambda row: validate_row(row, 'hired_employees'), axis=1)]
        departments_valid = departments_df[departments_df.apply(lambda row: validate_row(row, 'departments'), axis=1)]
        jobs_valid = jobs_df[jobs_df.apply(lambda row: validate_row(row, 'jobs'), axis=1)]

        # Inserción en BigQuery
        if not hired_employees_valid.empty:
            insert_data_to_bq('hired_employees', hired_employees_valid)
        if not departments_valid.empty:
            insert_data_to_bq('departments', departments_valid)
        if not jobs_valid.empty:
            insert_data_to_bq('jobs', jobs_valid)

        return jsonify({"status": "success", "message": "Data inserted successfully!"}), 200

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
