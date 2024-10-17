# globant
# API REST para Carga de Archivos CSV en BigQuery

Este proyecto implementa un servicio API REST utilizando Google Cloud Functions que permite cargar datos desde archivos CSV desde Cloud Storage a BigQuery. Se valida el servicio valida y registra las transacciones.

## Requisitos

- Python 3.9
- Google Cloud SDK
- Bibliotecas necesarias en `requirements.txt`:
  - `google-cloud-bigquery`
  - `google-cloud-storage`
  - `pandas`
  - `flask`
  - `pyarrow`

## Estructura de Archivos

- **main.py**: Código de la función de la API.
- **requirements.txt**: Dependencias del proyecto.
- **README.md**: Documentación del proyecto.

## Esquema de Archivos CSV

1. **hired_employees.csv**
   - id: INTEGER
   - name: STRING
   - datetime: STRING
   - department_id: INTEGER
   - job_id: INTEGER

2. **departments.csv**
   - id: INTEGER
   - department: STRING

3. **jobs.csv**
   - id: INTEGER
   - job: STRING

## Despliegue

Para desplegar la función en Google Cloud Functions, utiliza el siguiente comando:

## Bash
gcloud functions deploy upload_csvs --runtime python39 --trigger-http --allow-unauthenticated

## URL

https://us-central1-coral-velocity-438815-v4.cloudfunctions.net/upload_csvs

# Body / raw / json

{
    "bucket_name": "pe-archivos-sensibles-diferencial"
}
