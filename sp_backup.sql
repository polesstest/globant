CREATE OR REPLACE PROCEDURE `coral-velocity-438815-v4.operacional.sp_backup`()
BEGIN

  EXPORT DATA OPTIONS (
    uri='gs://pe-archivos-sensibles-respaldo/backup-jobs-*.avro',  -- Cambia por tu bucket y nombre de archivo
    FORMAT='AVRO',
    OVERWRITE = TRUE  -- Si deseas sobrescribir archivos existentes
  ) AS
  SELECT  id, job
  FROM    `coral-velocity-438815-v4.stg_rrhh.jobs`;


  EXPORT DATA OPTIONS (
    uri='gs://pe-archivos-sensibles-respaldo/backup-departments-*.avro',  -- Cambia por tu bucket y nombre de archivo
    FORMAT='AVRO',
    OVERWRITE = TRUE  -- Si deseas sobrescribir archivos existentes
  ) AS
  SELECT  id, department
  FROM    `coral-velocity-438815-v4.stg_rrhh.departments`;


  EXPORT DATA OPTIONS (
    uri='gs://pe-archivos-sensibles-respaldo/backup-hired_employees-*.avro',  -- Cambia por tu bucket y nombre de archivo
    FORMAT='AVRO',
    OVERWRITE = TRUE  -- Si deseas sobrescribir archivos existentes
  ) AS
  SELECT  id, name, datetime, department_id, job_id
  FROM    `coral-velocity-438815-v4.stg_rrhh.hired_employees`;

END;