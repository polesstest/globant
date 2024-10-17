CREATE OR REPLACE PROCEDURE `coral-velocity-438815-v4.operacional.sp_restore`()
BEGIN

  LOAD DATA OVERWRITE `coral-velocity-438815-v4.stg_rrhh.departments`
  FROM FILES (
    FORMAT = 'AVRO',
    uris = ['gs://pe-archivos-sensibles-respaldo/backup-departments-000000000000.avro']);


  LOAD DATA OVERWRITE `coral-velocity-438815-v4.stg_rrhh.hired_employees`
  FROM FILES (
    FORMAT = 'AVRO',
    uris = ['gs://pe-archivos-sensibles-respaldo/backup-hired_employees-000000000000.avro']);


  LOAD DATA OVERWRITE `coral-velocity-438815-v4.stg_rrhh.jobs`
  FROM FILES (
    FORMAT = 'AVRO',
    uris = ['gs://pe-archivos-sensibles-respaldo/backup-jobs-000000000000.avro']);

END;