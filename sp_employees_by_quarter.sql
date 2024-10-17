CREATE OR REPLACE PROCEDURE `coral-velocity-438815-v4.operacional.sp_employees_by_quarter`()
BEGIN
    CREATE OR REPLACE TABLE `coral-velocity-438815-v4.presentacion.employees_by_quarter` AS

    WITH
    QUARTER1 AS (
                  SELECT  de.department                         AS department,
                          jo.job                                AS job,
                          CASE WHEN EXTRACT(MONTH FROM DATE(he.datetime)) BETWEEN 1 AND 3 THEN 'Q1'
                               WHEN EXTRACT(MONTH FROM DATE(he.datetime)) BETWEEN 4 AND 6 THEN 'Q2'
                               WHEN EXTRACT(MONTH FROM DATE(he.datetime)) BETWEEN 7 AND 9 THEN 'Q3' 
                               ELSE 'Q4' END AS QUARTER,
                          COUNT(1)                              AS cantidad
                  FROM    `coral-velocity-438815-v4.stg_rrhh.hired_employees` AS he
                  INNER JOIN `coral-velocity-438815-v4.stg_rrhh.jobs`         AS jo ON he.job_id        = jo.id
                  INNER JOIN `coral-velocity-438815-v4.stg_rrhh.departments`  AS de ON he.department_id = de.id
                  WHERE   EXTRACT(YEAR FROM DATE(he.datetime)) = 2021
                  GROUP BY 1,2,3
              ),

    QUARTER2 AS (
                    SELECT *
                    FROM  (SELECT * FROM QUARTER1)
                    PIVOT (SUM(cantidad) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
                    ORDER BY 1
                )


    SELECT  q2.department       AS department, 
            q2.job              AS job, 
            COALESCE(q2.Q1,0)   AS Q1, 
            COALESCE(q2.Q2,0)   AS Q2, 
            COALESCE(q2.Q3,0)   AS Q3, 
            COALESCE(q2.Q4,0)   AS Q4
    FROM    QUARTER2 AS q2
    ORDER BY 1, 2;
END;