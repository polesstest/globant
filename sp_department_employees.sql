CREATE OR REPLACE PROCEDURE `coral-velocity-438815-v4.operacional.sp_department_employees`()
BEGIN

    DECLARE PROMEDIO INT64;
    SET     PROMEDIO  =     (SELECT  CAST(ROUND(AVG(cantidad)) AS INT64)  AS promedio 
                            FROM    (SELECT  de.department    AS department,
                                              COUNT(1)         AS cantidad
                                      FROM    `coral-velocity-438815-v4.stg_rrhh.hired_employees` AS he
                                      INNER JOIN `coral-velocity-438815-v4.stg_rrhh.departments`  AS de ON he.department_id = de.id
                                      WHERE   EXTRACT(YEAR FROM DATE(he.datetime)) = 2021
                                      GROUP BY 1
                                    )
                            );

    CREATE OR REPLACE TABLE `coral-velocity-438815-v4.presentacion.department_employees` AS 
    SELECT  de.id         AS id,                 
            de.department AS department,
            COUNT(1)      AS hired
    FROM    `coral-velocity-438815-v4.stg_rrhh.hired_employees` AS he
    INNER JOIN `coral-velocity-438815-v4.stg_rrhh.departments`  AS de ON he.department_id = de.id
    WHERE  EXTRACT(YEAR FROM DATE(he.datetime)) = 2021
    GROUP BY 1,2
    HAVING COUNT(1) >= PROMEDIO
    ORDER BY 3 DESC;

END;