{{
    config(
        materialized='incremental',
        unique_key='"project_id"'
    )
}}

SELECT
    "id" AS "project_id" -- PK
    ,"key" AS "project_key"
    ,"name" AS "project_name"
    ,"project_category_name"
    ,"archived" AS "archived"
    ,CASE WHEN "project_category_name" = 'Keboola Internal' THEN '0011t00000GslojAAB' 
          ELSE PARSE_JSON("description"):SFDC_ID::VARCHAR(255) 
        END AS "sfdc_id"
    ,CASE WHEN "project_category_name" = 'Keboola Internal' THEN '0' 
          ELSE PARSE_JSON("description"):BUDGET::VARCHAR(255) 
        END AS "budget"
    ,CASE WHEN "project_category_name" = 'Keboola Internal' THEN '0' 
          ELSE PARSE_JSON("description"):PRICE::VARCHAR(255) 
        END AS "hour_rate"
FROM {{ source('"WORKSPACE_7938092"', '"projects"') }}
WHERE
    "project_category_id" IN ('10001','10004') -- Project Implementation and Internal
    AND
    "project_id" NOT IN ('10028') -- not old CSAS project - rubbish data
    AND
    "archived" != 'True'