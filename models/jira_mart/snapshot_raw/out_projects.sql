{{ config(
  materialized='incremental',
  unique_key='"id"'
) }}

SELECT
	"id"
    ,"key"
    ,"name"
    ,"description"
    ,"project_category_id"
    ,"project_category_name"
    ,"project_category_description"
    ,"project_type_key"
    ,"is_private"
    ,"archived"
    ,"archived_by_account_id"
    ,"archived_by_display_name"
    ,CURRENT_DATE() AS "snapshot_date"
FROM {{ source('"WORKSPACE_7938092"', '"projects"') }}