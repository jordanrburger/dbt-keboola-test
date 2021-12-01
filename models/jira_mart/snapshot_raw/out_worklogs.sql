{{ config(
  materialized='incremental',
  unique_key='"id"'
) }}

SELECT
	  "id"
    ,"issue_id"
    ,"author_account_id"
    ,"author_display_name"
    ,"update_author_account_id"
    ,"update_author_display_name"
    ,"created"
    ,"updated"
    ,"started"
    ,"time_spent"
    ,"time_spent_seconds"
    ,"comment"
    ,CURRENT_DATE() AS "snapshot_date"
FROM {{ source('"WORKSPACE_7938092"', '"worklogs"') }}
