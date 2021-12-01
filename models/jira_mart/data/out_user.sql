{{
  config(
    materialized='incremental',
    unique_key='"user_id"'
    )
}}

SELECT
    "account_id" AS "user_id"
    ,"display_name" AS "user_name"
    ,"active" AS "active"
    ,"email_address" AS "user_email"
FROM {{ source('"WORKSPACE_7938092"', '"users"') }}
WHERE
    "account_type" != 'app'