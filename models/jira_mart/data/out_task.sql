{{ config(
  sql_header="set DEF_TIMESTAMP_FORMAT= 'YYYY-MM-DDTHH:MI:SSTZH';
              set DEF_TIMEZONE= 'Etc/UTC';",
  materialized='incremental',
  unique_key='"task_id"'
) }}

SELECT
    i."id" AS "task_id" -- PK
    ,i."key" AS "task_key"
    ,i."summary" AS "task_name"
    ,i."description" AS "task_description"
    ,i."issue_type_id" AS "task_type_id"
    ,i."issue_type_name" AS "task_type_name"
--    ,i."key" AS "task_key"
    ,i."time_estimate" AS "time_estimate"
    ,i."status_name"
    ,i."priority_name" AS "priority"
    ,PARSE_JSON(i."custom_fields"):customfield_10015::VARCHAR(255) AS "start_date"
    ,TRY_TO_DATE(i."due_date") AS "due_date"
--    ,TO_CHAR(TRY_TO_TIMESTAMP(i."created",'YYYY-MM-DDTHH24:MI:SS.FF3+TZHTZM'),$DEF_TIMESTAMP_FORMAT) AS "task_created"
    ,TO_CHAR(CONVERT_TIMEZONE($DEF_TIMEZONE, TRY_TO_TIMESTAMP(i."created",'YYYY-MM-DDTHH24:MI:SS.FF3+TZHTZM')),$DEF_TIMESTAMP_FORMAT) AS "task_created"
    ,p."project_name" -- linked for testing ref integrity
    ,p."project_id"
    ,i."assignee_account_id" AS "assignee_user_id" -- user_id
--    ,u."user_name" AS "assignee_user_name" -- linked for testing ref integrity, beware unassigned tasks!
    ,i."creator_account_id" AS "created_by_user_id"
    ,PARSE_JSON(i."custom_fields"):customfield_10014::VARCHAR(255) AS "epic_name_link"
    ,i."parent_id" AS "parent_id"
    -- highly manual mapping of the tasks.
    -- three layers: issue types->dedicated project name->custom fields (customer projects)
    ,CASE
        WHEN
            i."issue_type_name" = 'Epic' THEN 'epic'::VARCHAR(255)
        WHEN
            i."issue_type_name" = 'Use Case' THEN 'epic'::VARCHAR(255)
        WHEN
            i."issue_type_name" IN ('Sub-task','Subtask') THEN 'sub-task'::VARCHAR(255)
        WHEN
            i."issue_type_name" IN ('R&D - Component creation') THEN 'research_and_development'::VARCHAR(255)
        WHEN
            p."project_name" = 'Keboola Sales' THEN 'sales'::VARCHAR(255)
        WHEN
            p."project_name" = 'Keboola Component Factory' THEN 'research_and_development'::VARCHAR(255)
        WHEN
            p."project_name" = 'Keboola Internal Projects' AND i."issue_type_name" = 'Idea' THEN 'research_and_development'::VARCHAR(255)
        WHEN
            p."project_name" = 'Keboola Team' THEN 'internal'::VARCHAR(255)
        WHEN
            PARSE_JSON(i."custom_fields"):customfield_10136[0]::VARCHAR(255) IS NOT NULL THEN PARSE_JSON(i."custom_fields"):customfield_10136[0]::VARCHAR(255)
        WHEN
            PARSE_JSON(i."custom_fields"):customfield_10150[0] IS NOT NULL THEN PARSE_JSON(i."custom_fields"):customfield_10150[0]::VARCHAR(255)
        WHEN
            PARSE_JSON(i."custom_fields"):customfield_10149[0] IS NOT NULL THEN PARSE_JSON(i."custom_fields"):customfield_10149[0]::VARCHAR(255)
        WHEN
            PARSE_JSON(i."custom_fields"):customfield_10142[0] IS NOT NULL THEN PARSE_JSON(i."custom_fields"):customfield_10142[0]::VARCHAR(255)
        ELSE 
            PARSE_JSON(i."custom_fields"):customfield_10148[0]::VARCHAR(255)
        END AS "task_type"
     ,CASE
        WHEN
            PARSE_JSON(i."custom_fields"):customfield_10147[0]:value::VARCHAR(255) IS NOT NULL THEN PARSE_JSON(i."custom_fields"):customfield_10147[0]:value::VARCHAR(255)
        WHEN
            PARSE_JSON(i."custom_fields"):customfield_10134[0]:value::VARCHAR(255) IS NOT NULL THEN PARSE_JSON(i."custom_fields"):customfield_10134[0]:value::VARCHAR(255)
        WHEN
            PARSE_JSON(i."custom_fields"):customfield_10141[0]:value::VARCHAR(255) = 'Unbillable' THEN 'No'::VARCHAR(255)
        WHEN
            PARSE_JSON(i."custom_fields"):customfield_10142[0]:value::VARCHAR(255) IS NOT NULL THEN PARSE_JSON(i."custom_fields"):customfield_10142[0]:value::VARCHAR(255)
        WHEN
            p."project_name" IN ('Keboola Internal Projects','Keboola Sales','Keboola Team') THEN 'No'::VARCHAR(255)
        ELSE
            NULL --'NA'::VARCHAR(255)
        END AS "billable"
FROM
    {{ source('"WORKSPACE_7938092"', '"issues"') }} i
JOIN
    {{ ref('out_project') }} p ON -- FULL JOIN FOR PS projects only
    i."project_key" = p."project_key"