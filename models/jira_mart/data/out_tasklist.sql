{{
  config(
    post_hook = "{{ macro_update_task() }}"
    )
}}

SELECT
    t."task_id" AS "tasklist_id"
    ,t."task_id" AS "task_id"
    ,t."task_key"
    ,t."task_name"
    ,t."task_description"
    ,t."task_type_id"
    ,t."task_type_name"
    ,t."time_estimate"
    ,t."status_name"
    ,t."priority"
    ,t."start_date"
    ,t."due_date"
    ,t."task_created"
    ,t."project_name"
    ,t."project_id"
    ,t."assignee_user_id"
    ,t."created_by_user_id"
    ,t."task_type"
    ,t."billable"
FROM {{ ref('out_task') }} t
WHERE t."task_type_name" = 'Epic'
UNION
SELECT
    DISTINCT(ot."parent_id") AS "task_list_id"
    ,ot."parent_id" AS "task_id"
    ,o."task_key"
    ,o."task_name"
    ,o."task_description"
    ,o."task_type_id"
    ,o."task_type_name"
    ,o."time_estimate"
    ,o."status_name"
    ,o."priority"
    ,o."start_date"
    ,o."due_date"
    ,o."task_created"
    ,o."project_name"
    ,o."project_id"
    ,o."assignee_user_id"
    ,o."created_by_user_id"
    ,o."task_type"
    ,o."billable"
FROM {{ ref('out_task') }} ot
LEFT JOIN
    {{ ref('out_task') }} o ON
    ot."parent_id" = o."task_id"
WHERE
    ot."parent_id" IS NOT NULL
    AND
    ot."parent_id" != ''
    AND
    ot."parent_id" IS NOT NULL
    AND
    o."task_id" IS NOT NULL
    AND
    ot."task_type" != 'sub-task'