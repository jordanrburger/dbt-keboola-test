{% macro macro_update_task() %} 
update {{ ref('out_task') }} set "task_type" = 'epic' 
where "task_id" IN (SELECT DISTINCT("task_id") FROM {{ this }} );
UPDATE {{ ref('out_task') }} t SET t."parent_id" = tl."task_id"
FROM {{ this }} tl
WHERE t."epic_name_link" = tl."task_key" AND
      t."epic_name_link" IS NOT NULL AND t."parent_id" = '';
{% endmacro %}