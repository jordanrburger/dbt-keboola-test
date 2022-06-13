with products as (

    select *
    from  {{ source('my_source', 'shopify_product') }}

), order_lines as (

    select *
    from {{ source('my_source', 'shopify_order_lines') }}


), orders as (

    select *
    from {{ source('my_source', 'shopify_orders') }}


), order_lines_aggregated as (

    select 
        order_lines.product_id, 
        order_lines.source_relation,
        sum(order_lines.quantity) as quantity_sold,
        sum(order_lines.pre_tax_price) as subtotal_sold,


        min(orders.created_timestamp) as first_order_timestamp,
        max(orders.created_timestamp) as most_recent_order_timestamp
    from order_lines
    left join orders
        using (order_id, source_relation)
    group by 1,2

), joined as (

    select
        products.*,
        coalesce(order_lines_aggregated.quantity_sold,0) as quantity_sold,
        coalesce(order_lines_aggregated.subtotal_sold,0) as subtotal_sold,

        
        order_lines_aggregated.first_order_timestamp,
        order_lines_aggregated.most_recent_order_timestamp
    from products
    left join order_lines_aggregated
        using (product_id, source_relation)

)

select *
from joined
