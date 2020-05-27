{{
  config(
    materialized='view'
  )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders
    from customers
    left join customer_orders using (customer_id)
)
,
final_two as (
    select 
        final.*,
        coalesce(sum(orders.amount), 0) as lifetime_value
    from final 
    left join {{ref ("orders")}} on final.customer_id=orders.customer_id

    group by final.customer_id, first_name, last_name, first_order_date, most_recent_order_date, number_of_orders
)
select * from final_two