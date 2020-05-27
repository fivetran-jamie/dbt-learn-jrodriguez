with payments as (
    select
        "orderID" as order_id,
        amount
    from raw.stripe.payment
)
,
stg_orders as (
    select * from  {{ref ('stg_orders') }}
)
,
final as (
    select
        payments.order_id,
        stg_orders.customer_id,
        order_date,
        coalesce(sum(payments.amount), 0) as amount
    from
    payments 
    join stg_orders on payments.order_id = stg_orders.order_id
    group by 1,2,3
)

select * from final
