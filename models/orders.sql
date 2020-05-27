with payments as (
    select
        "orderID" as order_id,
        amount
    from raw.stripe.payment
)
,
stg_orders as (
    select 
        order_id,
        customer_id
    from  {{ref ('stg_orders') }}
)
,
final as (
    select
        payments.order_id,
        stg_orders.customer_id,
        sum(payments.amount) as amount
    from
    payments 
    join stg_orders on payments.order_id = stg_orders.order_id
    group by 1,2
)

select * from final
