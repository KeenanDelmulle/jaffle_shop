with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),
payment_orders as (

    select
        order_id,

        sum(amount) as total_amount
        

    from payments

    group by 1

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        payment_orders.total_amount as amount
     
     from orders   

     left join payment_orders
        on orders.order_id = payment_orders.order_id

)

select * from final