select
   dor.DirectOrderNumber as direct_order_number
   , sq.ShippingQueueNumber as sq_number
   , case
     when count(distinct oi.product_line) = 1 then max(oi.product_line)
     else 'Multiple' end
     as product_line

from
    "HVR_TCGSTORE_PRODUCTION"."TCGD"."DIRECTORDER" as dor
        inner join
           "HVR_TCGSTORE_PRODUCTION"."DBO"."ORDER" as o
                on dor.OrderId = o.OrderId

        left outer join
            "HVR_TCGSTORE_PRODUCTION"."TCGD"."SHIPPINGQUEUE" as sq
                on dor.ShippingQueueId = sq.ShippingQueueId

        left outer join
            "ANALYTICS"."CORE"."ORDER_ITEMS" as oi
                on oi.Order_Id = dor.OrderId

        inner join
            "ANALYTICS"."CORE"."SELLER_ORDERS" as so
                on so.id = oi.seller_order_id
 where
    o.OrderStatusId = 3
    and o.OrderDate >= dateadd(dd, -150, getdate())

group by
   dor.DirectOrderNumber
   , sq.ShippingQueueNumber