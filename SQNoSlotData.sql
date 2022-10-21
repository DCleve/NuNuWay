select
    case
        when len(sq.shippingqueuenumber) in (11, 14) then left(sq.shippingqueuenumber, 8)
        when len(sq.shippingqueuenumber) in (12, 15) then left(sq.shippingqueuenumber, 9)
        when len(sq.shippingqueuenumber) in (13, 16) then left(sq.shippingqueuenumber, 10)
        else sq.shippingqueuenumber
            end as queue_number
    , sq.shippingqueuenumber
    , sq.ordercount as order_count
    , sq.productcount as product_count

from
    "HVR_TCGSTORE_PRODUCTION"."TCGD"."SHIPPINGQUEUE" as sq

where
    sq.createdat >= dateadd(dd, -115, getdate())

union all

select
    concat(bpoq.buylistpurchaseorderqueuenumber, 'POQ')
    , bpoq.buylistpurchaseorderqueuenumber as raw_blo_number
    , bpoq.ordercount
    , bpoq.productcount

from
    "HVR_TCGSTORE_PRODUCTION"."BYL"."BUYLISTPURCHASEORDERQUEUE" as bpoq

where
    bpoq.createdat >= dateadd(dd, -115, getdate())