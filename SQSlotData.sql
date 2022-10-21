select
    case
        when len(sq.shippingqueuenumber) in (11, 14) then left(sq.shippingqueuenumber, 8)
        when len(sq.shippingqueuenumber) in (12, 15) then left(sq.shippingqueuenumber, 9)
        when len(sq.shippingqueuenumber) in (13, 16) then left(sq.shippingqueuenumber, 10)
        else sq.shippingqueuenumber
            end as queue_number

    , sq.shippingqueuenumber as shippingqueuenumber

    , case
        when sqps.slot = 'A' then '1'
        when sqps.slot = 'B' then '2'
        when sqps.slot = 'C' then '3'
        when sqps.slot = 'D' then '4'
        when sqps.slot = 'E' then '5'
        when sqps.slot = 'F' then '6'
        when sqps.slot = 'G' then '7'
        when sqps.slot = 'H' then '8'
        when sqps.slot = 'I' then '9'
        when sqps.slot = 'J' then '10'
        when sqps.slot = 'K' then '11'
        when sqps.slot = 'L' then '12'
        when sqps.slot = 'M' then '13'
        when sqps.slot = 'N' then '14'
        when sqps.slot = 'O' then '15'
        when sqps.slot = 'P' then '16'
        when sqps.slot = 'Q' then '17'
        when sqps.slot = 'R' then '18'
        when sqps.slot = 'S' then '19'
        when sqps.slot = 'T' then '20'
        when sqps.slot = 'U' then '21'
        when sqps.slot = 'V' then '22'
        when sqps.slot = 'W' then '23'
        when sqps.slot = 'X' then '24'
        else sqps.slot
            end as slot

    , count(distinct sqps.productconditionid) as unique_pcids
    , sum(sqps.quantity) as quantity

from
    "HVR_TCGSTORE_PRODUCTION"."TCGD"."SHIPPINGQUEUE" as sq
        inner join
            "HVR_TCGSTORE_PRODUCTION"."TCGD"."SHIPPINGQUEUEPULLSHEET" as sqps
                on sq.shippingqueueid = sqps.shippingqueueid

where
    sq.createdat >= dateadd(dd, -115, getdate())
    and left(sq.shippingqueuenumber, 6) <> 'Holdin'

group by
    sq.shippingqueuenumber
    , sqps.slot

union all

select
    concat(blpoq.buylistpurchaseorderqueuenumber, 'POQ')
    , blpoq.buylistpurchaseorderqueuenumber

    , case
        when blpoqps.slot = 'A' then '1'
        when blpoqps.slot = 'B' then '2'
        when blpoqps.slot = 'C' then '3'
        when blpoqps.slot = 'D' then '4'
        when blpoqps.slot = 'E' then '5'
        when blpoqps.slot = 'F' then '6'
        when blpoqps.slot = 'G' then '7'
        when blpoqps.slot = 'H' then '8'
        when blpoqps.slot = 'I' then '9'
        when blpoqps.slot = 'J' then '10'
        when blpoqps.slot = 'K' then '11'
        when blpoqps.slot = 'L' then '12'
        when blpoqps.slot = 'M' then '13'
        when blpoqps.slot = 'N' then '14'
        when blpoqps.slot = 'O' then '15'
        when blpoqps.slot = 'P' then '16'
        when blpoqps.slot = 'Q' then '17'
        when blpoqps.slot = 'R' then '18'
        when blpoqps.slot = 'S' then '19'
        when blpoqps.slot = 'T' then '20'
        when blpoqps.slot = 'U' then '21'
        when blpoqps.slot = 'V' then '22'
        when blpoqps.slot = 'W' then '23'
        when blpoqps.slot = 'X' then '24'
        else blpoqps.slot
            end as slot

    , count(distinct blpoqps.productconditionid) as unique_pcids
    , sum(blpoqps.quantity) as quantity

from
    "HVR_TCGSTORE_PRODUCTION"."BYL"."BUYLISTPURCHASEORDERQUEUE" as blpoq
        inner join
            "HVR_TCGSTORE_PRODUCTION"."BYL"."BUYLISTPURCHASEORDERQUEUEPULLSHEET" as blpoqps
                on blpoq.buylistpurchaseorderqueueid = blpoqps.buylistpurchaseorderqueueid

where
    blpoq.createdat >= dateadd(dd, -115, getdate())

group by
    blpoq.buylistpurchaseorderqueuenumber
    , blpoqps.slot