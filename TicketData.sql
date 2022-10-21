select distinct
    tickets.id
    , case
        when contains(tickets.tags,'warehousefeedback_cardname') then 'card'
        when contains(tickets.tags,'warehousefeedback_card') then 'card'
        when contains(tickets.tags,'warehousefeedback_quantity') then 'quantity'
        when contains(tickets.tags,'warehousefeedback_package') then 'package'
        when contains(tickets.tags,'wrongpackage') then 'package'
        else NULL
        end as ticket_tag

    , coalesce(regexp_substr(ticket_comments.body, '\\d{6}-\\w{4}'), tickets.order_number) as direct_order_number

    from segment.zendesk.tickets
    inner join
        segment.zendesk.ticket_comments
            on tickets.id = ticket_comments.ticket_id

    where
    coalesce(regexp_substr(ticket_comments.body, '\\d{6}-\\w{4}'), tickets.order_number) is not null
    and len(coalesce(regexp_substr(ticket_comments.body, '\\d{6}-\\w{4}'), tickets.order_number)) <= 11

    """)