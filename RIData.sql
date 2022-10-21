select
    ri.reimbursement_invoice_number as ri_number
    , ri.total_product_quantity as number_of_cards

    , ri.processed_by_user_email as processor_email
    , ri.processing_ended_at_et as processing_ended
    , cast(ri.processing_ended_at_et as date) as day_processing_ended
    , ri.active_processing_time_minutes as proc_time_minutes
    , ri.active_verification_time_minutes as ver_time_minutes
    , case
        when ri.verified_by_user_email = processor_email and ver_time_minutes > 5 then 'Flow'
        else 'RI Proc'
        end as ri_tag

from
    "ANALYTICS"."CORE"."REIMBURSEMENT_INVOICES" as ri

where
    ri.processing_ended_at_et >= dateadd(dd, -110, getdate())
    and ri.is_auto = false