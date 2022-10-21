select
    bo.offer_number as buylist_offer_number
    , bo.expected_product_count as blo_product_count
    , bo.processed_by_user_email as blo_processor_email
    , bo.verified_by_user_email as blo_verifier_email

    , bo.active_processing_started_at_et as blo_processing_at
    , cast(bo.active_processing_started_at_et as date) as day_of_blo_processing

    , bo.active_verification_started_at_et as blo_verifying_at
    , cast(bo.active_verification_started_at_et as date) as day_of_blo_verifying

from
    "ANALYTICS"."CORE"."BUYLIST_OFFERS" as bo
        inner join
            "ANALYTICS"."CORE"."USERS" as u
                on u.id = bo.player_id

where
    bo.active_processing_started_at_et >= dateadd(dd, -100, getdate())