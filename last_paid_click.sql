select
    visitor_id,
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
from
    (
        select
            visitor_id,
            visit_date,
            utm_source,
            utm_medium,
            utm_campaign,
            lead_id,
            created_at,
            amount,
            closing_reason,
            status_id,
            lead_id_max,
            row_number() over (
                partition by visitor_id
                order by lead_id_max desc, visit_date desc
            ) as num
        from
            (
                select
                    sessions.visitor_id,
                    sessions.visit_date,
                    sessions.source as utm_source,
                    sessions.medium as utm_medium,
                    sessions.campaign as utm_campaign,
                    leads.lead_id,
                    leads.created_at,
                    leads.amount,
                    leads.closing_reason,
                    leads.status_id,
                    coalesce(
                        max(leads.lead_id)
                            over (partition by sessions.visitor_id),
                        ''
                    ) as lead_id_max
                from sessions
                left join
                    leads
                    on
                        sessions.visitor_id = leads.visitor_id
                        and sessions.visit_date <= leads.created_at
                where
                    sessions.medium in (
                        'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
                    )
            ) as x1
    ) as x2
where num = 1
order by
    amount desc nulls last, visit_date asc, utm_source asc, utm_medium asc, utm_campaign asc;
