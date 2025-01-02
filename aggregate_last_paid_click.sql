with purchases_tab1 as (
    select
        leads.visitor_id,
        sessions.visit_date,
        sessions.source,
        sessions.medium,
        sessions.campaign,
        sessions.content,
        leads.amount
    from leads
    inner join sessions
        on
            leads.visitor_id = sessions.visitor_id
            and (
                leads.closing_reason = 'Успешно реализовано'
                or leads.status_id = 142
            )
    where
        sessions.medium in (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
),

purchases_tab2 as (
    select
        visitor_id,
        visit_date,
        source,
        medium,
        campaign,
        content,
        amount,
        row_number()
        over (
            partition by visitor_id
            order by visit_date desc
        )
        as num
    from purchases_tab1
),

purchases_tab3 as (
    select
        cast(visit_date as date) as visit_date,
        source,
        medium,
        campaign,
        count(*) as purchases_count,
        sum(amount) as revenue
    from purchases_tab2
    where
        num = 1
        and medium != 'cpa'
    group by cast(visit_date as date), source, medium, campaign
),

leads_tab1 as (
    select distinct
        leads.visitor_id,
        sessions.visit_date,
        sessions.source,
        sessions.medium,
        sessions.campaign,
        sessions.content
    from leads
    inner join sessions
        on
            leads.visitor_id = sessions.visitor_id
            and leads.created_at >= sessions.visit_date
    where
        sessions.medium in (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
),

leads_tab2 as (
    select
        visitor_id,
        visit_date,
        source,
        medium,
        campaign,
        content,
        row_number()
        over (
            partition by visitor_id
            order by visit_date desc
        )
        as num
    from leads_tab1
),

leads_tab3 as (
    select
        cast(visit_date as date) as visit_date,
        source,
        medium,
        campaign,
        count(visitor_id) as leads_count
    from leads_tab2
    where num = 1
    group by cast(visit_date as date), source, medium, campaign
),

cost_tab1 as (
    select
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        daily_spent
    from vk_ads
    union all
    select
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        daily_spent
    from ya_ads
),

cost_tab2 as (
    select
        cast(campaign_date as date) as campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from cost_tab1
    group by cast(campaign_date as date), utm_source, utm_medium, utm_campaign
),

visitors_tab1 as (
    select
        leads.visitor_id,
        sessions.visit_date,
        sessions.source,
        sessions.medium,
        sessions.campaign,
        sessions.content
    from leads
    inner join sessions
        on leads.visitor_id = sessions.visitor_id
    where
        sessions.medium in (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
),

visitors_tab2 as (
    select
        visitor_id,
        visit_date,
        source,
        medium,
        campaign,
        content,
        row_number()
        over (
            partition by visitor_id
            order by visit_date desc
        )
        as num
    from visitors_tab1
),

visitors_tab3 as (
    select
        visitor_id,
        cast(visit_date as date) as visit_date,
        source,
        medium,
        campaign,
        content
    from visitors_tab2
    where num = 1
    union
    select
        visitor_id,
        cast(visit_date as date) as visit_date,
        source,
        medium,
        campaign,
        content
    from sessions
    where
        not exists (
            select *
            from leads
            where leads.visitor_id = sessions.visitor_id
        )
),

visitors_tab4 as (
    select
        visit_date,
        source,
        medium,
        campaign,
        count(distinct visitor_id) as visitors_count
    from visitors_tab3
    group by visit_date, source, medium, campaign
),

tab1 as (
    select distinct
        cast(visit_date as date) as visit_date,
        source,
        medium,
        campaign
    from sessions
),

tab2 as (
    select
        tab1.visit_date,
        tab1.source as utm_source,
        tab1.medium as utm_medium,
        tab1.campaign as utm_campaign,
        coalesce(visitors_tab4.visitors_count, 0) as visitors_count,
        coalesce(cast(cost_tab2.total_cost as text), '') as total_cost,
        coalesce(leads_tab3.leads_count, 0) as leads_count,
        coalesce(purchases_tab3.purchases_count, 0) as purchases_count,
        coalesce(purchases_tab3.revenue, 0) as revenue
    from tab1
    left join purchases_tab3
        on
            tab1.visit_date = purchases_tab3.visit_date
            and tab1.source = purchases_tab3.source
            and tab1.medium = purchases_tab3.medium
            and tab1.campaign = purchases_tab3.campaign
    left join leads_tab3
        on
            tab1.visit_date = leads_tab3.visit_date
            and tab1.source = leads_tab3.source
            and tab1.medium = leads_tab3.medium
            and tab1.campaign = leads_tab3.campaign
    left join cost_tab2
        on
            tab1.visit_date = cost_tab2.campaign_date
            and tab1.source = cost_tab2.utm_source
            and tab1.medium = cost_tab2.utm_medium
            and tab1.campaign = cost_tab2.utm_campaign
    left join visitors_tab4
        on
            tab1.visit_date = visitors_tab4.visit_date
            and tab1.source = visitors_tab4.source
            and tab1.medium = visitors_tab4.medium
            and tab1.campaign = visitors_tab4.campaign
)

select *
from tab2
order by
    revenue desc, visit_date asc, visitors_count desc,
    utm_source asc, utm_medium asc, utm_campaign asc;
