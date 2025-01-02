select
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    visitors_count,
    total_cost,
    leads_count,
    purchases_count,
    revenue,
    extract(week from visit_date) as visit_week,
    extract(month from visit_date) as visit_month,
    case
        when visitors_count = 0
            then 0
        else total_cost / visitors_count
    end as cpu,
    case
        when leads_count = 0
            then 0
        else total_cost / leads_count
    end as cpl,
    case
        when purchases_count = 0
            then 0
        else total_cost / purchases_count
    end as cppu,
    case
        when total_cost = 0
            then 0
        else (revenue - total_cost) / total_cost * 100
    end as roi
from
    (select
        z2.visit_date,
        z2.source as utm_source,
        z2.medium as utm_medium,
        z2.campaign as utm_campaign,
        sum(z2.visitors_count) as visitors_count,
        coalesce(sum(z3.daily_spent), 0) as total_cost,
        sum(z2.leads_count) as leads_count,
        sum(z2.purchases_count) as purchases_count,
        sum(z2.revenue) as revenue
    from
        (select
            cast(visit_date as date) as visit_date,
            source,
            medium,
            campaign,
            content,
            count(*) as visitors_count,
            sum(lead_cnt) as leads_count,
            sum(purchase_cnt) as purchases_count,
            coalesce(sum(amount), 0) as revenue
        from
            (select
                x1.visitor_id,
                x1.visit_date,
                x1.source,
                x1.medium,
                x1.campaign,
                x1.content,
                x2.amount,
                x2.closing_reason,
                x2.status_id,
                case
                    when x2.amount is null
                        then 0
                    else 1
                end as lead_cnt, 
                case
                    when x2.closing_reason = 'Успешно реализовано' or
                         x2.status_id = 142 
                        then 1
                    else 0
                end as purchase_cnt
from
(select distinct 
   visitor_id,
   visit_date,
   source,
   medium,
   campaign,
   content 
 from sessions) as x1
   left join (select 
                visitor_id,
                visit_date,
                source,
                medium,
                campaign,
                content,
                amount,
                closing_reason,
                status_id 
              from
                (select 
                   visitor_id,
                   visit_date,
                   source,
                   medium,
                   campaign,
                   content,
                   amount,
                   closing_reason,
                   status_id,
                   paid_flag, 
                   row_number( )
                   over(partition by visitor_id 
                        order by paid_flag, visit_date desc) as num 
                    from
                      (select 
                         leads.visitor_id,
                         sessions.visit_date,
                         sessions.source,
                         sessions.medium,
                         sessions.campaign,
                         sessions.content,
                         leads.amount,
                         leads.closing_reason,
                         leads.status_id, 
                         case
                             when sessions.medium in ('cpc', 'cpm', 'cpa', 
                                                      'youtube', 'cpp', 
                                                      'tg', 'social')
                                 then 0
                             else 1
                         end as paid_flag
                       from leads 
                         inner join sessions 
                           on leads.visitor_id = sessions.visitor_id and 
                              leads.created_at >= sessions.visit_date) y1) y2
               where num = 1) as x2   
   on x1.visitor_id = x2.visitor_id and 
      x1.visit_date = x2.visit_date and 
      x1.source  = x2.source and 
      x1.medium = x2.medium and
      x1.campaign = x2.campaign and
      x1.content = x2.content) z1
   group by cast(visit_date as date), source, medium, campaign, content) z2
   left join (select 
                cast(campaign_date as date) as camp_date,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_content,
                sum(daily_spent) as daily_spent
              from
                (select
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
                 from ya_ads) x1
              group by cast(campaign_date as date),
                       utm_source,
                       utm_medium,
                       utm_campaign,
                       utm_content) z3
   on z2.visit_date = z3.camp_date and
      z2.source = z3.utm_source and
      z2.medium = z3.utm_medium and
      z2.campaign = z3.utm_campaign and
      z2.content = z3.utm_content 
   group by z2.visit_date, z2.source, z2.medium, z2.campaign
  order by sum(z2.revenue) desc nulls last, 
           z2.visit_date, 
           sum(z2.visitors_count) desc, 
           z2.source, 
           z2.medium, 
           z2.campaign) z4;

  
  