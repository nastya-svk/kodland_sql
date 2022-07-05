with client_messages as (
    select 
        message_id ,
        created_at + interval '3 hours' as created_at,
        case_id
    from omnidesk.messages m
    where 
        m.user_id is not null 
        and message_type = 'reply_user'
        and m.created_at >= '2022-01-01'
),
staff_messages as (
    select 
        message_id,
        created_at + interval '3 hours' as created_at,
        case_id, 
        staff_id 
    from omnidesk.messages m
    where 
        message_type = 'reply_staff'
        and m.created_at >= '2022-01-01'
),
first_client_message as (
    select
        cm.case_id, 
        min(cm.created_at) as created_at
    from client_messages cm
    where cm.created_at >= '2022-01-01' 
    group by 1
),
first_staff_message as (
    select 
        sm.case_id, 
        min(sm.created_at) as created_at
    from staff_messages sm 
    where sm.created_at >= '2022-01-01'
    group by 1
),
distinct_messages as (
select 
	distinct
	m.message_id,
	m.created_at + interval '3 hours' as created_at,
    m.case_id,
    m.message_type,
    m.staff_id
from omnidesk.messages m
join omnidesk.cases c
	on m.case_id = c.case_id 
where 
	--m.case_id = 204592180 and --temp
    message_type not like '%%note%%' 
    and message_type <> ''
    and m.created_at >= '2022-01-01'
    and c.labels not like '%%87475%%' and c.labels not like '%%89906%%'
    and m.sent_via_rule = false
),
lenta_active_staff as (
select  
    la.staff_id, 
    la."action",
    m.created_at as created_at,
    case 
        when min(datediff(second, la."timestamp",  m.created_at)) is null 
            then 0 
            else coalesce(min(datediff(second, la."timestamp",  m.created_at)),0)
        end as sla_by_last_online
from distinct_messages m
join omnidesk.lenta_active la
    on m.staff_id = la.staff_id 
where 
    lower(la."action") like '%%�� �����%%' 
    and m.created_at > la."timestamp" 
    and m.message_type = 'reply_staff'
group by 1,2,3
),
lenta_active_staff_response as (
select  
	distinct
    m.staff_id, 
    la."change",
    la.case_id,
    m.created_at as created_at,
    case 
        when min(datediff(second, la."timestamp",  m.created_at)) is null 
            then 0 
            else coalesce(min(datediff(second, la."timestamp",  m.created_at)),0)
        end as sla_by_response
from distinct_messages m
left join omnidesk.lenta_active la
    on m.case_id = la.case_id 
    and lower(la."action") like '%%�������������%%'
    and lower(la."change") not like '%%- �����������%%' 
where 
    m.created_at > la."timestamp" 
    and m.message_type = 'reply_staff'
    and m.created_at >= '2022-01-01'
group by 1,2,3,4
),
staff_case_sla as (
select 
	distinct
    ms.case_id,
    ms.created_at,
    ms.message_type,
    ms.staff_id,
    case when ms.message_type = 'reply_staff'
        then 
            case when datediff(second, isnull(lag(ms.created_at) over (partition by ms.case_id order by ms.created_at), ms.created_at), ms.created_at) < 0
            		  or extract(year from ms.created_at) - extract(year from isnull(lag(ms.created_at) over (partition by ms.case_id order by ms.created_at), ms.created_at)) <> 0
                then
                    0
                else datediff(second, isnull(lag(ms.created_at) over (partition by ms.case_id order by ms.created_at), ms.created_at), ms.created_at)
            end
        else 0
    end as sla_by_message,
    /*case when ms.message_type = 'reply_user'
        then 
            case when datediff(second, isnull(lag(ms.created_at) over (partition by ms.case_id order by ms.created_at), ms.created_at), ms.created_at) < 0
            		  or extract(year from ms.created_at) - extract(year from isnull(lag(ms.created_at) over (partition by ms.case_id order by ms.created_at), ms.created_at)) <> 0
                then
                    0
                else datediff(second, isnull(lag(ms.created_at) over (partition by ms.case_id order by ms.created_at), ms.created_at), ms.created_at)
            end
        else 0
    end as min_waiting,*/
    coalesce(las.sla_by_last_online, 0) as sla_by_last_online,
    coalesce(lasr.sla_by_response, 0) as sla_by_response,
    case when sla_by_message <= sla_by_last_online or ms.staff_id = 0 then sla_by_message else sla_by_last_online end as pre_min_sla,
    case when ms.staff_id = lasr.staff_id and pre_min_sla > sla_by_response then sla_by_response else pre_min_sla end as min_sla
from distinct_messages ms
left join lenta_active_staff las
    on las.staff_id = ms.staff_id and las.created_at = ms.created_at
left join lenta_active_staff_response lasr
    on lasr.created_at = ms.created_at and lasr.case_id = ms.case_id 
    ),
  /* select 
		ms.case_id,
		ms.staff_id,
		ms.message_type,
		ms.created_at,
		case 
			when ms.message_type = 'reply_user' and lead(ms.message_type) over (partition by ms.case_id order by ms.created_at) = 'reply_user'
			then 0
			else 1
		end as is_last_reply
	from distinct_messages ms
	order by ms.case_id, ms.created_at*/
waiting_periods as (
	select 
		ms.case_id,
		ms.staff_id,
		ms.message_type,
		case when ms.message_type = 'reply_user' and lag(ms.message_type) over (partition by ms.case_id order by ms.created_at) <> 'reply_user'
	        then 
				lag(ms.created_at) over (partition by ms.case_id order by ms.created_at) || '-' || ms.created_at
			else 
				''
		end as waiting_period,
		rank() over (partition by case_id order by ms.created_at) as rank_asc
	from distinct_messages ms
	order by ms.case_id, ms.created_at
),
json_waiting_periods as (
select 
	distinct
	case_id,
    listagg(distinct waiting_period, '; ') within group (order by rank_asc) as waiting_periods
from waiting_periods wp
where message_type = 'reply_user'
group by 1
),
sla_frt_cases as (
	select
		distinct
		c.closed_at,
		c.case_id,
		c.user_id,
		c.staff_id,
		round(sum(min_sla)::float/60,2) as sla_minutes
	from omnidesk.cases c
	join staff_case_sla scs
		on c.case_id = scs.case_id
	 left join json_waiting_periods jwp 
	 	on jwp.case_id = c.case_id
	 join omnidesk."groups" g 
	 	on g.group_id = c.group_id and g.group_title not similar to '%%M1%%|%%�1%%'
	where c.staff_id > 0 and c.status = 'closed' and c.deleted = false and c.spam = false
	group by 1,2,3,4
	order by 1
)
select 
	sfc.closed_at::date as closed_day,
	sfc.staff_id,
	sum(sfc.sla_minutes) as sla_minutes,
	count(distinct case_id) as tasks_sla
from sla_frt_cases sfc 
join (
		select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
		from forms.personal_data_active_cs pdac 
			union
		select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
		from forms.personal_data_dismissed_cs pddc 
	) pd
	on pd.corporate_email like '%%' || sfc.staff_id || '%%'
group by 1,2