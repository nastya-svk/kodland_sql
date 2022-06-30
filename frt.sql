with 
triggers_labels as (
	select 
		l.label_id,
		rank() over (order by label_id) as label_rank
	from omnidesk.labels l
	where lower(l.label_title) like '%%дз тл п%%'
	   or lower(l.label_title) like '%%прогул тл п%%'
	   or lower(l.label_title) like '%%group transfer%%' 
	   or lower(l.label_title) like '%%new payments%%' 
	   or lower(l.label_title) like '%%flm%%'
),
client_messages as (
    select 
        message_id ,
        created_at + interval '3 hours' as created_at,
        case_id
    from omnidesk.messages m
    where 
        m.user_id is not null 
        and message_type = 'reply_user'
        and m.created_at >= '2022-04-01'
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
        and m.created_at >= '2022-04-01'
),
first_client_message as (
    select
        cm.case_id, 
        min(cm.created_at) as created_at
    from client_messages cm
    where cm.created_at >= '2022-04-01' 
    group by 1
),
first_staff_message as (
    select 
    	distinct
        sm.case_id, 
        fsm.created_at,
        sm.staff_id
    from staff_messages sm 
    join (
    		select 
    			sm.case_id, 
        		min(sm.created_at) as created_at
    		from staff_messages sm
    		group by 1
    	 ) fsm
    	on sm.case_id = fsm.case_id and sm.created_at = fsm.created_at
    where sm.created_at >= '2022-01-01'
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
    lower(la."action") like '%%на линии%%' 
    and m.created_at > la."timestamp" 
    and m.message_type = 'reply_staff'
group by 1,2,3
),
lenta_active_staff_response as (
select  
	distinct
    m.staff_id, 
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
    and lower(la."action") like '%%ответственный%%'
    and lower(la."change") not like '%%- неизвестный%%' 
where 
    m.created_at > la."timestamp" 
    and m.message_type = 'reply_staff'
    and m.created_at >= '2022-04-01'
group by 1,2,3
),
staff_case_sla as (
select 
	distinct
    ms.case_id,
    ms.created_at,
    ms.message_type,
    --ms.staff_id,
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
    case 
    	when extract(hour from ms.created_at) < 8
    	then ms.created_at::date - interval '16 hours'
    	else ms.created_at::date + interval '8 hours'
    end as last_8am,
    datediff(second, last_8am, ms.created_at) as sla_by_last_8am,
    case when sla_by_message <= sla_by_last_online or ms.staff_id = 0 then sla_by_message else sla_by_last_online end as pre_min_sla,
    case when ms.staff_id = lasr.staff_id and pre_min_sla > sla_by_response then sla_by_response else pre_min_sla end as min_sla,
    case when sla_by_message <= sla_by_last_8am or ms.staff_id = 0 then sla_by_message else sla_by_last_8am end as pre_min_sla_8am
from distinct_messages ms
left join lenta_active_staff las
    on las.staff_id = ms.staff_id and las.created_at = ms.created_at
left join lenta_active_staff_response lasr
    on lasr.created_at = ms.created_at and lasr.case_id = ms.case_id 
),
scs_without_doubles as (
select 
	scs.*,
	row_number() over (partition by scs.message_type, scs.created_at order by scs.created_at, scs.min_sla desc) as rank_doubles
from staff_case_sla scs
),
min_frt as (
	select
		distinct 
		fsm.staff_id as frt_staff_id,
		scs.case_id ,
		scs.created_at,
		min_sla as frt,
		pre_min_sla as full_worktime_frt,
		pre_min_sla_8am as full_8am_frt,
		scs.sla_by_message as full_frt
	from scs_without_doubles scs
	join first_staff_message fsm 
    	on scs.case_id = fsm.case_id and fsm.created_at = scs.created_at
	where scs.message_type = 'reply_staff'
	and scs.rank_doubles = 1 
),
sla_frt_cases as (
	select
		distinct
		c.closed_at + interval '3 hours' as closed_at,
		c.case_id,
		c.user_id,
		c.staff_id,
        mf.frt_staff_id,
		round(mf.frt::float/60,2) as frt_minutes,
		round(mf.full_worktime_frt::float/60,2) as full_worktime_frt,
		round(mf.full_8am_frt::float/60,2) as full_8am_frt,
		round(mf.full_frt::float/60,2) as full_frt_minutes
	from omnidesk.cases c
	 left join min_frt mf 
	 	on mf.case_id = c.case_id 
	 join omnidesk."groups" g 
	 	on g.group_id = c.group_id and g.group_title not similar to '%%M1%%|%%М1%%'
	where c.staff_id > 0 and frt_staff_id > 0 and c.status = 'closed' and c.deleted = false and c.spam = false
	--and frt_staff_id = 39340 and c.closed_at ::date = '2022-06-11'
	order by 1
)
select 
	sfc.closed_at::date as closed_day,
    frt_staff_id,
	sum(sfc.frt_minutes) as frt_minutes,
	sum(sfc.full_worktime_frt) as full_worktime_frt_minutes,
	sum(sfc.full_8am_frt) as full_8am_frt_minutes,
	sum(sfc.full_frt_minutes) as full_frt_minutes,
	count(distinct sfc.case_id) as tasks_frt,
	'chats' as case_type
from sla_frt_cases sfc 
join omnidesk.cases c
	on c.case_id = sfc.case_id
join (
		select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
		from forms.personal_data_active_cs pdac 
			union
		select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
		from forms.personal_data_dismissed_cs pddc 
	) pd
	on pd.corporate_email like '%%' || frt_staff_id || '%%' and frt_staff_id > 0
where (c.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 1
    		 ) || '%%'
    		and c.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 2
    		 ) || '%%'
    		and c.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 3
    		 ) || '%%'
    		and c.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 4
    		 ) || '%%'
    		and c.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 5
    		 ) || '%%')
    	and c.parent_case_id = 0 and c.channel <> 'call'
		and c.created_at >= '2022-05-01'
        and c.channel <> 'web'
--and frt_staff_id = 39340 and closed_day = '2022-06-11'
group by 1,2