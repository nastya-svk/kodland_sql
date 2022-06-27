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
    and m.created_at >= '2022-01-01'
group by 1,2,3
)/*,
chats_sla as (*/
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
    case when ms.message_type = 'reply_staff'
        then 
            case when datediff(second, isnull(lag(ms.created_at) over (partition by ms.case_id order by ms.created_at), c.created_at), ms.created_at) < 0
            		  or extract(year from ms.created_at) - extract(year from isnull(lag(ms.created_at) over (partition by ms.case_id order by c.created_at), ms.created_at)) <> 0
                then
                    0
                else datediff(second, isnull(lag(ms.created_at) over (partition by ms.case_id order by ms.created_at), c.created_at), ms.created_at)
            end
        else 0
    end as sla_by_message_from_creating,
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
    case when ms.staff_id = lasr.staff_id and pre_min_sla > sla_by_response then sla_by_response else pre_min_sla end as sla_chats,
    case when sla_by_message_from_creating <= sla_by_last_online or ms.staff_id = 0 then sla_by_message_from_creating else sla_by_last_online end as pre_min_sla2,
    case when ms.staff_id = lasr.staff_id and pre_min_sla2 > sla_by_response then sla_by_response else pre_min_sla2 end as sla_chats_from_creating
from distinct_messages ms
join omnidesk.cases c
	on c.case_id = ms.case_id
left join lenta_active_staff las
    on las.staff_id = ms.staff_id and las.created_at = ms.created_at
left join lenta_active_staff_response lasr
    on lasr.created_at = ms.created_at and lasr.case_id = ms.case_id 
where c.parent_case_id = 0 and c.channel <> 'call'
and c.case_id = 207141708

    ),
triggers_sla as (
	select 
		distinct
		max(la."timestamp") over (partition by la.case_id) as last_change_respons,
		la.case_id,
		c.staff_id,
		c.closed_at + interval '3 hours' as closed_time,
		c.created_at + interval '3 hours' as created_time,
		datediff(second, last_change_respons, closed_time) as sla_triggers,
		datediff(second, created_time, closed_time) as full_sla_triggers,
		c.labels 
	from omnidesk.lenta_active la
	join omnidesk.cases c
		on la.case_id = c.case_id 
	where lower(la."action") like '%%ответственный%%'
    and lower(la."change") not like '%%- неизвестный%%' 
    and la."timestamp" < closed_time
    and (  c.labels like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 1
    		 ) || '%%'
    	or c.labels like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 2
    		 ) || '%%'
    	or c.labels like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 3
    		 ) || '%%'
    	or c.labels like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 4
    		 ) || '%%'
    	or c.labels like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 5
    		 ) || '%%')
    and c.parent_case_id = 0 and c.channel <> 'call'
    and c.status = 'closed'
),
docherki_sla as (
	select 
		distinct
		c.case_id,
		c.staff_id,
		c.closed_at + interval '3 hours' as closed_time,
		c.created_at + interval '3 hours' as created_time,
		datediff(second, created_time, closed_time) as full_sla_docherki
	from omnidesk.cases c
	left join omnidesk.labels l
 		on c.labels like '%%' || l.label_id || '%%'
 	where (( c.labels not like '%%' || (select 
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
 	    	or c.labels = '')
    and c.parent_case_id <> 0
    and c.status = 'closed'  
),
sla_frt_cases as (
	select
		distinct
		c.closed_at,
		c.case_id,
		c.user_id,
		c.staff_id,
		c.labels ,
		round(datediff(second, c.created_at, c.closed_at)::float/60,2) as full_sla_chats,
		round(sum(cs.sla_chats)::float/60,2) as sla_chats,
		round(sum(cs.sla_chats_from_creating)::float/60,2) as sla_chats_from_creating 
	from omnidesk.cases c
	join chats_sla cs
		on c.case_id = cs.case_id
	join omnidesk."groups" g 
	 	on g.group_id = c.group_id and g.group_title not similar to '%%M1%%|%%М1%%'
	where c.staff_id > 0 and c.status = 'closed' and c.deleted = false and c.spam = false
	group by 1,2,3,4,5,6
	order by 1
),
all_cases as (
select 
	distinct
	sfc.closed_at::date as closed_day,
	sfc.staff_id,
	sfc.case_id,
	'Chats' as case_type,
	'' as trigger_type,
	sfc.sla_chats as sla_minutes,
	sfc.full_sla_chats as full_sla_minutes
from sla_frt_cases sfc 
join (
		select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
		from forms.personal_data_active_cs pdac 
			union
		select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
		from forms.personal_data_dismissed_cs pddc 
	) pd
	on pd.corporate_email like '%%' || sfc.staff_id || '%%'
 left join omnidesk.labels l 
 	on sfc.labels like '%%' || l.label_id || '%%'
where ((		sfc.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 1
    		 ) || '%%'
    		and sfc.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 2
    		 ) || '%%'
    		and sfc.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 3
    		 ) || '%%'
    		and sfc.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 4
    		 ) || '%%'
    		and sfc.labels not like '%%' || (select 
    			tl.label_id
    		 from triggers_labels tl
    		 where tl.label_rank = 5
    		 ) || '%%')
	   or sfc.labels = '')
union 
select 
	distinct
	trs.closed_time::date as closed_day,
	trs.staff_id,
	trs.case_id,
	'Triggers' as case_type,
	case 
		when lower(l.label_title) like '%%дз тл п%%' or lower(l.label_title) like '%%прогул тл п%%' 
			then 'Учеником был пропущен урок'
		when lower(l.label_title) like '%%group transfer%%' 
			then 'Перевод группы/1-1 на следующий курс ****'
		when lower(l.label_title) like '%%new payments%%' 
			then 'New case from new payments'
		when lower(l.label_title) like '%%flm%%' 
			then 'First lesson missed'
	end as trigger_type,
	round(trs.sla_triggers::float/60,2) as sla_minutes,
	round(trs.full_sla_triggers::float/60,2) as full_sla_minutes
from triggers_sla trs
join (
		select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
		from forms.personal_data_active_cs pdac 
			union
		select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
		from forms.personal_data_dismissed_cs pddc 
	) pd
	on pd.corporate_email like '%%' || trs.staff_id || '%%'
left join omnidesk.labels l 
	on trs.labels like '%%' || l.label_id || '%%'
where trigger_type notnull
union 
select 
	distinct
	ds.closed_time::date as closed_day,
	ds.staff_id,
	ds.case_id,
	'Child Tickets' as case_type,
	'' as trigger_type,
	null::float as sla_minutes,
	round(ds.full_sla_docherki::float/60,2) as full_sla_minutes
from docherki_sla ds
join (
		select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
		from forms.personal_data_active_cs pdac 
			union
		select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
		from forms.personal_data_dismissed_cs pddc 
	) pd
	on pd.corporate_email like '%%' || ds.staff_id || '%%'
)
select 
	ac.closed_day,
	ac.staff_id,
	ac.case_type,
	ac.trigger_type,
	sum(ac.sla_minutes) as sla_minutes,
	sum(ac.full_sla_minutes) as full_sla_minutes,
	count(distinct ac.case_id) as tasks_sla
from all_cases ac
join omnidesk.cases c
	on c.case_id = ac.case_id and c.created_at >= '2022-05-01'
where closed_day >= '2022-05-01'
group by 1,2,3,4