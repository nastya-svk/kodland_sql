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
distinct_messages as (
select 
	distinct
	m.message_id,
	m.created_at + interval '3 hours' as created_at,
  m.case_id,
  m.message_type,
  m.staff_id,
  case when m.message_type = 'reply_staff' or m.message_type = 'note_regular'
        then 
            case when datediff(second, isnull(lag(m.created_at) over (partition by m.case_id order by m.created_at), m.created_at), m.created_at) < 0
            		  or extract(year from m.created_at) - extract(year from isnull(lag(m.created_at) over (partition by m.case_id order by m.created_at), m.created_at)) <> 0
                then
                    0
                else datediff(second, isnull(lag(m.created_at) over (partition by m.case_id order by m.created_at), m.created_at), m.created_at)
            end
        else 0
    end as reply_sec
from (select distinct * from omnidesk.messages m) m
join omnidesk.cases c
	on m.case_id = c.case_id 
where 
    message_type <> ''
    and m.created_at >= '2022-01-01'
    and m.sent_via_rule = false
),
first_staff_message as (
    select 
    	distinct
        sm.case_id, 
        fsm.created_at,
        sm.staff_id
    from distinct_messages sm
    join (
    		select 
    			sm.case_id, 
        	min(sm.created_at) as created_at
    		from distinct_messages sm
    		where message_type = 'reply_staff' or message_type = 'note_regular'
    		group by 1
    	 ) fsm
    	on sm.case_id = fsm.case_id and sm.created_at = fsm.created_at
    where sm.created_at >= '2022-04-01' and (message_type = 'reply_staff' or message_type = 'note_regular')
),
frt as (
select 
	distinct
	c.case_id,
	ms.reply_sec as frt_sec
from omnidesk.cases c
join distinct_messages ms 
	on ms.case_id = c.case_id 
join first_staff_message fsm 
    	on ms.case_id = fsm.case_id and fsm.created_at = ms.created_at
),
bad_chats as (
(select
	distinct
	(c.closed_at + interval '3 hours')::date as closed_day,
	c.case_id,
	c.case_number,
	c.staff_id,
	'Chats without staff working' as case_type
from omnidesk.cases c 
join (
			select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
			from forms.personal_data_active_cs pdac 
				union
			select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
			from forms.personal_data_dismissed_cs pddc 
		) pd
		on pd.corporate_email like '%%' || c.staff_id || '%%' and c.staff_id > 0
join omnidesk."groups" g 
	on g.group_id = c.group_id and g.group_title not similar to '%%M1%%|%%М1%%'
where c.parent_case_id = 0 and c.channel <> 'call'
and ((		c.labels not like '%%' || (select 
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
	   or c.labels = '') and c.channel <> 'web'
	   and c.labels not like '%%87475%%' and c.labels not like '%%89906%%' and c.labels not like '%%81345%%' and c.labels not like '%%93174%%'
      and c.labels not like '%%93648%%' and c.labels not like '%%79903%%' and c.labels not like '%%93022%%'  
     and c.status = 'closed'
     and c.closed_at >= '2022-06-01'
     and json_extract_path_text(replace(c.custom_fields,'''','"'), 'cf_' || '4476') not similar to '52'
except 
select 
	distinct 
	(c.closed_at + interval '3 hours')::date,
	c.case_id,
	c.case_number,
	c.staff_id,
	'Chats without staff working'
from omnidesk.messages m 
join omnidesk.cases c 
	on c.case_id = m.case_id 
join frt
	on frt.case_id = c.case_id 
join (
			select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
			from forms.personal_data_active_cs pdac 
				union
			select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
			from forms.personal_data_dismissed_cs pddc 
		) pd
		on pd.corporate_email like '%%' || m.staff_id || '%%' and m.staff_id > 0
)
union 
select 
	distinct 
	(c.closed_at + interval '3 hours')::date,
	c.case_id,
	c.case_number,
	c.staff_id,
	'Chats where FRT > 5 hours'
from omnidesk.messages m 
join omnidesk.cases c 
	on c.case_id = m.case_id 
join frt
	on frt.case_id = c.case_id 
join (
			select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
			from forms.personal_data_active_cs pdac 
				union
			select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
			from forms.personal_data_dismissed_cs pddc 
		) pd
		on pd.corporate_email like '%%' || m.staff_id || '%%' and m.staff_id > 0
where c.parent_case_id = 0 and c.channel <> 'call'
and ((		c.labels not like '%%' || (select 
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
	   or c.labels = '') and c.channel <> 'web'
	   and c.labels not like '%%87475%%' and c.labels not like '%%89906%%' and c.labels not like '%%81345%%' and c.labels not like '%%93174%%'
      and c.labels not like '%%93648%%' and c.labels not like '%%79903%%' and c.labels not like '%%93022%%'  
     and c.status = 'closed'
     and c.closed_at >= '2022-06-01'
     and json_extract_path_text(replace(c.custom_fields,'''','"'), 'cf_' || '4476') not similar to '52'
     and frt.frt_sec/3600 >= 5
union
select 
	distinct 
	(c.closed_at + interval '3 hours')::date,
	c.case_id,
	c.case_number,
	c.staff_id,
	'Chats without result' as case_type
from omnidesk.messages m 
join omnidesk.cases c 
	on c.case_id = m.case_id 
join (
			select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
			from forms.personal_data_active_cs pdac 
				union
			select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
			from forms.personal_data_dismissed_cs pddc 
		) pd
		on pd.corporate_email like '%%' || m.staff_id || '%%' and m.staff_id > 0
join (
			select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
			from forms.personal_data_active_cs pdac 
				union
			select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
			from forms.personal_data_dismissed_cs pddc 
		) pd_case
		on pd_case.corporate_email like '%%' || c.staff_id || '%%' and c.staff_id > 0
join omnidesk."groups" g 
	on g.group_id = c.group_id and g.group_title not similar to '%%M1%%|%%М1%%'
where (json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4756') is null
	or json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4756') = '')
and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4476') <> '52'
and c.closed_at >= '2022-06-01'
and c.parent_case_id = 0 and c.channel <> 'call'
and ((		c.labels not like '%%' || (select 
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
	   or c.labels = '') and c.channel <> 'web'
),
bad_tickets as (
select
	distinct
	(c.closed_at + interval '3 hours')::date as closed_day,
	c.case_id,
	c.case_number,
	c.staff_id,
	'Other tickets without staff working' as case_type
from omnidesk.cases c 
join (
			select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
			from forms.personal_data_active_cs pdac 
				union
			select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
			from forms.personal_data_dismissed_cs pddc 
		) pd
		on pd.corporate_email like '%%' || c.staff_id || '%%' and c.staff_id > 0
join omnidesk."groups" g 
	on g.group_id = c.group_id and g.group_title not similar to '%%M1%%|%%М1%%'
where c.parent_case_id = 0 and c.channel <> 'call'
		 and case_id not in (select case_id from bad_chats)
	   and c.labels not like '%%87475%%' and c.labels not like '%%89906%%' and c.labels not like '%%81345%%' and c.labels not like '%%93174%%'
     and c.labels not like '%%93648%%' and c.labels not like '%%79903%%' and c.labels not like '%%93022%%'  
     and c.status = 'closed'
     and c.closed_at >= '2022-06-01'
     and json_extract_path_text(replace(c.custom_fields,'''','"'), 'cf_' || '4476') not similar to '52'
except 
select 
	distinct 
	(c.closed_at + interval '3 hours')::date,
	c.case_id,
	c.case_number,
	c.staff_id,
	'Other tickets without staff working'
from omnidesk.messages m 
join omnidesk.cases c 
	on c.case_id = m.case_id 
join (
			select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
			from forms.personal_data_active_cs pdac 
				union
			select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
			from forms.personal_data_dismissed_cs pddc 
		) pd
		on pd.corporate_email like '%%' || m.staff_id || '%%' and m.staff_id > 0
)
select
	distinct
	bc.closed_day,
	bc.staff_id,
	bc.case_type,
	'https://support.kodland.org/staff/cases/chat/' || bc.case_number as omni_link
from (select * from bad_chats bc
			union
			select * from bad_tickets bt) bc