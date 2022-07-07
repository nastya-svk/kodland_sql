with main as (
   with
    mm as 
    (
        select
            distinct
            c.case_id as case_id,
            c.staff_id as staff_id,
            c.created_at + interval '3 hours' as crtime,
            c.closed_at + interval '3 hours' as closed_at,
            c.group_id, 
            c.user_id
            /*case
                when
                    lag(m.created_at) over (partition by m.case_id order by m.created_at) isnull 
                    then c.created_at
                when 
                    extract(h from lag(m.created_at) over (partition by m.case_id order by m.created_at)) between 8 and 22
                    then lag(m.created_at) over (partition by m.case_id order by m.created_at)
                else null
            end as prev_time,
            lag(m.message_type) over (partition by m.case_id order by m.created_at) as prev_t*/
        from omnidesk.cases c
        join omnidesk.messages m
        on c.case_id = m.case_id and c.staff_id = m.staff_id
        where 
        	c.status = 'closed' 
        	and not m.sent_via_rule
        	and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4756') is not null 
        	and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4756') not similar to '|3'
			and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4476') not similar to '52|110'
			and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4475') not similar to '6'
			and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '5391') not similar to '11'
            and c.labels not like '%87475%' and c.labels not like '%89906%'
            and c.deleted = false and c.spam = false
        union
        select 
            distinct
            c.case_id,
            c.staff_id,
            c.created_at + interval '3 hours' as created_at,
            c.closed_at + interval '3 hours' as closed_at,
            c.group_id, 
            c.user_id
        from omnidesk.cases c
        where 
        	c.status = 'closed'
            and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4756') is not null
			and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4756') not similar to '|3'
			and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4476') not similar to '110'
			and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '4475') not similar to '6'
			and json_extract_path_text(replace(custom_fields,'''','"'), 'cf_' || '5391') not similar to '11'
            and c.labels not like '%87475%' and c.labels not like '%89906%'
            and c.channel = 'call'
            and c.deleted = false and c.spam = false
    ),
    datas as(
        select 
            mm.staff_id,
            mm.case_id,
            mm.closed_at,
            mm.group_id,
            mm.user_id
            --sum(DATEDIFF(s, prev_time, crtime)) as time_for_task,
            --sum(time_for_task) over (partition by case_id) as all_time_task,
            --count(*) as mess_count
        from mm
        where mm.staff_id > 0
        --and prev_time notnull
        --group by staff_id, mm.closed_at, mm.case_id, group_id
    ),
    all_failed_cases as (
	select 
		distinct
		c.created_at + interval '3 hours' as created_at,
		c.closed_at + interval '3 hours'as closed_at,
		datediff(hour, c.created_at, c.closed_at) as closing_hours,
		c.case_id,
		'https://support.kodland.org/staff/cases/chat/' || c.case_number as omni_link,
		g.group_title as ticket_group,
		c.staff_id as last_responsible_id,
		pd.group as responsible_staff_group,
		'' as mch_staff_id,
		'Case without responsible MCH' as case_type
	from omnidesk.messages m 
	join omnidesk.cases c 
		on m.case_id = c.case_id 
	join omnidesk.labels l
		on c.labels like '%92169%'
	left join (
				select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
				from forms.personal_data_active_cs pdac 
				where pdac.corporate_email > 0
					union
				select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
				from forms.personal_data_dismissed_cs pddc 
				where pddc.corporate_email > 0
			) pd
			on pd.corporate_email like '%%' || c.staff_id || '%%' and c.staff_id > 0
	left join omnidesk."groups" g 
		on g.group_id = c.group_id
	where c.status = 'closed'
	union
	select 
		distinct
		c.created_at + interval '3 hours' as created_at,
		c.closed_at + interval '3 hours'as closed_at,
		datediff(hour, c.created_at, c.closed_at) as closing_hours,
		c.case_id,
		'https://support.kodland.org/staff/cases/chat/' || c.case_number as omni_link,
		g.group_title as ticket_group,
		c.staff_id as last_responsible_id,
		pd.group as responsible_staff_group,
		split_part(m.content_html, ' ', 2) as mch_staff_id,
		'Case without reply from responsible MCH' as case_type
	from omnidesk.messages m 
	join omnidesk.cases c 
		on m.case_id = c.case_id 
	left join omnidesk.labels l
		on c.labels like '%92168%'
	left join (
				select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
				from forms.personal_data_active_cs pdac 
				where pdac.corporate_email > 0
					union
				select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
				from forms.personal_data_dismissed_cs pddc 
				where pddc.corporate_email > 0
			) pd
			on pd.corporate_email like '%%' || c.staff_id || '%%' and c.staff_id > 0
	left join (
				select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
				from forms.personal_data_active_cs pdac 
				where pdac.corporate_email > 0
					union
				select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
				from forms.personal_data_dismissed_cs pddc 
				where pddc.corporate_email > 0
			) pd_mch
			on pd_mch.corporate_email like '%%' || split_part(m.content_html, ' ', 2) || '%%' and split_part(m.content_html, ' ', 2) <> ''
	left join omnidesk."groups" g 
		on g.group_id = c.group_id
	where c.status = 'closed' and lower(m.content_html) like '%%mch-cs-go-2%%' and c.status = 'closed'
	order by 1
	)
    select
        d.*,
        d.closed_at::date as closed_day,
        afc.case_id as failed_case_id
    from datas d
    left join all_failed_cases afc 
    	on d.case_id = afc.case_id and d.staff_id = afc.last_responsible_id
)
select
    m.closed_day, 
    m.staff_id as staff_id_real,
    pd.corporate_email as staff_id,
    count(distinct m.user_id) as tasks,
    count(distinct m.failed_case_id) as failed_tickets
from main m
left join (
	select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date, null as dismissal_date
	from forms.personal_data_active_cs pdac 
		union
	select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date, pddc.dismissal_date
	from forms.personal_data_dismissed_cs pddc
) pd
on pd.corporate_email like '%%' || m.staff_id || '%%' and (pd.dismissal_date >= m.closed_day or pd.dismissal_date is null)
left join omnidesk."groups" g 
	on g.group_id = m.group_id
where g.group_title not similar to '%M1%|%Ì1%' and closed_day >= pd.first_date
and m.staff_id not in (31765, 37299, 37246, 37237, 31235)
group by 1, 2, 3
union 
select
    m.closed_day, 
    m.staff_id as staff_id_real,
    pd.corporate_email as staff_id,
    count(distinct m.user_id) as tasks,
    count(distinct m.failed_case_id) as failed_tickets
from main m
left join (
	select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date, null as dismissal_date
	from forms.personal_data_active_cs pdac 
		union
	select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date, pddc.dismissal_date
	from forms.personal_data_dismissed_cs pddc
) pd
on pd.corporate_email like '%%' || m.staff_id || '%%' and (pd.dismissal_date >= m.closed_day or pd.dismissal_date is null)
left join omnidesk."groups" g 
	on g.group_id = m.group_id
where g.group_title not similar to '%M1%|%Ì1%' and closed_day >= pd.first_date
and ((m.staff_id in (31765, 37299) and closed_day <= '2022-04-14')
    or (m.staff_id = 37246 and closed_day < '2022-04-20')
    or (m.staff_id = 31765 and closed_day >= '2022-04-22')
    or (m.staff_id = 37237 and closed_day <= '2022-04-21')
    or (m.staff_id = 31235 and closed_day <= '2022-04-26')
    or (m.staff_id = 31235 and closed_day >= '2022-05-01')
    or (m.staff_id = 37237 and closed_day >= '2022-05-01')
    )
group by 1, 2, 3