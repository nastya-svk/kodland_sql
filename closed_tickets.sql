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
    )
    select
        d.*,
        d.closed_at::date as closed_day
    from datas d
)
select
    m.closed_day, 
    m.staff_id as staff_id_real,
    pd.corporate_email as staff_id,
    count(distinct m.user_id) as tasks
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
    count(distinct m.user_id) as tasks
from main m
left join (
	select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
	from forms.personal_data_active_cs pdac 
		union
	select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
	from forms.personal_data_dismissed_cs pddc 
) pd
on pd.corporate_email like '%%' || m.staff_id || '%%'
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