select 
	distinct
	c.created_at + interval '3 hours' as created_at,
	c.closed_at + interval '3 hours'as closed_at,
	datediff(hour, c.created_at, c.closed_at) as closing_hours,
	c.case_id,
	'https://support.kodland.org/staff/cases/chat/' || c.case_number as omni_link,
	g.group_title as ticket_group,
	pd.full_name as last_resposible,
	pd.group as responsible_staff_group,
	'' as mch_responsible,
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
	pd.full_name as last_resposible,
	pd.group as responsible_staff_group,
	coalesce(pd_mch.full_name, 'unknown') as mch_responsible,
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