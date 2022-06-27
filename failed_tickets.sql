select 
	distinct
	c.created_at + interval '3 hours' as created_at,
	c.closed_at + interval '3 hours'as closed_at,
	datediff(hour, c.created_at, c.closed_at) as closing_hours,
	c.case_id,
	g.group_title as ticket_group,
	'https://support.kodland.org/staff/cases/chat/' || c.case_number as omni_link,
	pd.group as responsible_staff_group
from omnidesk.messages m 
join omnidesk.cases c 
	on m.case_id = c.case_id 
left join (
			select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
			from forms.personal_data_active_cs pdac 
				union
			select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
			from forms.personal_data_dismissed_cs pddc 
		) pd
		on pd.corporate_email like '%%' || c.staff_id || '%%'
left join omnidesk."groups" g 
	on g.group_id = c.group_id
where lower(m.content_html) like '%%mch-cs-go%%' and c.status = 'closed'
order by 1