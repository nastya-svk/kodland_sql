with region as (
	select 
		lf.id,
		cvr.country 
	from amocrm.leads_fact lf
	join amocrm.contacts_view_region cvr 
		on lf.contacts_id = cvr.id
)
select distinct
 date(getdate()) as download_date,
 'https://kodland.amocrm.ru/leads/detail/' ||bs.amocrm_id as amocrm_link,
 --np.course,
 np.courseset,
 bsg.title as "group",
 case
  when (case
  when bl.title like '%%Ì_.Ó%%' then right(left(bl.title, charindex('.', bl.title) - 1), charindex('.', bl.title) - 1 - charindex('Ì', bl.title))::int
  when bl.title like '%%M_.L%%' then right(left(bl.title, charindex('.', bl.title) - 1), charindex('.', bl.title) - 1 - charindex('M', bl.title))::int
  when bl.title like '%%Ì_.L%%' then right(left(bl.title, charindex('.', bl.title) - 1), charindex('.', bl.title) - 1 - charindex('Ì', bl.title))::int
  end) = 8 then 'graduate'
 else ((case
  when bl.title like '%%Ì_.Ó%%' then right(left(bl.title, charindex('.', bl.title) - 1), charindex('.', bl.title) - 1 - charindex('Ì', bl.title))::int + 1
  when bl.title like '%%M_.L%%' then right(left(bl.title, charindex('.', bl.title) - 1), charindex('.', bl.title) - 1 - charindex('M', bl.title))::int + 1
  when bl.title like '%%Ì_.L%%' then right(left(bl.title, charindex('.', bl.title) - 1), charindex('.', bl.title) - 1 - charindex('Ì', bl.title))::int + 1
  end)::varchar)
 end
 as next_module,
 np.amount_raw,
 np.pricing_package,
 'backoffice.kodland.org/ru/student_' || bs.id as backoffice_link,
 date(btfirst."time") as M1L1,
 date(dateadd(week, 1*
 (case
  when bl.title like '%%Ì_.Ó%%' then 4 - left(right(bl.title, len(bl.title) - charindex('Ó', bl.title)), 1)::int
  when bl.title like '%%M_.L%%' then 4 - left(right(bl.title, len(bl.title) - charindex('L', bl.title)), 1)::int
  when bl.title like '%%Ì_.L%%' then 4 - left(right(bl.title, len(bl.title) - charindex('L', bl.title)), 1)::int
  end)
 , btlast."time")) as L4,
 wb.balance,
 r.country
from
 kodland_shared.basics_student bs
left join finance.new_payments np on
 bs.amocrm_id = np.amocrm_id
left join kodland_shared.basics_studentgroup bsg on
 bs.group_id = bsg.id
left join finance.wallet_balance wb on
 bs.user_id = wb.user_id 
left join kodland_shared.basics_timetable btlast on
 bsg.last_timeslot_id = btlast.id
left join kodland_shared.basics_lesson bl on 
 btlast.lesson_id = bl.id
left join kodland_shared.basics_timetable btfirst on
 bsg.start_timeslot_id = btfirst.id
left join region r 
	on bs.amocrm_id = r.id
where
 bs.is_active = true
 and bs.deleted is null
 and bsg.title is not null 
 and (lower(bsg.title) not like '%%tc%%' or lower(bsg.title) like '%%scratch%%')
 and bsg.title not like '%%1-1%%'
 and lower(bsg.title) like 'online%%'
 --and (lower(bsg.title) like 'esp%%' or lower(bsg.title) like 'chi%%' or lower(bsg.title) like 'arg%%')
 --and date(L4) between date(dateadd(day, 1, getdate())) and date(dateadd(week, 1, getdate()))
 and date(L4) between '2022-06-20' and '2022-06-26'
 --and lower(bsg.title) like '%%scratch%%'
 --and m1l1 < '2022-06-01'
order by L4