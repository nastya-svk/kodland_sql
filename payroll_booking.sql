with tc_status(tc_name,segment,change_date,tc_group,status, rag)  as (
 select 
  tc_name,
  null as segment,
  change_date,
  old_group,
  'Переведен' as status,
  rank() over (partition by tc_name order by change_date desc) as rank
 from forms.personal_data_mimk_change pdmc 
 join (select
        tc_name tcname,
        null as segment,
        min(change_date) cdate,
        old_group oldgr
       from forms.personal_data_mimk_change pdmc2
       group by 1, 4) as cstl
   on cstl.tcname = pdmc.tc_name 
   and pdmc.old_group = cstl.oldgr
   and pdmc.change_date = cstl.cdate
 union all
 select 
   pdam.tc_manager,
   pdam.segment,
   null,
   pdam.tc_group,
   'Работает',
   0
  from forms.personal_data_activ_mimk pdam
  union all
  select 
   pddm.tc_manager,
   null as segment,
   null,
   pddm.tc_group,
   'Уволен',
   0
  from forms.personal_data_dismissed_mimk pddm
),
deal as(
select
 np.event_time::date as tc_date ,
 np.responsible ,
 sum(np.amount_raw) as Amount,
 count(np.responsible) as payments
from
 finance.new_payments np
where
 np.responsible similar to '[A|E|I|L]{1}[0-9]%%'
group by
 1,
 2 ),
da as( select
 dateadd(day,(singles+tens+hundreds),'2022-01-01')::date as date_d ,
  mimk,
  tc_group 
 from 
 (
  SELECT 0 singles
  UNION select   1 
  UNION SELECT   2 
  UNION SELECT   3
  UNION SELECT   4 
  UNION SELECT   5 
  UNION SELECT   6
  UNION SELECT   7 
  UNION SELECT   8 
  UNION SELECT   9
 ) num_0_9,(
  SELECT 0 tens
  UNION SELECT  10 
  UNION SELECT  20 
  UNION SELECT  30
  UNION SELECT  40 
  UNION SELECT  50 
  UNION SELECT  60  
  UNION SELECT  70 
  UNION SELECT  80 
  UNION SELECT  90
 )num_0_90,(
  SELECT 0 hundreds
  UNION SELECT  100 
  UNION SELECT  200
  UNION SELECT  300
  UNION SELECT  400
  UNION SELECT  500
  UNION SELECT  600
  UNION SELECT  700
 )num_0_300
 ,(select distinct 
  ts.tc_manager as mimk, 
  ts.tc_group 
 from kodland.tc_source ts  
 where ts.tc_manager similar to '[A|E|I|L]{1}[0-9]%%'
  )mis
),
payroll_mena as (
select 
da.mimk as manager,
date_trunc('month', da.date_d)::date date_m,
sum(t.tc_completed) as completed,
sum(r.payments) as payments,
round(((sum(r.payments)::float/nullif(sum(t.tc_completed),0))*100),0) as A2P,
round((sum(r.Amount)::float/nullif(sum(r.payments),0)),0) as sred,
pdam3.region,
case 
    when pdam3.segment = 'MENAP Department' and pdam3.region = 'Turkey' then 11
    when pdam3.segment = 'MENAP Department' and pdam3.region = 'UAE' then 12
    else 2 
end as segment,
case
    when pdam3.segment = 'MENAP Department' and pdam3.region = 'Turkey' then ((sum(t.tc_completed))*50) 
    when pdam3.segment = 'MENAP Department' and pdam3.region = 'UAE' then ((sum(t.tc_completed))*50)
    else (sum(t.tc_completed)*50) 
end as Base,
case
    when pdam3.segment = 'MENAP Department' and pdam3.status = 'Turkey' 
    then isnull((sum(r.Amount)*(case when A2P < 35 then 0.1 else 0.15 end)),0)
    when pdam3.segment = 'MENAP Department' and pdam3.status = 'UAE' 
    then isnull((sum(r.Amount)*(case when A2P < 35 then 0.1 else 0.15 end)),0)
    else 0 
end as Bonus,
(Base + Bonus) as award,
case 
	when pdam3.segment = 'MENAP Department' and pdam3.region = 'Turkey' then 'TL'
    when pdam3.segment = 'MENAP Department' and pdam3.region = 'UAE' then 'AED'
    else 'TL'
end
as cur
from kodland.tc_source t
right join da
on
 t.tc_manager = da.mimk
 and da.date_d = t.tc_date
left join deal r 
on
 r.tc_date = da.date_d
 and r.responsible = da.mimk
left join
  forms.personal_data_activ_mimk pdam3 
 on pdam3.tc_manager = da.mimk
left join forms.personal_data_group_match pdgm 
 on da.tc_group = pdgm.tc_group
where
 da.date_d > '2022-04-01'
 and da.date_d <= current_date and not (t.tc_completed = 0 and r.Amount=0  and r.payments =0)
 and pdgm.department = 'MENAP department'
 and date_m notnull
group by
da.mimk,
date_m,
pdam3.segment,
pdam3.region,
pdam3.status
order by 1
),
payroll_europe as (
select 
da.mimk as manager,
date_trunc('month', da.date_d)::date date_m,
sum(t.tc_completed) as completed,
sum(r.payments) as payments,
round(((sum(r.payments)::float/nullif(sum(t.tc_completed),0))*100),0) as A2P,
round((sum(r.Amount)::float/nullif(sum(r.payments),0)),0) as sred,
pdam3.region,
case 
    when pdam3.segment = 'Europe Department' and pdam3.region = 'Poland' then 7
    when pdam3.segment = 'Europe Department' and pdam3.region = 'Italy' then 9
    else 2 
end as segment,
case
    when pdam3.segment = 'Europe Department' and pdam3.region = 'Poland' then ((sum(t.tc_completed))*32) 
    when pdam3.segment = 'Europe Department' and pdam3.region = 'Italy' then ((sum(t.tc_completed))*7.26)
    else (sum(t.tc_completed)*2) 
end as Base,
case
    when pdam3.segment = 'Europe Department' and pdam3.status = 'Poland' then isnull((sum(r.Amount)*0.1),0)
    when pdam3.segment = 'Europe Department' and pdam3.status = 'Italy' then isnull((sum(r.Amount)*0.1),0)
    else 0 
end as Bonus,
(Base + Bonus) as award,
case 
	when pdam3.segment = 'Europe Department' and pdam3.region = 'Poland' then 'PLN'
    when pdam3.segment = 'Europe Department' and pdam3.region = 'Italy' then 'EURO'
    else 'EURO'
end
as cur
from kodland.tc_source t
right join da
on
 t.tc_manager = da.mimk
 and da.date_d = t.tc_date
left join deal r 
on
 r.tc_date = da.date_d
 and r.responsible = da.mimk
left join
  forms.personal_data_activ_mimk pdam3 
 on pdam3.tc_manager = da.mimk
left join forms.personal_data_group_match pdgm 
 on da.tc_group = pdgm.tc_group
where
 da.date_d > '2022-04-01'
 and da.date_d <= current_date and not (t.tc_completed = 0 and r.Amount=0  and r.payments =0)
 and pdgm.department = 'Europe department'
 and date_m notnull
group by
da.mimk,
date_m,
pdam3.segment,
pdam3.region,
pdam3.status
order by 1
),
PayrollAsia as (
select 
da.mimk as manager,
date_trunc('month', da.date_d)::date date_m,
sum(t.tc_completed) as completed,
sum(r.payments) as payments,
(Round(((sum(r.payments)::float/nullif(sum(t.tc_completed),0))*100),0)) as A2P,
Round((sum(r.Amount)::float/nullif(sum(r.payments),0)),0) as sred,
pdam3.region,
case 
       when pdam3.segment = 'Asia Department' and pdam3.region = 'Indonesia' then 4
       when pdam3.segment = 'Asia Department' and pdam3.region = 'Philippines' then 5
       else 2 end as segment,
case
       when pdam3.segment = 'Asia Department' and pdam3.region = 'Indonesia' then ((sum(t.tc_completed))*2) 
     when pdam3.segment = 'Asia Department' and pdam3.region = 'Philippines' then ((sum(t.tc_completed))*3)
     else (sum(t.tc_completed)*2) end as Base,
case
   when pdam3.segment = 'Asia Department' and pdam3.status = 'Indonesia' then isnull((sum(r.Amount)*0.1),0)
       when pdam3.segment = 'Asia Department' and pdam3.status = 'Indonesia(Old)' then isnull((sum(r.Amount)*0.15),0) 
       when pdam3.segment = 'Asia Department' and pdam3.status = 'Philippines' then isnull((sum(r.Amount)*0.1),0)
       else 0 end as Bonus,
(Base + Bonus) as award,
'USD' as cur
from kodland.tc_source t
right join da
on
 t.tc_manager = da.mimk
 and da.date_d = t.tc_date
left join deal r 
on
 r.tc_date = da.date_d
 and r.responsible = da.mimk
left join
  forms.personal_data_activ_mimk pdam3 
 on pdam3.tc_manager = da.mimk
left join forms.personal_data_group_match pdgm 
 on da.tc_group = pdgm.tc_group
where
 da.date_d > '01-04-2022'
 and da.date_d <= current_date and not (t.tc_completed = 0 and r.Amount=0  and r.payments =0)
 and pdgm.department = 'Asia department'
 and date_m notnull
 and da.date_d > '2022-04-01'
group by
da.mimk,
date_m,
pdam3.segment,
pdam3.region,
pdam3.status
order by 1
),
Payroll as (
 select
        date_trunc('month', ts.tc_date )::date  date_month,
        ts.tc_manager as manager,
        sum(ts.tc_completed),
        (Round(((sum(payments_count)::float/nullif(sum(tc_completed),0))*100),0)) as A2P,
     case 
        when pdam2.segment = 'Russian Department' then 'RUB'
        when pdam2.segment = 'Asia Department' then 'USD'
        else 'USD' end as Currency,
     case 
        when pdam2.region = 'Russia' then 'СЗ РФ' 
       when pdam2.region = 'CIS' then 'Deel'
      else 'ИП' end as Status,
     case 
       when pdam2.segment = 'Russian Department' then 1
       when pdam2.segment = 'Asia Department' and pdam2.region = 'Indonesia' then 4
       when pdam2.segment = 'Asia Department' and pdam2.region = 'Philippines' then 5
       else 2 end as segment,
     case
       when pdam2.segment = 'Russian Department' then (sum(tc_completed)*225) 
       when pdam2.segment = 'Asia Department' and pdam2.region = 'Indonesia' then ((sum(tc_completed))*2) 
     when pdam2.segment = 'Asia Department' and pdam2.region = 'Philippines' then ((sum(tc_completed))*3)
     else (sum(tc_completed)*2) end as Base,
    case 
      when pdam2.segment = 'Russian Department' then
      isnull((sum(amount_all)*(case 
        when A2P < 25 then 0
        when (A2P >= 25) and (A2P <= 29) then 0.05
        when (A2P >= 30) and (A2P <= 34) then 0.125
        when (A2P >= 35) and (A2P <= 39) then 0.15
        when (A2P >= 40) and (A2P <= 44) then 0.16
        when (A2P >= 45) and (A2P <= 49) then 0.18
             else 0.2 end)),0) 
      else 0 end as Bonus
 from kodland.tc_source ts 
 left join
  forms.personal_data_activ_mimk pdam2
   on ts.tc_manager = pdam2.tc_manager
where pdam2.segment = 'Russian Department'
group by 
 date_month,
    ts.tc_manager,
    pdam2.region,
    pdam2.segment,
    pdam2.status,
    pdam2.tc_group,
    currency
),
id_back as (
 select distinct
 bb.mimk as id_mimk,
 bb.mimk_name as mimk_name
 from booking.backoffice_bookings bb 
 where bb.mimk notnull
),
ru_main as (
select 
 id_back.id_mimk as user_id,
 payroll.segment as segment_id,
 ts.tc_manager as tcm_name,
 date_trunc('month', date_month) as date_month,
 sum(tc_completed) as cnt_successful_lessons,
 sum(payments_count) as cnt_payments,
 Round((sum(amount_all)::float/nullif(cnt_payments,0)),0) as avg_check,
 Round((sum(quality_tc_sum)::float/nullif(sum(quality_tc_count),0)),3) as avg_quality,
 Round((cnt_payments::float/nullif(cnt_successful_lessons,0)),3) as avg_a2p,
 Round((case when tsl.payment_month < date_trunc('month', current_date )::date and tsl.payment_total notnull then tsl.payment_total
      else
    case 
     when payroll.status = 'LATAM(dollar)' then (payroll.base+ payroll.bonus)
     else (payroll.base+ payroll.bonus) end
    end),0) as award,
  --payroll.base,
 -- payroll.bonus,
 case when tsl.payment_month < date_trunc('month', current_date )::date
    then tsl.payment_currency
    else payroll.currency end as currency
from
 kodland.tc_source ts
left join id_back
 on id_back.mimk_name = ts.tc_manager 
left join tc_status
 on tc_status.tc_name = ts.tc_manager
left join Payroll
 on Payroll.manager = ts.tc_manager
 and Payroll.date_month = date_trunc('month', ts.tc_date)::date
left join(
 select  
  tsl.tc_manager,
  dateadd(month, (tsl.payment_period - 1)::int, date_trunc('year', tsl.payment_date)::date)::date payment_month,
  sum(tsl.payment_total) as payment_total,
  tsl.payment_currency
 from
  finance.tc_salaries_list tsl
 group by payment_month,tsl.tc_manager,tsl.payment_currency) tsl
  on tsl.tc_manager = ts.tc_manager
  and tsl.payment_month = date_trunc('month', date_month)::date
 where
 ts.tc_date <= current_date
  and not (ts.tc_group = 'Unknown')
  and date_month > '2022-04-01'
  and tc_status.status = 'Работает'
  and tc_status.segment in ('Russian Department')
 group by
   ts.tc_manager,
   date_month,
   payroll.bonus,
   payroll.base,
   payroll.status,
   tsl.payment_month,
   tsl.payment_total,
   payroll.currency,
   id_back.id_mimk,
   payroll.segment,
   tsl.payment_currency
 order by 
    tcm_name,
    date_month desc
 ),
 asia_main as (
 	select 
 id_back.id_mimk as user_id,
 payrollasia.segment as segment_id,
 payrollasia.manager as tcm_name,
 date_trunc('month', date_m) as date_month,
 payrollasia.completed as cnt_successful_lessons,
 payrollasia.payments as cnt_payments,
 payrollasia.sred as avg_check,
Round((sum(quality_tc_sum)::float/nullif(sum(quality_tc_count),0)),3) as avg_quality,
Round((cnt_payments::float/nullif(cnt_successful_lessons,0)),3) as avg_a2p,
Round(coalesce(payrollasia.base, 0) + coalesce(payrollasia.bonus, 0), 0) as award,
-- payrollasia.base,
-- payrollasia.bonus,
 case when tsl.payment_month < date_trunc('month', current_date)::date
    then tsl.payment_currency
    else 'USD' end as currency
from
 kodland.tc_source ts
left join id_back
 on id_back.mimk_name = ts.tc_manager
left join PayrollAsia
 on PayrollAsia.manager = ts.tc_manager
 and PayrollAsia.date_m = date_trunc('month', ts.tc_date)::date
left join(
 select  
  tsl.tc_manager,
  dateadd(month, (tsl.payment_period - 1)::int, date_trunc('year', tsl.payment_date)::date)::date payment_month,
  --(date_trunc('year', tsl.payment_date::date)::date + interval '1 month'*(tsl.payment_period - 1))::date  payment_month,
  sum(tsl.payment_total) as payment_total,
  tsl.payment_currency
 from
  finance.tc_salaries_list tsl
 group by payment_month,tsl.tc_manager,tsl.payment_currency
) tsl
  on tsl.tc_manager = ts.tc_manager 
  and tsl.payment_month = date_trunc('month', date_m)
 where
  ts.tc_date <= current_date
  and not (ts.tc_group = 'Unknown')
  and ts.tc_date > '2022-04-01'
  and payrollasia.segment in (4, 5)
  and award notnull 
 group by
   date_m,
   payrollasia.bonus,
   payrollasia.base,
   tsl.payment_month,
   tsl.payment_total,
   id_back.id_mimk,
   payrollasia.segment,
   payrollasia.manager,
   payrollasia.completed,
   payrollasia.sred,
   payrollasia.payments,
   tsl.payment_currency,
   payrollasia.award
 order by 
    tcm_name,
    date_m desc
),
europe_main as (
	select 
		id_back.id_mimk as user_id,
		pe.segment as segment_id,
		pe.manager as tcm_name,
		date_trunc('month', date_m) as date_month,
		pe.completed as cnt_successful_lessons,
		pe.payments as cnt_payments,
		pe.sred as avg_check,
		round((sum(quality_tc_sum)::float/nullif(sum(quality_tc_count),0)),3) as avg_quality,
		round((cnt_payments::float/nullif(cnt_successful_lessons,0)),3) as avg_a2p,
		round(coalesce(pe.base, 0) + coalesce(pe.bonus, 0), 0) as award,
		case 
			when tsl.payment_month < date_trunc('month', current_date)::date
		    then coalesce(tsl.payment_currency, pe.cur)
		    else pe.cur
		end as currency
	from
	 kodland.tc_source ts
	left join id_back
	 on id_back.mimk_name = ts.tc_manager
	left join payroll_europe pe
	 on pe.manager = ts.tc_manager
	 and pe.date_m = date_trunc('month', ts.tc_date)::date
	left join(
	 select  
	  tsl.tc_manager,
	  dateadd(month, (tsl.payment_period - 1)::int, date_trunc('year', tsl.payment_date)::date)::date payment_month,
	  --(date_trunc('year', tsl.payment_date::date)::date + interval '1 month'*(tsl.payment_period - 1))::date  payment_month,
	  sum(tsl.payment_total) as payment_total,
	  tsl.payment_currency
	 from
	  finance.tc_salaries_list tsl
	 group by payment_month,tsl.tc_manager,tsl.payment_currency
	) tsl
	  on tsl.tc_manager = ts.tc_manager 
	  and tsl.payment_month = date_trunc('month', date_m)
	 where
	  ts.tc_date <= current_date
	  and not (ts.tc_group = 'Unknown')
	  and ts.tc_date > '2022-04-01'
	  and pe.segment in (7, 9)
	  and award notnull 
	 group by
	   date_m,
	   pe.bonus,
	   pe.base,
	   tsl.payment_month,
	   tsl.payment_total,
	   id_back.id_mimk,
	   pe.segment,
	   pe.manager,
	   pe.completed,
	   pe.sred,
	   pe.payments,
	   tsl.payment_currency,
	   pe.award,
	   pe.cur
	 order by 
	    tcm_name,
	    date_m desc
 ),
 mena_main as (
	select 
		id_back.id_mimk as user_id,
		pm.segment as segment_id,
		pm.manager as tcm_name,
		date_trunc('month', date_m) as date_month,
		pm.completed as cnt_successful_lessons,
		pm.payments as cnt_payments,
		pm.sred as avg_check,
		round((sum(quality_tc_sum)::float/nullif(sum(quality_tc_count),0)),3) as avg_quality,
		round((cnt_payments::float/nullif(cnt_successful_lessons,0)),3) as avg_a2p,
		round(coalesce(pm.base, 0) + coalesce(pm.bonus, 0), 0) as award,
		case 
			when tsl.payment_month < date_trunc('month', current_date)::date
		    then coalesce(tsl.payment_currency, pm.cur)
		    else pm.cur
		end as currency
	from
	 kodland.tc_source ts
	left join id_back
	 on id_back.mimk_name = ts.tc_manager
	left join payroll_mena pm
	 on pm.manager = ts.tc_manager
	 and pm.date_m = date_trunc('month', ts.tc_date)::date
	left join(
	 select  
	  tsl.tc_manager,
	  dateadd(month, (tsl.payment_period - 1)::int, date_trunc('year', tsl.payment_date)::date)::date payment_month,
	  --(date_trunc('year', tsl.payment_date::date)::date + interval '1 month'*(tsl.payment_period - 1))::date  payment_month,
	  sum(tsl.payment_total) as payment_total,
	  tsl.payment_currency
	 from
	  finance.tc_salaries_list tsl
	 group by payment_month,tsl.tc_manager,tsl.payment_currency
	) tsl
	  on tsl.tc_manager = ts.tc_manager 
	  and tsl.payment_month = date_trunc('month', date_m)
	 where
	  ts.tc_date <= current_date
	  and not (ts.tc_group = 'Unknown')
	  and ts.tc_date > '2022-04-01'
	  and pm.segment in (11, 12)
	  and award notnull 
	 group by
	   date_m,
	   pm.bonus,
	   pm.base,
	   tsl.payment_month,
	   tsl.payment_total,
	   id_back.id_mimk,
	   pm.segment,
	   pm.manager,
	   pm.completed,
	   pm.sred,
	   pm.payments,
	   tsl.payment_currency,
	   pm.award,
	   pm.cur
	 order by 
	    tcm_name,
	    date_m desc
 )
 select distinct *
 from asia_main
 union
 select distinct *
 from ru_main
 union
 select distinct *
 from europe_main
 union
 select distinct *
 from mena_main