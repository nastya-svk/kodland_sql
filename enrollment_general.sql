with students_enrol as
(
 select 
  distinct
  date(min(define_time)) enrol_date,
  amocrm_id,
  bs2.id,
  bs2.group_id,
  bs2.user_id,
  bs2.first_name || ' ' || bs2.last_name as student_name
 from
  kodland_shared.basics_student bs2
 join
  kodland_shared.basics_studentanalytics bs
 on
  bs.student_id = bs2.id 
 where 
  bs.is_active-- and 
 group by 2,3,4,5,6
),
/*students_start as
(
 select 
  distinct
  date(min(define_time)) start_date,
  amocrm_id,
  bs2.id
 from
  kodland_shared.basics_studentanalytics bs 
 join
  kodland_shared.basics_student bs2
 on
  bs.student_id = bs2.id 
 where 
  bs.is_active and last_ls_id notnull and bs2.created >= '2022-06-01'
 group by 2,3
)*/
data_cl as (
select 
 distinct 
 se.enrol_date,
 se.student_name,
 se.id as bo_student_id,
 cl.student_id as clo_student_id,
 ('https://kodland.amocrm.ru/leads/detail/' || se.amocrm_id) as enrolled_amo,
 np.amocrm_id as new_payments_amo,
 case 
  when datediff(day, bt."time"::date, wt.transaction_date) > 6
   then wt.transaction_date
   else 
    case 
     when bt."time"::date notnull
      then bt."time"::date 
      else btfirst."time"::date
    end
 end as first_lesson,
 case 
  when pay_order_date > np.event_time or pay_order_date isnull
   then
    np.event_time
   else
    case 
     when pay_order_date::date > first_lesson ::date
      then (select 
        min(wt.transaction_date) as first_add
         from finance.wallet_transaction wt
         where wt.type = 'add' and wt.user_id = se.user_id) 
      else pay_order_date
    end
 end as pay_date,
 datediff(day, pay_date, first_lesson) as pay_m1l1_gap,
 --datediff(day, pay_date, enrol_date) as pay_enrol_gap,
 case 
  when lower(lf.pipeline) similar to '%%italy%%|%%poland%%' then 'Europe'
  when lower(lf.pipeline) similar to '%%asia%%|%%indonesia%%|%%philippines%%' then 'Asia'
  when lower(lf.pipeline) similar to '%%menap%%|%%turkey%%|%%uae%%' then 'Mid-East & North Africa'
  when lower(lf.pipeline) similar to '%%latam%%' then 'Latin America'
  when lower(lf.pipeline) similar to '%%english%%' then 'English'
  when lower(lf.pipeline) similar to '%%онлайн%%' then 'CIS'
 end as department,
 case 
  when cl.order_link like '%%backoffice%%' then 'easy_payments'
  else 'not_ep'
 end as ep
from (
 select * from(
 select 
  distinct
  c.payed_datetime as pay_order_date,
  c.student_id,
  c.order_link,
  row_number() over (partition by c.student_id order by pay_order_date) as rn
 from finance.clientorder c
 where c.is_payed = true
 ) cl
 where cl.rn = 1) cl
left join students_enrol se
 on se.id = cl.student_id and se.amocrm_id > 0
left join 
 (select
  max(np.event_time) as event_time,
  np.amocrm_id
 from finance.new_payments np 
 where np.responsible not similar to 'ISM%%||МВП%%'
 group by 2
 ) np
 on np.amocrm_id = se.amocrm_id
left join amocrm.leads_fact lf 
 on lf.id = se.amocrm_id
left join kodland_shared.basics_studentgroup bsg 
 on bsg.id = se.group_id
left join kodland_shared.basics_timetable btfirst 
 on bsg.start_timeslot_id = btfirst.id
left join ( 
 select
  wt.user_id,
  min(wt.transaction_date) as first_substract
 from finance.wallet_transaction wt 
 where wt.type = 'subtract'
 group by 1
 ) wt_fisrt
 on wt_fisrt.user_id = se.user_id
left join finance.wallet_transaction wt
 on wt_fisrt.first_substract = wt.transaction_date and wt_fisrt.user_id = wt.user_id
left join kodland_shared.basics_timetable bt 
 on bt.lesson_id = wt.lesson_id and bt.group_id = wt.group_id
where cl.pay_order_date >= '2022-06-01'
and se.id = 672627
)
--where --and se.amocrm_id = 33591444 
--bt."time"::date <= getdate()
--and se.id = 593126
select 
 se.enrol_date,
 se.student_name,
 se.id as bo_student_id,
 null as clo_student_id,
 ('https://kodland.amocrm.ru/leads/detail/' || se.amocrm_id) as enrolled_amo,
 np.amocrm_id as new_payments_amo,
 case 
  when datediff(day, bt."time"::date, wt.transaction_date) > 6 and wt.transaction_date notnull
   then wt.transaction_date
   else 
    case 
     when bt."time"::date notnull
      then bt."time"::date 
      else btfirst."time"::date
    end
 end as first_lesson,
 case 
  when np.event_time::date > first_lesson ::date
   then (select 
     min(wt.transaction_date) as first_add
      from finance.wallet_transaction wt
      where wt.type = 'add' and wt.user_id = se.user_id) 
   else np.event_time::date 
 end as pay_date,
 datediff(day, pay_date, first_lesson) as pay_m1l1_gap,
 --datediff(day, pay_date, enrol_date) as pay_enrol_gap,
 case 
  when lower(lf.pipeline) similar to '%%italy%%|%%poland%%' then 'Europe'
  when lower(lf.pipeline) similar to '%%asia%%|%%indonesia%%|%%philippines%%' then 'Asia'
  when lower(lf.pipeline) similar to '%%menap%%|%%turkey%%|%%uae%%' then 'Mid-East & North Africa'
  when lower(lf.pipeline) similar to '%%latam%%' then 'Latin America'
  when lower(lf.pipeline) similar to '%%english%%' then 'English'
  when lower(lf.pipeline) similar to '%%онлайн%%' then 'CIS'
 end as department,
 'not ep' as ep
from (select
  max(np.event_time) as event_time,
  np.amocrm_id
 from finance.new_payments np 
 where np.responsible not similar to 'ISM%%||МВП%%'
 group by 2
 ) np
left join students_enrol se
 on se.amocrm_id = np.amocrm_id and se.amocrm_id > 0
left join amocrm.leads_fact lf 
 on lf.id = np.amocrm_id
left join kodland_shared.basics_studentgroup bsg 
 on bsg.id = se.group_id
left join kodland_shared.basics_timetable btfirst 
 on bsg.start_timeslot_id = btfirst.id --первый урок группы
left join ( 
 select
  wt.user_id,
  min(wt.transaction_date) as first_substract
 from finance.wallet_transaction wt 
 where wt.type = 'subtract'
 group by 1
 ) wt_fisrt
 on wt_fisrt.user_id = se.user_id
left join finance.wallet_transaction wt
 on wt_fisrt.first_substract = wt.transaction_date and wt_fisrt.user_id = wt.user_id
left join kodland_shared.basics_timetable bt 
 on bt.lesson_id = wt.lesson_id and bt.group_id = wt.group_id --первый урок по кошельку студента
where np.event_time >= '2022-06-01' 
--and se.id = 672627 --and 'https://kodland.amocrm.ru/leads/detail/33803274' not in (select dc.enrolled_amo from data_cl dc)
and ('https://kodland.amocrm.ru/leads/detail/' || np.amocrm_id) not in (select dc.enrolled_amo from data_cl dc)