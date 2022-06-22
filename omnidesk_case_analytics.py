import psycopg2
import pandas as pd
import datetime as dt

from timeit import default_timer as timer
import numpy as np
from sqlalchemy import create_engine

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe as gd

HOST = 'kodland-dwh.crcdtnutgfzg.eu-west-1.redshift.amazonaws.com'
PORT = 5439
DBNAME = 'dev'
USER = 'kodland_admins'
PASSWORD = 'sr;lO#6uhBGV'

json_service_account = {
    "type": "service_account",
    "project_id": "access-to-tables-317614",
    "private_key_id": "f95e305d4665c2fcde6e9c710d2b7bece7db0e08",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDN+/pGos4kVpDY\nzqIroTA5WaVJxWPwt3yJv/JsRaZ/N3jzbPLLxA72dP9G0pe3ZWUySa84lhbvce/n\n123m0/Qt7WMhJG86xy3ULwADRK/Uc7tqH1SI2j9o5qM4W4ehxyJPTWvc5QkzuB0f\n27IMb0Wws2G3LM7NKCvHdvDto/jFJ3CErrztndd1/ktSr6aP1w0/LwJ3Lv0WMTed\nQl1u4vPUy8vXYC4BnGEMfHIWhmnElUt/aKfSeZCVICkTf4kBjYVzHr4ejwa6itHB\nRz151wvBgHJRjDVnseSF6IJdXReAoRQMNLkocpLN1WkFQaWclA/VvmWp90UbDB8o\nPkOSLljBAgMBAAECggEAD7onUUuumUgCFjOss8/CUAFVZUJiJOfK5UpCYbGYX/U7\nJxGQoNiO2z1ynOneYJ0oHVn9PBZQQ6b7jhmSOsIVzjM7qDT7yHfwZVd9/5c3z5eo\niGvs9z1RWG/nqBlroSwEUBwVWg/ODzoFq0A0d3jq0BjaI0i+ChxWP2Fdot9WqALS\n7WajDpChxvSrUY15uiwup8uZCkmwWfRshPSYUuYPzYJ3IfPa0yy8xfha6mBOpFic\nWMuxFoVBGEkMpiBfQlzJ5yIqiYck2vROhPD6dB9m9HMJuC8+1gcgoFfWXMzp6NTu\nqYz8BqLa+ffT4kaYuR3D9Gf6g0UMisq1GrqYal17OwKBgQDy9LQRjAMhpP/boOah\n1kNA3i29V/a9QXnY5ekd0945W4f3qaQAhClKy42YlsxVrxdDWL3pY5cduyq2B5me\nSDg7cEC+0IH9XU8MwAawMUU4FdD6zvjTKCb36VS4p4GrRHIPKH39mdYMdFfYzwdA\nR1GgLDCXoGmXViMyoiXJjFBJJwKBgQDZCx6wVWJhuqo6i0wah2SryDnWL4c+NBdq\nyazmKxWkz93OlKvTfykGnQhrNcVmrwepWKy2A836egIFcUw1zrUXVPfZruoiRBRk\n51s80/bs0uIqxBse7HfMnt9z5e8icK7GwW/liW7mkzFbv+t4zB+m2PUyRi8vmQkG\nKsn66E9v1wKBgAg7zMJveUexnM4npMlFRqAzJ1+sVHtTdbqpB/5vyK8u7+uvvxQZ\nZoDXZyQNsD0TIvmwTzdSnbNvPWJP3Z/kmKtDAZ612EHq4JvxAgkEknD1JFDrpLkb\nOj7alHlxi85vEmJ7H9HxXbSsWLHLSlaeVCDWfosU758mVykHH7q1256vAoGATpQP\nDUosbzN0k/OAnw4rrWG1Rs04SpXzcG8JkN9CW8QCCtuahdzAzqsltCLoj++Id5Aq\nH0+rUCbB+pR1QfnPaF4TyZThIXzCYXG9f31CJaWHynHrW7vC96sBPWWeuTpQnJbF\n2zVxmDrsIMqQBtDoGLDtvMDaLoJaWQg+zf8zW0sCgYEA5d+NDf5r77Y4UQxQgJRm\nnQjWNl3ydqUX4BUQA/JCoZE9fzfF34M7w9OTdvqxaBdjBkf1ygWhwHT4AFGTo1Pe\n/QLhNVHg5f3/DWph1lmPJAMPOZ+QyDgPX9t2LVRsj8/koHpBnXXqRXrUdHjuAP4g\nOyFRbL5HMYSs0eEto8q/lek=\n-----END PRIVATE KEY-----\n",
    "client_email": "kodland@access-to-tables-317614.iam.gserviceaccount.com",
    "client_id": "115531972219613712399",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/kodland%40access-to-tables-317614.iam.gserviceaccount.com"
}


def connect_to_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    # add credentials to the account
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json_service_account,
        scope)
    # authorize the clientsheet
    client = gspread.authorize(creds)
    return client

def _upload_to_google_sheets_rus(
        client,
        data: pd.DataFrame,
        table_name: str = 'Omnidesk. Case Analytics',
        sheet_name: str = 'data',
):
    sheet = client.open(table_name).worksheet(sheet_name)

    old_data = sheet.get_all_values()
    old_dataframe = pd.DataFrame()

    updated = old_dataframe.append(data)
    updated.reset_index(inplace=True)
    updated.drop('index', axis=1, inplace=True)

    updated.drop(
        updated[updated.duplicated(updated.columns, keep='first')].index,
        axis=0,
        inplace=True,
    )

    sheet.clear()
    gd.set_with_dataframe(sheet, updated)

    return updated

def compute_time(total: float) -> str:
    """
    Приводит время в наглядный формат

    :param total: количество секунд
    :return: строка "Nh Mmin Ls"
    """
    if total // 3600 == 0:
        if total // 60 == 0:
            return f'{np.round(total, 1)}s'
        else:
            return f'{int(total // 60)}min {int(np.round(total % 60, 0))}s'
    else:
        hours = int(total // 3600)
        minutes = int((total - hours * 3600) // 60)
        seconds = int(np.round((total - 3600 * hours) % 60, 0))
        return f'{hours}h {minutes}min {seconds}s'


start_time = timer()

conn = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}')

client = connect_to_google_sheets()
tc_statistics_doc = client.open('Omnidesk. Case Analytics')

def query(
):
    temp = pd.read_sql('''with client_messages as (
    select 
        message_id ,
        created_at + interval '3 hours' as created_at,
        case_id
    from omnidesk.messages m
    where 
        m.user_id is not null 
        and message_type = 'reply_user'
        and m.created_at >= '2022-06-01'
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
        and m.created_at >= '2022-06-01'
),
first_client_message as (
    select
        cm.case_id, 
        min(cm.created_at) as created_at
    from client_messages cm
    where cm.created_at >= '2022-06-01' 
    group by 1
),
first_staff_message as (
    select 
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
    la."change",
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
    and m.created_at >= '2022-06-01'
group by 1,2,3,4
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
    case when sla_by_message <= sla_by_last_online or ms.staff_id = 0 then sla_by_message else sla_by_last_online end as pre_min_sla,
    case when ms.staff_id = lasr.staff_id and pre_min_sla > sla_by_response then sla_by_response else pre_min_sla end as min_sla
from distinct_messages ms
left join lenta_active_staff las
    on las.staff_id = ms.staff_id and las.created_at = ms.created_at
left join lenta_active_staff_response lasr
    on lasr.created_at = ms.created_at and lasr.case_id = ms.case_id 
    ),
  /* select 
		ms.case_id,
		ms.staff_id,
		ms.message_type,
		ms.created_at,
		case 
			when ms.message_type = 'reply_user' and lead(ms.message_type) over (partition by ms.case_id order by ms.created_at) = 'reply_user'
			then 0
			else 1
		end as is_last_reply
	from distinct_messages ms
	order by ms.case_id, ms.created_at*/
waiting_periods as (
	select 
		ms.case_id,
		ms.staff_id,
		ms.message_type,
		case when ms.message_type = 'reply_user' and lag(ms.message_type) over (partition by ms.case_id order by ms.created_at) <> 'reply_user'
	        then 
				lag(ms.created_at) over (partition by ms.case_id order by ms.created_at) || '-' || ms.created_at
			else 
				''
		end as waiting_period,
		rank() over (partition by case_id order by ms.created_at) as rank_asc
	from distinct_messages ms
	order by ms.case_id, ms.created_at
),
json_waiting_periods as (
select 
	distinct
	case_id,
    listagg(distinct waiting_period, '; ') within group (order by rank_asc) as waiting_periods
from waiting_periods wp
where message_type = 'reply_user'
group by 1
),
min_frt as (
	select
		distinct 
		fsm.staff_id as frt_staff_id,
		scs.case_id ,
		scs.created_at,
		min_sla as frt
	from staff_case_sla scs
	join first_staff_message fsm 
    	on scs.case_id = fsm.case_id and fsm.created_at = scs.created_at
	where scs.message_type = 'reply_staff'
)
select
	distinct
	c.created_at,
	date_part("month", c.created_at) as month, 
	date_part("week", c.created_at) as week,
	case when c.channel = 'cch17' then 'whatsapp' else c.channel end as channel,
	c.case_id,
	g.group_title as ticket_group,
	'https://support.kodland.org/staff/cases/chat/' || c.case_number as omni_link,
	pd.department,
	--coalesce(u.user_phone, u.user_email) as user_data,
	pd.full_name as last_responsible,
	pd.group as cs_group,
	json_extract_path_text(cf.field_data, json_extract_path_text(replace(c.custom_fields,'\''','"'), 'cf_' || '4756')) as case_result,
	pd_frt.full_name as frt_responsible,
	pd_frt.group as frt_cs_group,
	round(mf.frt::float/60,2) as frt_minutes,
	round(sum(min_sla)::float/60,2) as sla_minutes,
	--round(sum(min_waiting)::float/60,2) as waiting_minutes,
	coalesce(jwp.waiting_periods, '') as waiting_periods,
	ms.replies_staff,
	ms.replies_user,
    datediff(minutes, c.created_at, c.closed_at) as full_closed_time,
	listagg(distinct coalesce(l.label_title,''), ', ') as labels
from omnidesk.cases c
join staff_case_sla scs
	on c.case_id = scs.case_id
join (
		select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
		from forms.personal_data_active_cs pdac
			union
		select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
		from forms.personal_data_dismissed_cs pddc
	) pd
	on pd.corporate_email like '%%' || c.staff_id || '%%'
left join min_frt mf
 	on mf.case_id = c.case_id
join (
		select pdac.full_name, pdac."group", pdac.corporate_email, pdac.department, pdac.first_date
		from forms.personal_data_active_cs pdac
			union
		select pddc.full_name, pddc."group", pddc.corporate_email, pddc.department, pddc.first_date
		from forms.personal_data_dismissed_cs pddc
	) pd_frt
	on pd_frt.corporate_email like '%%' || mf.frt_staff_id || '%%'
 left join json_waiting_periods jwp
 	on jwp.case_id = c.case_id
 join
	(
		select
			ms.case_id,
			count(case when ms.message_type = 'reply_staff' then ms.message_id end) as replies_staff,
			count(case when ms.message_type = 'reply_user' then ms.message_id end) as replies_user
		from distinct_messages ms
		group by 1
	) as ms
 	on ms.case_id = c.case_id
 left join omnidesk.labels l
 	on c.labels like '%%' || l.label_id || '%%'
 left join omnidesk.users u
 	on u.user_id = c.user_id
 left join omnidesk."groups" g
 	on g.group_id = c.group_id
 left join omnidesk.custom_fields cf
 	on cf.field_id = 4756
where c.staff_id > 0 and c.status = 'closed' and c.deleted = false and c.spam = false
and c.created_at >= '2022-05-01' and g.group_title not similar to '%%M1%%|%%М1%%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19
order by 1
    ''', con=conn)

    temp_list = tc_statistics_doc.worksheet('data')
    #gd.set_with_dataframe(temp_list, temp)

    _upload_to_google_sheets_rus(
            client=client,
            data=temp,
    )

    return temp

if __name__ == '__main__':
    query()
