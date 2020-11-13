with statuses as (
  select 
    c.vanid
  , ev.eventname
  , left(su.datetimeoffsetbegin,19) as shift_time
  , su.eventsignupid
  , st.eventstatusname
  , date(st.datemodified) as datemodified
  , row_number() over (partition by st.eventsignupid order by st.datecreated desc) as row
  from van.tsm_nextgen_contacts_mym c
  	inner join van.tsm_nextgen_eventsignups su using(vanid)
  	inner join van.tsm_nextgen_eventsignupsstatuses st using(eventsignupid)
  	inner join van.tsm_nextgen_events ev using(eventid)
  
  
-- Replace with your committeeid
  where ev.createdbycommitteeid = '56351'
-- Tailor to your event names or edit Custom VAN event names on back end of Mobilize
  and (ev.eventname ilike 'DryRun%'
  or ev.eventname ilike 'Final5%')
)  

, signups as (
  
  select 
    c.vanid
  , ev.eventname
  , su.eventsignupid
  , date(st.datecreated) as datecreated
  , row_number() over (partition by st.eventsignupid order by st.datecreated asc) as row
  from van.tsm_nextgen_contacts_mym c
  	inner join van.tsm_nextgen_eventsignups su using(vanid)
  	inner join van.tsm_nextgen_eventsignupsstatuses st using(eventsignupid)
  	inner join van.tsm_nextgen_events ev using(eventid)
  
  
-- Replace with your committeeid
  where ev.createdbycommitteeid = '56351'
-- Tailor to your event names or edit Custom VAN event names on back end of Mobilize
  and (ev.eventname ilike 'DryRun%'
  or ev.eventname ilike 'Final5%')
       
  )
  
  , shifts as (
    select 
    	  st.vanid
    	, st.eventname
    	, st.shift_time
    	, su.datecreated as signupdate
    	, st.eventstatusname as status
    	, st.datemodified as dateupdated
    from statuses st
    	left join signups su using(eventsignupid)
    where st.row = 1
    	and su.row = 1
    )


, groupedvols as (
select 
      right(sg.supportergroupname,2) as groupid
    , c.vanid
    , c.firstname
    , c.lastname
    , c.phone
    , row_number() over (partition by c.vanid order by csg.datemodified desc) as row
 from van.tsm_nextgen_contactssupportergroups_mym csg
		inner join van.tsm_nextgen_contacts_mym c using(vanid)
		inner join van.tsm_nextgen_supportergroups_mym sg using(supportergroupid)
  
  
-- Replace with your supportergroupid's or "where supportergroupname ilike '%ME%'"
 where sg.supportergroupid between 112 and 119
  and csg.datesuppressed is null
)




select 
	  case when gv.groupid is null then 'Unturfed' else gv.groupid end
    , c.firstname
    , c.lastname
    

-- Tailor to your event names or edit Custom VAN event names on back end of Mobilize
    , case 
   		 when sh.eventname ilike 'DryRun1%' then 'Dry Run 1' 
		 when sh.eventname ilike 'DryRun2%' then 'Dry Run 2' 
         when sh.eventname ilike 'Final5%' then 'Final Five' 
         else null end as gotv_event
    , case 
    	 when sh.eventname ilike '%Phone%' then 'Phone Bank' 
         when sh.eventname ilike '%Text%' then 'Text Bank'
         else null end as shift_type
         
    , sh.signupdate
    , sh.status
    , sh.dateupdated
		, sh.shift_time
from shifts sh
	left join van.tsm_nextgen_contacts_mym c using(vanid)
    left join groupedvols gv using(vanid)
order by gotv_event asc, shift_type asc, sh.shift_time asc, sh.status asc, gv.groupid asc, signupdate desc, dateupdated desc
