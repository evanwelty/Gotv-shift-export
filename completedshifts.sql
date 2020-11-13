-- Enter your SQL here.
Drop Table if exists ewelty.notcompleted_NV ;

Create Table ewelty.notcompleted_NV AS (

  Select *
From (
Select row_number() over (partition by eventname, vanid order by datemodified desc) as row, *
From
(Select evt.eventname ||' ' ||date(evt.dateoffsetbegin),
   T.turf,
   SNP.vanid,
    ct.firstname,
    ct.lastname,
    EVT.eventid,
    EVT.eventname,
    date(evt.dateoffsetbegin),
 	  er.eventrolename,
    ss.eventstatusname,
    ss.datemodified,
 		ss.eventstatusid
 

 From van.tsm_nextgen_events as evt
 

LEFT JOIN van.tsm_nextgen_eventsignups as snp
ON evt.eventid = snp.eventid
 
LEFT JOIN van.tsm_nextgen_eventsignupsstatuses as ss
ON snp.eventsignupid = ss.eventsignupid
 
LEFT JOIN van.tsm_nextgen_contactssurveyresponses_mym as csr
ON snp.vanid = csr.vanid
 
LEFT JOIN van.tsm_nextgen_contacts_mym as ct
ON SNP.vanid = ct.vanid

 LEFT JOIN rising.turf as t

ON SNP.vanid = t.vanid
 
LEFT JOIN van.tsm_nextgen_eventseventroles_mym as evr 
ON snp.eventroleid = evr.eventroleid
 
LEFT JOIN van.tsm_nextgen_eventroles as er
ON evr.eventroleid = er.eventroleid
 
-- NEVADA is 56351 --
WHERE evt.createdbycommitteeid = 56351

AND ss.eventstatusid = 2

AND date(evt.dateoffsetbegin) >= date('2020-01-01') 
 	
 )
 )
 WHERE row = 1 
 )
