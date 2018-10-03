----Duplicate datasets by Identifier field by Organization:
----Listed all the records with Identifier fields populated. We excluded datasets which don’t have identifiers 
select *, regexp_split_to_table(value,',')as str_identifier, substring (regexp_split_to_table(value,','), '"identifier"' ) into temp_dsident_Fstr  from package_extra where value like '%"identifier"%' 
select * , trim(replace(replace(str_identifier,'"identifier":',''),'"' ,'') ) as identifier into temp_dsident_F from temp_dsident_Fstr where substring='"identifier"'  
Drop table temp_dsident_Fstr

---Filtered the datasets and listed records with more than one duplicate Identifier 
Select p.id,p.name,p.title,p.url,p.notes, p.revision_id,p.maintainer, p.state,p.type,p.owner_org,p.metadata_modified,d.key,d.value,d.str_identifier,d.identifier into temp_dsdupIdent  from package p Join temp_dsident_F d on p.id=d.package_id and d.identifier in (select identifier from  temp_dsident_F   group by identifier HAVING count(identifier) >1 )   
---Analyzed the list and found same identifier in more than one organizations (image 3 attached)
---Sorted identifiers and arranged by organizations (grouped by organizations)
select p.id,p.name,p.title,p.url,p.notes, p.revision_id,p.maintainer, p.state,p.type,p.owner_org,p.metadata_modified,p.key,p.value,p.str_identifier, p.identifier  into temp_dsdupIdent_final from temp_dsdupIdent p  join (select  identifier, owner_org, count(*) from temp_dsdupIdent  group by owner_org,identifier HAVING count(identifier) >1) d on p.identifier=d.identifier and p.owner_org=d.owner_org  order by p.identifier 
---Listed each identifiers with duplicate datasets  (image 5 attached)
Select * from temp_dsdupIdent_final
---That gives us a list of duplicate datasets for each unique identifier by organizations that can now be deleted.
