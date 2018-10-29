----Duplicate datasets by Identifier field by Organization:
SELECT *, regexp_split_to_table(value,',')as str_identifier, substring (regexp_split_to_table(value,','), '"identifier"' )
INTO temp_dsident_Fstr  FROM package_extra WHERE value like '%"identifier"%';

SELECT * , trim(replace(replace(str_identifier,'"identifier":',''),'"' ,'') ) as identifier
INTO temp_dsident_F FROM temp_dsident_Fstr WHERE substring='"identifier"' ;

SELECT p.id,p.name,p.title,p.url,p.notes, p.revision_id,p.maintainer, p.state,p.type,p.owner_org,p.metadata_modified,d.key,d.value,d.str_identifier,d.identifier
INTO temp_dsdupIdent_sh_final FROM package p Join temp_dsident_F d on p.id=d.package_id WHERE p.owner_org='d8a6202b-c0c3-463d-88a0-3bc021a178ed'
and d.identifier in (select identifier from  temp_dsident_F   GROUP by identifier HAVING count(identifier) >1 ) 

SELECT * INTO tempDS FROM temp_dsIdent_sh_nasa_final ORDER by identifier, metadata_created  ASC

ALTER TABLE tempDS ADD COLUMN  SNo SERIAL;

SELECT *  INTO  tempDupDS FROM tempDS td LEFT JOIN ( SELECT MIN(SNo) AS SlNo FROM tempDS GROUP BY identifier ) t ON td.SNo=t.SlNo WHERE t.slno IS null ORDER BY td.SNo;

DELETE FROM tempDS WHERE id IN(SELECT td.id FROM tempDupDS td LEFT JOIN ( SELECT MIN(SNo) AS SlNo FROM tempDS GROUP BY title ) t ON td.SNo=t.SlNo WHERE t.SLNo IS null);

ALTER TABLE tempDS DROP COLUMN SNO;

ALTER TABLE tempDupDS  DROP COLUMN SNO;

--Below Query is to convert Duplicates state to 'to_delete'


--UPDATE package SET state='to_dduplicate-removedelete' WHERE id in (SELECT p.id FROM  Package AS p JOIN tempdupds AS T  ON p.id=T.id);
