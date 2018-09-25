SELECT p.id,p.name AS datasetname,p.title, p.metadata_modified AS updated_date,p.state,g.name AS or_name, g.title AS or_title INTO tempDS FROM package p JOIN "group" g ON p.owner_org = g.id WHERE (p.owner_org IN ( SELECT "group".id FROM "group")) and p.state='active' order by datasetname;

ALTER TABLE tempDS ADD COLUMN  SNo SERIAL;

SELECT *  INTO  tempDupDS FROM tempDS td LEFT JOIN ( SELECT MIN(SNo) AS SlNo FROM tempDS GROUP BY title ) t ON td.SNo=t.SlNo WHERE t.slno IS null ORDER BY td.SNo;

DELETE FROM tempDS WHERE id IN(SELECT td.id FROM tempDupDS td LEFT JOIN ( SELECT MIN(SNo) AS SlNo FROM tempDS GROUP BY title ) t ON td.SNo=t.SlNo WHERE t.SLNo IS null);

ALTER TABLE tempDS DROP COLUMN SNO;



ALTER TABLE tempDupDS  DROP COLUMN SNO;

Below Query is to convert Duplicates state to 'to_delete'



--UPDATE package SET state='to_delete' WHERE id in (SELECT p.id FROM  Package AS p JOIN tempdupds AS T  ON p.id=T.id);