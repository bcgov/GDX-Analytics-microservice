BEGIN;
UPDATE test.gdxdsd4256
  SET date_removed = CURRENT_DATE 
  WHERE
    date_removed IS NULL 
    AND Site NOT IN (
        SELECT Site FROM test.gdxdsd4256 WHERE date_added IS NULL
    );


WITH copy AS (select * from test.gdxdsd4256 where data_status = 'new')
UPDATE test.gdxdsd4256 SET 
    asset_tag = copy.asset_tag,
    printer_queue = copy.printer_queue,
    idid_id = copy.idid_id,
    item_type = copy.item_type,
    path = copy.path,
    date_removed = NULL
FROM copy
WHERE gdxdsd4256.site = copy.site;


UPDATE test.gdxdsd4256 SET 
    date_added = CURRENT_DATE,
    data_status = 'old'
WHERE
  Site NOT IN (
  SELECT Site FROM test.gdxdsd4256 WHERE data_status = 'old'
  );


DELETE FROM test.gdxdsd4256 
  WHERE
    data_status = 'new';


COMMIT;