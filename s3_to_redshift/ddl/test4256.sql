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
    site = copy.site,
    asset_tag = copy.asset_tag,
    printer_queue = copy.printer_queue,
    idir_id = copy.idir_id,
    item_type = copy.item_type,
    path = copy.path,
    notes = copy.notes,
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