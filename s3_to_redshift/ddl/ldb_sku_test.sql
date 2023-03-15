BEGIN;
UPDATE test.ldb_sku
  SET date_removed = CURRENT_DATE 
  WHERE
    date_removed IS NULL 
    AND Site NOT IN (
        SELECT Site FROM test.ldb_sku WHERE date_added IS NULL
    );


WITH copy AS (select * from test.ldb_sku where data_status = 'new')
UPDATE test.ldb_sku SET 
    product_name = copy.product_name,
    image = copy.image,
    body = copy.body,
    volume = copy.volume,
    bottles_per_pack = copy.bottles_per_pack,
    regular_price = copy.regular_price,
    lto_price = copy.lto_price,
    lto_start = copy.lto_start,
    lto_end = copy.lto_end,
    price_override = copy.price_override,
    store_count = copy.store_count,
    inventory = copy.inventory,
    availability_override = copy.availability_override,
    whitelist = copy.whitelist,
    blacklist = copy.blacklist,
    upc = copy.upc,
    all_upcs = copy.all_upcs,
    alcohol = copy.alcohol,
    kosher = copy.kosher,
    organic = copy.organic,
    sweetness = copy.sweetness,
    vqa = copy.vqa,
    craft_beer = copy.craft_beer,
    bcl_select = copy.bcl_select,
    new_flag = copy.new_flag,
    rating = copy.rating,
    votes = copy.votes,
    product_type = copy.product_type,
    category = copy.category,
    sub_category = copy.sub_category,
    country = copy.country,
    region = copy.region,
    sub_region = copy.sub_region,
    grape_variety = copy.grape_variety,
    restriction_code = copy.restriction_code,
    status_code = copy.status_code,
    inventory_code = copy.inventory_code,
    date_removed = NULL
FROM copy
WHERE ldb_sku.sku = copy.sku;


UPDATE test.ldb_sku SET 
    date_added = CURRENT_DATE,
    data_status = 'old'
WHERE
  Site NOT IN (
  SELECT Site FROM test.ldb_sku WHERE data_status = 'old'
  );


DELETE FROM test.ldb_sku 
  WHERE
    data_status = 'new';


COMMIT;