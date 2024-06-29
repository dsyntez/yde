/* создание таблицы tmp_sources с данными из всех источников */

DROP TABLE IF EXISTS tmp_sources;
CREATE TEMP TABLE tmp_sources AS 
SELECT  order_id,
		order_created_date,
		order_completion_date,
		order_status,
		craftsman_id,
		craftsman_name,
		craftsman_address,
		craftsman_birthday,
		craftsman_email,
		product_id,
		product_name,
		product_description,
		product_type,
		product_price,
		customer_id,
		customer_name,
		customer_address,
		customer_birthday,
		customer_email,
		'1' as source_id
FROM source1.craft_market_wide
UNION
SELECT  t2.order_id,
		t2.order_created_date,
		t2.order_completion_date,
		t2.order_status,
		t1.craftsman_id,
		t1.craftsman_name,
		t1.craftsman_address,
		t1.craftsman_birthday,
		t1.craftsman_email,
		t1.product_id,
		t1.product_name,
		t1.product_description,
		t1.product_type,
		t1.product_price,
		t2.customer_id,
		t2.customer_name,
		t2.customer_address,
		t2.customer_birthday,
		t2.customer_email,
		'2' as source_id
  FROM source2.craft_market_orders_customers t2
    JOIN source2.craft_market_masters_products t1 ON t2.product_id = t1.product_id and t2.craftsman_id = t1.craftsman_id 
UNION
SELECT  t1.order_id,
		t1.order_created_date,
		t1.order_completion_date,
		t1.order_status,
		t2.craftsman_id,
		t2.craftsman_name,
		t2.craftsman_address,
		t2.craftsman_birthday,
		t2.craftsman_email,
		t1.product_id,
		t1.product_name,
		t1.product_description,
		t1.product_type,
		t1.product_price,
		t3.customer_id,
		t3.customer_name,
		t3.customer_address,
		t3.customer_birthday,
		t3.customer_email,
		'3' as source_id
  FROM source3.craft_market_orders t1
    JOIN source3.craft_market_craftsmans t2 ON t1.craftsman_id = t2.craftsman_id 
    JOIN source3.craft_market_customers t3 ON t1.customer_id = t3.customer_id
UNION
SELECT  t1.order_id,
		t1.order_created_date,
		t1.order_completion_date,
		t1.order_status,
		t1.craftsman_id,
		t1.craftsman_name,
		t1.craftsman_address,
		t1.craftsman_birthday,
		t1.craftsman_email,
		t1.product_id,
		t1.product_name,
		t1.product_description,
		t1.product_type,
		t1.product_price,
		t2.customer_id,
		t2.customer_name,
		t2.customer_address,
		t2.customer_birthday,
		t2.customer_email,
		'4' as source_id
  FROM external_source.craft_products_orders t1
    JOIN external_source.customers t2 on t1.customer_id = t2.customer_id;
 
/* создание таблицы tmp_sources_fact */

DROP TABLE IF EXISTS tmp_sources_fact;
CREATE TEMP TABLE tmp_sources_fact AS 
SELECT  
		order_id,
		order_created_date,
		order_completion_date,
		order_status,
		product_id,
		craftsman_id,
		customer_id,
		current_timestamp,
		source_id
FROM tmp_sources src;
 
/* обновление существующих записей и добавление новых в dwh.f_order */

MERGE INTO dwh_v2.f_order f
USING tmp_sources_fact t
ON f.product_id = t.product_id AND f.craftsman_id = t.craftsman_id AND f.customer_id = t.customer_id AND f.order_created_date = t.order_created_date
WHEN MATCHED THEN
  UPDATE SET order_completion_date = t.order_completion_date, order_status = t.order_status, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (order_id, order_created_date, order_completion_date, order_status, product_id, craftsman_id, customer_id, load_dttm, source_id)
  VALUES (t.order_id, t.order_created_date, t.order_completion_date, t.order_status, t.product_id, t.craftsman_id, t.customer_id, current_timestamp, t.source_id);
