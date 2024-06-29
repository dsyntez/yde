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

/* обновление существующих записей и добавление новых в dwh_v2.d_craftsmans */

MERGE INTO dwh_v2.d_craftsman d
USING (select distinct craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, source_id from tmp_sources) t
on d.craftsman_ext_id = t.craftsman_id and d.source_id = t.source_id
WHEN MATCHED THEN
  UPDATE SET 
  	craftsman_name = t.craftsman_name,
  	craftsman_address = t.craftsman_address,
	craftsman_birthday = t.craftsman_birthday,
	craftsman_email = t.craftsman_email,
	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (craftsman_ext_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm, source_id)
  VALUES (t.craftsman_id, t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp, t.source_id);
 
/* обновление существующих записей и добавление новых в dwh_v2.d_products*/

MERGE INTO dwh_v2.d_product d
USING (select distinct product_id, product_name, product_description, product_type, product_price, source_id from tmp_sources) t
on d.product_name = t.product_name and
	d.product_description = t.product_description and
	d.product_price = t.product_price
WHEN MATCHED THEN
  UPDATE set
	  product_name = t.product_name,
	  product_description = t.product_description,
	  product_type = t.product_type,
	  product_price = t.product_price
WHEN NOT MATCHED THEN
  INSERT (product_ext_id, product_name, product_description, product_type, product_price, load_dttm, source_id)
  VALUES (t.product_id, t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp, t.source_id);
 
/* обновление существующих записей и добавление новых в dwh_v2.d_customer*/

MERGE INTO dwh_v2.d_customer d
USING (select distinct customer_id, customer_name, customer_address, customer_birthday, customer_email, source_id from tmp_sources) t
on d.customer_ext_id = t.customer_id and d.source_id = t.source_id
WHEN MATCHED THEN
  UPDATE set
  	customer_name = t.customer_name,
  	customer_address = t.customer_address,
  	customer_birthday = t.customer_birthday,
  	customer_email = t.customer_email
WHEN NOT MATCHED THEN
  INSERT (customer_ext_id, customer_name, customer_address, customer_birthday, customer_email, load_dttm, source_id)
  VALUES (t.customer_id, t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp, t.source_id);
