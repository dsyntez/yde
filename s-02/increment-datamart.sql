WITH
	dwh_delta AS (
SELECT
	dcs.customer_ext_id AS customer_id,
	dcs.customer_name as customer_name,
	dcs.customer_address as customer_address,
	dcs.customer_birthday as customer_birthday,
	dcs.customer_email as customer_email,
	dc.craftsman_ext_id as craftsman_id,
	fo.order_id as order_id,
	dp.product_ext_id as product_id,
	dp.product_price as product_price,
	dp.product_type as product_type,
	date_part('year', age(dcs.customer_birthday)) as customer_age,
	(fo.order_completion_date - fo.order_created_date) AS diff_order_date,
	fo.order_status as order_status,
	to_char(fo.order_created_date, 'yyyy-mm') as report_period,
	crd.customer_id as exist_customer_id,
	dc.load_dttm as craftsman_load_dttm,
	dcs.load_dttm as customers_load_dttm,
	dp.load_dttm AS products_load_dttm,
	fo.source_id
FROM dwh_v2.f_order fo
INNER JOIN dwh_v2.d_customer dcs ON fo.customer_id = dcs.customer_ext_id and fo.source_id = dcs.source_id
INNER JOIN dwh_v2.d_craftsman dc ON fo.craftsman_id = dc.craftsman_ext_id and fo.source_id = dc.source_id
INNER JOIN dwh_v2.d_product dp ON fo.product_id = dp.product_ext_id and fo.source_id = dp.source_id
LEFT JOIN dwh_v2.customer_report_datamart crd on crd.customer_id = fo.customer_id and crd.source_id = fo.source_id
WHERE
	dc.load_dttm > (select COALESCE(max(load_dttm), '1900-01-01') from dwh_v2.load_dates_craftsman_report_datamart ldcrd)
	or dcs.load_dttm > (select COALESCE(max(load_dttm), '1900-01-01') from dwh_v2.load_dates_craftsman_report_datamart)
	or dp.load_dttm > (select COALESCE(max(load_dttm), '1900-01-01') from dwh_v2.load_dates_craftsman_report_datamart)
	or fo.load_dttm > (select COALESCE(max(load_dttm), '1900-01-01') from dwh_v2.load_dates_craftsman_report_datamart)
),
dwh_update_delta AS (
select
	customer_id AS customer_id,
	customer_name as customer_name,
	customer_address as customer_address,
	customer_birthday as customer_birthday,
	customer_email as customer_email
FROM
	dwh_delta dd
WHERE
	dd.exist_customer_id is not null
),
dwh_delta_insert_result AS (
select
	T4.customer_id AS customer_id,
	T4.customer_name as customer_name,
	T4.customer_address as customer_address,
	T4.customer_birthday as customer_birthday,
	T4.customer_email as customer_email,
	T4.customer_money AS customer_money,
	T4.platform_money AS platform_money,
	T4.count_order AS count_order,
	T4.avg_price_order AS avg_price_order,
	T4.avg_age_customer AS avg_age_customer,
	T4.top_product_category AS top_product_category,
	T4.top_craftsman as top_craftsman,
	T4.median_time_order_completed AS median_time_order_completed,
	T4.count_order_created AS count_order_created,
	T4.count_order_in_progress AS count_order_in_progress,
	T4.count_order_delivery AS count_order_delivery,
	T4.count_order_done AS count_order_done,
	T4.count_order_not_done AS count_order_not_done,
	T4.report_period AS report_period,
	T4.source_id as source_id
from (
	SELECT
		T2.customer_id AS customer_id,
		T2.customer_name as customer_name,
		T2.customer_address as customer_address,
		T2.customer_birthday as customer_birthday,
		T2.customer_email as customer_email,
		T2.customer_money AS customer_money,
		T2.platform_money AS platform_money,
		T2.count_order AS count_order,
		T2.avg_price_order AS avg_price_order,
		T2.avg_age_customer AS avg_age_customer,
		T3.product_type AS top_product_category,
		T5.craftsman_id_for_top_craftsman as top_craftsman,
		T2.median_time_order_completed AS median_time_order_completed,
		T2.count_order_created AS count_order_created,
		T2.count_order_in_progress AS count_order_in_progress,
		T2.count_order_delivery AS count_order_delivery,
		T2.count_order_done AS count_order_done,
		T2.count_order_not_done AS count_order_not_done,
		T2.report_period AS report_period,
		T2.source_id as source_id,
		RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product,
		RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_craftsman
	from (
		SELECT
			customer_id,
			customer_name,
			customer_address,
			customer_birthday,
			customer_email,
			craftsman_id,
			sum(product_price) as customer_money,
			0.1*sum(product_price) as platform_money,
			COUNT(order_id) as count_order,
			avg(product_price) as avg_price_order,
			avg(customer_age) as avg_age_customer,
			percentile_cont(0.5) WITHIN GROUP(ORDER BY diff_order_date) as median_time_order_completed,
			sum(CASE WHEN order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
			sum(CASE WHEN order_status = 'in-progress' or order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
			sum(CASE WHEN order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
			sum(CASE WHEN order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
			sum(CASE WHEN order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
			report_period,
			source_id
		FROM dwh_delta
		WHERE exist_customer_id IS NULL
		GROUP BY customer_id, craftsman_id, customer_name, customer_address, customer_birthday, customer_email, report_period, source_id
	) as T2
	INNER JOIN (
		SELECT
			craftsman_id as craftsman_id_for_product_type,
			product_type,
			count(product_id) as count_product
		FROM dwh_delta AS dd
		GROUP BY dd.craftsman_id, dd.product_type
		ORDER BY count_product desc
	) AS T3 
	on T2.craftsman_id = T3.craftsman_id_for_product_type
	inner join (
		select 
			craftsman_id as craftsman_id_for_top_craftsman,
			count(craftsman_id) as count_craftsman
		FROM dwh_delta AS dd
		GROUP BY dd.craftsman_id
		ORDER BY count_craftsman desc
	) as T5
	on T2.craftsman_id = T5.craftsman_id_for_top_craftsman
) as T4
WHERE T4.rank_count_product = 1 and T4.rank_craftsman = 1 ORDER BY report_period
),
dwh_delta_update_result AS (
SELECT
	T4.customer_id AS customer_id,
	T4.customer_name as customer_name,
	T4.customer_address as customer_address,
	T4.customer_birthday as customer_birthday,
	T4.customer_email as customer_email,
	T4.customer_money AS customer_money,
	T4.platform_money AS platform_money,
	T4.count_order AS count_order,
	T4.avg_price_order AS avg_price_order,
	T4.avg_age_customer AS avg_age_customer,
	T4.top_product_category AS top_product_category,
	T4.top_craftsman as top_craftsman,
	T4.median_time_order_completed AS median_time_order_completed,
	T4.count_order_created AS count_order_created,
	T4.count_order_in_progress AS count_order_in_progress,
	T4.count_order_delivery AS count_order_delivery,
	T4.count_order_done AS count_order_done,
	T4.count_order_not_done AS count_order_not_done,
	T4.report_period AS report_period,
	T4.source_id as source_id
FROM (
	SELECT
		T2.customer_id AS customer_id,
		T2.customer_name as customer_name,
		T2.customer_address as customer_address,
		T2.customer_birthday as customer_birthday,
		T2.customer_email as customer_email,
		T2.customer_money AS customer_money,
		T2.platform_money AS platform_money,
		T2.count_order AS count_order,
		T2.avg_price_order AS avg_price_order,
		T2.avg_age_customer AS avg_age_customer,
		T3.product_type AS top_product_category,
		T5.craftsman_id_for_top_craftsman as top_craftsman,
		T2.median_time_order_completed AS median_time_order_completed,
		T2.count_order_created AS count_order_created,
		T2.count_order_in_progress AS count_order_in_progress,
		T2.count_order_delivery AS count_order_delivery,
		T2.count_order_done AS count_order_done,
		T2.count_order_not_done AS count_order_not_done,
		T2.report_period AS report_period,
		T2.source_id as source_id,
		RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product,
		RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_craftsman
	FROM (
		SELECT
			customer_id,
			customer_name,
			customer_address,
			customer_birthday,
			customer_email,
			craftsman_id,
			sum(product_price) as customer_money,
			0.1*sum(product_price) as platform_money,
			COUNT(order_id) as count_order,
			avg(product_price) as avg_price_order,
			avg(customer_age) as avg_age_customer,
			percentile_cont(0.5) WITHIN GROUP(ORDER BY diff_order_date) as median_time_order_completed,
			sum(CASE WHEN order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
			sum(CASE WHEN order_status = 'in-progress' or order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
			sum(CASE WHEN order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
			sum(CASE WHEN order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
			sum(CASE WHEN order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
			report_period,
			source_id
		FROM dwh_delta
		GROUP BY customer_id, craftsman_id, customer_name, customer_address, customer_birthday, customer_email, report_period, source_id
		) as T2
	INNER JOIN dwh_update_delta ud ON T2.customer_id = ud.customer_id
	INNER JOIN (
		SELECT
			craftsman_id as craftsman_id_for_product_type,
			product_type,
			count(product_id) as count_product
		FROM dwh_delta AS dd
		GROUP BY dd.craftsman_id, dd.product_type
		ORDER BY count_product desc
	) AS T3 
	on T2.craftsman_id = T3.craftsman_id_for_product_type
	inner join (
		select 
			craftsman_id as craftsman_id_for_top_craftsman,
			count(craftsman_id) as count_craftsman
		FROM dwh_delta AS dd
		GROUP BY dd.craftsman_id
		ORDER BY count_craftsman desc
	) as T5
	on T2.craftsman_id = T5.craftsman_id_for_top_craftsman
) AS T4
WHERE T4.rank_count_product = 1 ORDER BY report_period
),
insert_delta AS (
	insert into dwh_v2.customer_report_datamart as t (
		customer_id,
		customer_name,
		customer_address,
		customer_birthday,
		customer_email,
		customer_money,
		platform_money,
		count_order,
		avg_price_order,
		top_product_category,
		top_craftsman,
		median_time_order_completed,
		count_order_created,
		count_order_in_progress,
		count_order_delivery,
		count_order_done,
		count_order_not_done,
		report_period,
		source_id
	)
	select
		customer_id,
		customer_name,
		customer_address,
		customer_birthday,
		customer_email,
		customer_money,
		platform_money,
		count_order,
		avg_price_order,
		top_product_category,
		top_craftsman,
		median_time_order_completed,
		count_order_created,
		count_order_in_progress,
		count_order_delivery,
		count_order_done,
		count_order_not_done,
		report_period,
		source_id
	from dwh_delta_insert_result
	returning
		t.customer_id,
		t.customer_name,
		t.customer_address,
		t.customer_birthday,
		t.customer_email,
		t.customer_money,
		t.platform_money,
		t.count_order,
		t.avg_price_order,
		t.top_product_category,
		t.top_craftsman,
		t.median_time_order_completed,
		t.count_order_created,
		t.count_order_in_progress,
		t.count_order_delivery,
		t.count_order_done,
		t.count_order_not_done,
		t.report_period,
		t.source_id
),
update_delta AS (
UPDATE dwh_v2.customer_report_datamart as tableA set
	customer_id = tableB_updates.customer_id,
	customer_name = tableB_updates.customer_name,
	customer_address = tableB_updates.customer_address,
	customer_birthday = tableB_updates.customer_birthday,
	customer_email = tableB_updates.customer_email,
	customer_money = tableB_updates.customer_money,
	platform_money = tableB_updates.platform_money,
	count_order = tableB_updates.count_order,
	avg_price_order = tableB_updates.avg_price_order,
	top_product_category = tableB_updates.top_product_category,
	top_craftsman = tableB_updates.top_craftsman,
	median_time_order_completed = tableB_updates.median_time_order_completed,
	count_order_created = tableB_updates.count_order_created,
	count_order_in_progress = tableB_updates.count_order_in_progress,
	count_order_delivery = tableB_updates.count_order_delivery,
	count_order_done = tableB_updates.count_order_done,
	count_order_not_done = tableB_updates.count_order_not_done,
	report_period = tableB_updates.report_period,
	source_id = tableB_updates.source_id
  FROM (
    SELECT 
      customer_id,
      customer_name,
      customer_address,
      customer_birthday,
      customer_email,
      customer_money,
      platform_money,
      count_order,
      avg_price_order,
      top_product_category,
      top_craftsman,
      median_time_order_completed,
      count_order_created,
      count_order_in_progress,
      count_order_delivery,
      count_order_done,
      count_order_not_done,
      report_period,
      source_id
    from dwh_delta_update_result
    ) AS tableB_updates
  WHERE tableA.customer_id = tableB_updates.customer_id
  returning
  	tableA.customer_id,
  	tableA.customer_name,
  	tableA.customer_address,
  	tableA.customer_birthday,
  	tableA.customer_email,
  	tableA.customer_money,
  	tableA.platform_money,
  	tableA.count_order,
  	tableA.avg_price_order,
  	tableA.top_product_category,
  	tableA.top_craftsman,
  	tableA.median_time_order_completed,
  	tableA.count_order_created,
  	tableA.count_order_in_progress,
  	tableA.count_order_delivery,
  	tableA.count_order_done,
  	tableA.count_order_not_done,
  	tableA.report_period,
  	tableA.source_id
),
insert_load_date AS (
	INSERT INTO dwh.load_dates_craftsman_report_datamart (
		load_dttm
	)
	SELECT COALESCE((SELECT MAX(GREATEST(craftsman_load_dttm, customers_load_dttm, products_load_dttm))), NOW())
	FROM dwh_delta
),
increment_datamart as (
	select * from insert_delta union all select * from update_delta
)
select * from increment_datamart
