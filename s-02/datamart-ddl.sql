DROP TABLE IF EXISTS dwh_v2.customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh_v2.customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
    customer_id BIGINT NOT NULL, -- идентификатор клиента
    customer_name VARCHAR NOT NULL, -- Ф. И. О. клиента
    customer_address VARCHAR NOT NULL, -- адрес клиента
    customer_birthday DATE NOT NULL, -- дата рождения клиента
    customer_email VARCHAR NOT NULL, -- электронная почта
    customer_money NUMERIC(15,2) NOT NULL, -- сумма, которую потратил заказчик;
	platform_money BIGINT NOT NULL, -- сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик);
	count_order BIGINT NOT NULL, -- количество заказов у заказчика за месяц;
	avg_price_order NUMERIC(10,2) NOT NULL, -- средняя стоимость одного заказа у заказчика за месяц;
	median_time_order_completed NUMERIC(10,1), -- медианное время в днях от момента создания заказа до его завершения за месяц;
	top_product_category VARCHAR NOT NULL, -- самая популярная категория товаров у этого заказчика за месяц;
	top_craftsman BIGINT NOT NULL, -- идентификатор самого популярного мастера ручной работы у заказчика. Если заказчик сделал одинаковое количество заказов у нескольких мастеров, возьмите любого;
	count_order_created BIGINT NOT NULL, -- количество созданных заказов за месяц;
	count_order_in_progress BIGINT NOT NULL, -- количество заказов в процессе изготовки за месяц;
	count_order_delivery BIGINT NOT NULL, -- количество заказов в доставке за месяц;
	count_order_done BIGINT NOT NULL, -- количество завершённых заказов за месяц;
	count_order_not_done BIGINT NOT NULL, -- количество незавершённых заказов за месяц;
	report_period VARCHAR NOT NULL, -- отчётный период, год и месяц.
	source_id varchar NOT null,
	CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);

DROP TABLE IF EXISTS dwh_v2.load_dates_craftsman_report_datamart;
CREATE TABLE IF NOT EXISTS dwh_v2.load_dates_craftsman_report_datamart (
    id bigint GENERATED ALWAYS AS IDENTITY,
    load_dttm date not null,
    CONSTRAINT load_dates_craftsman_report_datamart_pk PRIMARY KEY(id)
);
