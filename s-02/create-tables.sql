CREATE SCHEMA dwh_v2;

--- dwh_v2.d_craftsman ---

drop table if exists dwh_v2.d_craftsman;

create table if not exists dwh_v2.d_craftsman(
	craftsman_id int8 GENERATED ALWAYS AS IDENTITY,
	craftsman_ext_id int8 NOT NULL,
	craftsman_name varchar NOT NULL,
	craftsman_address varchar NOT NULL,
	craftsman_birthday date NOT NULL,
	craftsman_email varchar NOT NULL,
	load_dttm timestamp NOT NULL,
	source_id varchar NOT null,
	CONSTRAINT craftsman_pk PRIMARY KEY (craftsman_id)
);

--- dwh_v2.d_customer ---

drop table if exists dwh_v2.d_customer;

create table if not exists dwh_v2.d_customer(
	customer_id int8 GENERATED ALWAYS AS IDENTITY,
	customer_ext_id int8 NOT null,
	customer_name varchar NULL,
	customer_address varchar NULL,
	customer_birthday date NULL,
	customer_email varchar NOT NULL,
	load_dttm timestamp NOT NULL,
	source_id varchar NOT null,
	CONSTRAINT customers_pk PRIMARY KEY (customer_id)
);

--- dwh_v2.d_product ---

drop table if exists dwh_v2.d_product;

create table if not exists dwh_v2.d_product(
	product_id int8 GENERATED ALWAYS AS IDENTITY,
	product_ext_id int8 NOT NULL,
	product_name varchar NOT NULL,
	product_description varchar NOT NULL,
	product_type varchar NOT NULL,
	product_price int8 NOT NULL,
	load_dttm timestamp NOT NULL,
	source_id varchar NOT null,
	CONSTRAINT products_pk PRIMARY KEY (product_id)
);

--- dwh_v2.f_order ---

drop table if exists dwh_v2.f_order;

create table if not exists dwh_v2.f_order(
	id int8 GENERATED ALWAYS AS IDENTITY,
	order_id int8 NOT NULL,
	product_id int8 NOT NULL,
	craftsman_id int8 NOT NULL,
	customer_id int8 NOT NULL,
	order_created_date date NULL,
	order_completion_date date NULL,
	order_status varchar NOT NULL,
	load_dttm timestamp NOT NULL,
	source_id varchar NOT null,
	CONSTRAINT orders_pk PRIMARY KEY (id),
	CONSTRAINT orders_craftsman_fk FOREIGN KEY (craftsman_id) REFERENCES dwh_v2.d_craftsman(craftsman_id) ON DELETE RESTRICT,
	CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES dwh_v2.d_customer(customer_id) ON DELETE RESTRICT,
	CONSTRAINT orders_product_fk FOREIGN KEY (product_id) REFERENCES dwh_v2.d_product(product_id) ON DELETE RESTRICT
);
