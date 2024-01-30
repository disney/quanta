quanta-admin drop orders_qa
quanta-admin drop customers_qa
quanta-admin create customers_qa
quanta-admin create orders_qa
-- 10 insert statements
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('1','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('2','Abby','234 Main','Tacoma','WA','98011','425-333-2222','cell;business');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('3','Annie','345 Main','Seattle','WA','98072','425-333-2233','cell;home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('4','Axel','456 Main','Everett','WA','98021','425-333-2244','landline;home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('5','Bob','567 Main','Bellingham','WA','98033','425-333-2255','cell;home;business');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('6','Bill','678 Main','Leavenworth','WA','98826','425-333-2666','landline;business');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('7','Bailey','789 Main','Seattle','WA','98072','425-333-2277','cell;home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('8','Bill','777 Main','Wenatchee','WA','98800','425-333-2288','landline;home;business');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('9','Bob','888 Main','Gig Harbor','WA','98444','425-333-2299','home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('10','Carl','999 Main','Spokane','WA','98231','425-333-2211','business');
commit
insert into orders_qa (cust_id, order_id, order_date, ship_via) values('1','1001','2023-06-01T01:00:00','UPS');
insert into orders_qa (cust_id, order_id, order_date, ship_via) values('1','1002','2023-06-01T02:00:00','UPS');
insert into orders_qa (cust_id, order_id, order_date, ship_via) values('10','1003','2023-06-01T03:00:00','FEDEX');
commit
