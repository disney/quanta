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
-- Inner JOIN statements
select o.cust_id, o.order_id, o.ship_via, c.first_name from customers_qa as c inner join orders_qa as o on c.cust_id = o.cust_id where o.cust_id = '1';@2
select o.* from customers_qa as c inner join orders_qa as o on o.cust_id == c.cust_id;@3
select count(*) from customers_qa as c inner join orders_qa as o on o.cust_id == c.cust_id;@3
select customers_qa.* from customers_qa inner join orders_qa on customers_qa.cust_id = orders_qa.cust_id;@3
select orders_qa.* from customers_qa inner join orders_qa on customers_qa.cust_id = orders_qa.cust_id;@3
select orders_qa.* from orders_qa inner join customers_qa on customers_qa.cust_id = orders_qa.cust_id;@3
select * from customers_qa inner join orders_qa on customers_qa.cust_id = orders_qa.cust_id;@3
select * from customers_qa inner join orders_qa on customers_qa.cust_id = orders_qa.cust_id where customers_qa.cust_id = '12321';@0
select customers_qa.first_name, last_name from customers_qa inner join orders_qa on customers_qa.cust_id = orders_qa.cust_id where customers_qa.first_name != null;@3
select customers_qa.first_name, customers_qa.last_name from customers_qa inner join orders_qa on customers_qa.cust_id = orders_qa.cust_id where customers_qa.first_name != 'Abe';@1
select customers_qa.first_name, orders_qa.order_date from customers_qa inner join orders_qa on customers_qa.cust_id = orders_qa.cust_id;@3
select customers_qa.first_name, orders_qa.order_date from customers_qa inner join orders_qa on customers_qa.cust_id = orders_qa.cust_id where customers_qa.first_name != null;@3
select customers_qa.first_name, orders_qa.order_date from customers_qa inner join orders_qa on customers_qa.cust_id = orders_qa.cust_id where customers_qa.first_name = null;@0
-- Outer JOIN statements
select o.* from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@11
select c.first_name, c.cust_id, o.cust_id, o.order_id from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@11
select count(*) from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@11
select * from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@11
select c.first_name, c.cust_id, o.cust_id, o.order_id, hash.sha256(c.cust_id) as myHash from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@11
-- Subquery statements
select first_name, cust_id, hashedCustId, hash.sha256(cust_id) as myHash from customers_qa where cust_id not in (select cust_id from orders_qa);@8
select * from customers_qa where cust_id not in (select cust_id from orders_qa where cust_id = '10');@9
select c.first_name, c.cust_id from customers_qa as c where c.cust_id not in (select cust_id from orders_qa);@8
select count(*) from customers_qa as c where c.cust_id not in (select cust_id from orders_qa);@8
select count(*) from customers_qa as c where c.cust_id in (select cust_id from orders_qa);@3
select * from customers_qa as c where c.cust_id not in (select cust_id from orders_qa);@8
select * from customers_qa as c where c.cust_id in (select cust_id from orders_qa);@3
-- Inserts
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('11','Carlo','888 Western','Boise','ID','87305','208-313-2211','business');
commit
insert into orders_qa (cust_id, order_id, order_date, ship_via) values('11','1004','2023-07-01T03:00:00','USPS');
commit
select count(*) from orders_qa where cust_id != null;@4
select o.* from customers_qa as c inner join orders_qa as o on o.cust_id == c.cust_id;@4
select o.* from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@12
-- insert into orders_qa (cust_id, order_id, order_date, ship_via) values('12','1005','2023-07-02T03:00:00','DHS');
select o.* from customers_qa as c inner join orders_qa as o on o.cust_id == c.cust_id;@4
select o.* from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@12
