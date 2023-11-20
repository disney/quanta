
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
-- FIXME: select count(*) from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@11
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
-- insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('11','Carlo','888 Western','Boise','ID','87305','208-313-2211','business');
-- commit
-- insert into orders_qa (cust_id, order_id, order_date, ship_via) values('11','1004','2023-07-01T03:00:00','USPS');
-- commit
-- select count(*) from orders_qa where cust_id != null;@4
-- select o.* from customers_qa as c inner join orders_qa as o on o.cust_id == c.cust_id;@4
-- select o.* from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@12
-- -- insert into orders_qa (cust_id, order_id, order_date, ship_via) values('12','1005','2023-07-02T03:00:00','DHS');
-- select o.* from customers_qa as c inner join orders_qa as o on o.cust_id == c.cust_id;@4
-- select o.* from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id;@12
select * from customers_qa as c where c.cust_id in (select cust_id from orders_qa);@3
