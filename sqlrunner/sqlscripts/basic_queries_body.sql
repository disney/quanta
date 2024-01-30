-- This is the body of the basic_queries.sql script

select * from customers_qa;@30
select first_name from customers_qa;@30
select * from customers_qa where first_name = 'Abe';@1
select * from customers_qa where city = 'Seattle';@3
select * from customers_qa where zip = '98072';@3
select distinct first_name from customers_qa;@20
select cust_id from customers_qa where cust_id = '101';@1
select cust_id from customers_qa where cust_id != '101';@29
select city from customers_qa where first_name = 'Bob';@3
select city from customers_qa where first_name != 'Bob';@27
select city from customers_qa where city = 'Sitka';@0
select city from customers_qa where city != 'Sitka';@30
select * from customers_qa where city = 'Seattle' and first_name = 'Annie' and zip = '99999';@0
select first_name from customers_qa where first_name = 'Bailey' and cust_id = '107' and city = 'Seattle' and state = 'WA';@1
select first_name from customers_qa where first_name = 'Carl' or city = 'Seattle';@4
select first_name from customers_qa where first_name = 'None' or city = 'Seattle';@3
select createdAtTimestamp from customers_qa where createdAtTimestamp != NULL;@30
select createdAtTimestamp from customers_qa where createdAtTimestamp = NULL;@0
select createdAtTimestamp from customers_qa where createdAtTimestamp is NULL;@0
select hashedCustId from customers_qa where hashedCustId != NULL;@30
select hashedCustId from customers_qa where hashedCustId = NULL;@0
-- select * from customers_qa where createdAtTimestamp > todate('now-1m');@26
select * from customers_qa where createdAtTimestamp > todate('now-1d');@26
select * from customers_qa where createdAtTimestamp < todate('now+1m');@30
-- select * from customers_qa where createdAtTimestamp < todate('now-1m');@4
select * from customers_qa where phoneType = 'cell';@10
select * from customers_qa where phoneType = 'home' or phoneType = 'landline';@11
select * from customers_qa where phoneType = 'none';@0
select * from customers_qa where city in ('Seattle','Tacoma','Everett');@6
select * from customers_qa where city not in ('Seattle','Tacoma','Everett');@24
select * from customers_qa where city not in ('Seattle','Tacoma','Everett') and city != null;@16
select * from customers_qa where city not in ('Seattle','Tacoma','Everett') and city is not null;@16
select timestamp_micro from customers_qa where timestamp_micro between '2010-01-01 00:00:00' and '2015-01-01 00:00:00';@5
select timestamp_micro from customers_qa where timestamp_micro not between '2010-01-01 00:00:00' and '2015-01-01 00:00:00';@25
select timestamp_micro from customers_qa where timestamp_micro not between '2010-01-01 00:00:00' and '2015-01-01 00:00:00' and timestamp_micro != null;@6
select timestamp_millis from customers_qa where timestamp_millis between '2010-01-01 00:00:00' and '2015-01-01 00:00:00';@5
select timestamp_millis from customers_qa where timestamp_millis not between '2010-01-01 00:00:00' and '2015-01-01 00:00:00';@25
select timestamp_millis from customers_qa where timestamp_millis not between '2010-01-01 00:00:00' and '2015-01-01 00:00:00' and timestamp_millis != null;@5
-- The following 3 between cases are failing because of bug https://jira.disneystreaming.com/browse/DFS-231
-- Follow up - between is not supported for string types - need to add these to error tests.
-- select cust_id from customers_qa where cust_id between '105' and '108';@4
-- select rownum from customers_qa where cust_id between '104' and '105';@2
-- select rownum from customers_qa where cust_id not between '104' and '105';@28
select age from customers_qa where age between 42 and 43;@1
select age from customers_qa where age not between 42 and 43;@29
select age from customers_qa where age not between 42 and 43 and age != null;@11
select height from customers_qa where height = 72.55000;@1
-- '\' means an additional line and ERR:error text means error expected
select height from customers_qa where height = 72.55001;@0\ERR:this would result in rounding error for field 'height', value should have 2 decimal places
select height from customers_qa where height between 74.9 and 75.1;@1
select height from customers_qa where height not between 75.0 and 75.1;@29
select height from customers_qa where height not between 75.0 and 75.1 and height != null;@11
select numFamilyMembers from customers_qa where numFamilyMembers between 4 and 5;@23
select numFamilyMembers from customers_qa where numFamilyMembers between 4 and 5 and numFamilyMembers != null;@23
select numFamilyMembers from customers_qa where numFamilyMembers not between 4 and 5;@7
select numFamilyMembers from customers_qa where numFamilyMembers not between 4 and 5 and numFamilyMembers != null;@7
select city from customers_qa limit 1;@1
select city from customers_qa limit 1000;@30
select first_name as fn, last_name as ln, city as c, createdAtTimestamp as cat, phoneType as pt from customers_qa;@30
select first_name as fn, last_name as ln, city as c, createdAtTimestamp as cat, phoneType as pt from customers_qa where fn = 'Bob' and ln = 'Burgers';@0
select first_name as fn, last_name as ln, city as c, createdAtTimestamp as cat, phoneType as pt from customers_qa where pt = 'cell';@10
select first_name as fn, last_name as ln, city as c, createdAtTimestamp as cat, phoneType as pt, age as Age from customers_qa where pt = 'cell' and Age = 55;@1
select first_name as fn, last_name as ln, city as c, createdAtTimestamp as cat, phoneType as pt, age as Age from customers_qa where pt = 'cell' or age = 55;@10
select first_name as fn, last_name as ln, city as c, createdAtTimestamp as cat, phoneType as pt, age as Age from customers_qa where pt = 'cell' and city = 'Walla Walla';@1
select count(*) from customers_qa where isActive = true;@24
select count(*) from customers_qa where isActive = false;@6
select count(*) from customers_qa where isActive = 1;@24
select count(*) from customers_qa where isActive = 0;@6
select count(*) from customers_qa where isActive != true;@6
select count(*) from customers_qa where isActive != false;@24
select count(*) from customers_qa where isActive != 1;@6
select count(*) from customers_qa where isActive != 0;@24
select max(age) as max_age from customers_qa where max_age = 88 limit 1;@1
select min(age) as min_age from customers_qa where min_age = 5 limit 1;@1
select avg(age) as avg_age from customers_qa where avg_age = 46 limit 1;@1
select sum(age) as sum_age from customers_qa where sum_age = 557 limit 1;@1
select avg(age) as avg_age from customers_qa where age between 43 and 54 and avg_age = 46 limit 1;@1
-- FIXME: (atw) This is returning a row even though avg(age) is 46. Zero seems correct and not 1.
-- select avg(age) as avg_age from customers_qa where age > 55 and avg_age = 70 limit 1;
select sum(age) as sum_age from customers_qa where age between 43 and 54 and sum_age = 92 limit 1;@1
select min(age) as min_age from customers_qa where age > 55 and min_age = 59;@1
select max(age) as max_age from customers_qa where age > 55 and max_age = 59;@1
select min(age) as min_age from customers_qa where age < 55 and min_age = 59;@1
select max(age) as max_age from customers_qa where age < 55 and max_age = 59;@1
select min(age) as min_age from customers_qa where age >= 55 and min_age = 59;@1
select max(age) as max_age from customers_qa where age >= 55 and max_age = 59;@1
select min(age) as min_age from customers_qa where age <= 55 and min_age = 59;@1
select max(age) as max_age from customers_qa where age <= 55 and max_age = 59;@1
select min(height) as min_height from customers_qa where min_height = 48 limit 1;@1
select max(height) as max_height from customers_qa where max_height = 76 limit 1;@1
select avg(height) as avg_height from customers_qa where avg_height = 0 limit 1;@1
select sum(height) as sum_height from customers_qa where sum_height = 0 limit 1;@1
