quanta-admin drop orders_qa
quanta-admin drop customers_qa
quanta-admin create customers_qa

-- 10 full insert statements
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('200','Bob','Madden','313 First Dr','Santa Fe','NM','99887',,'2010-01-03 00:00:00.000Z','2010-01-01 12:23:34.000Z','2011-01-05 01:02:03.000Z','aaaaabbbbbcccccdddd','887-222-3333','unknown',true,'2000-01-01',true,42,71,4,1)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('210','Iris','Henderson','999 Main Dr','Albequerque','NM','99887','2011-03-01 00:00:00.000Z','2011-01-05 01:02:03.000Z','2010-01-01 12:23:34.000Z','aaaaabbbbbcccccdddd','554-222-3333','',0,'2000-01-01',true,25,62,1,2);
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('220','Ted','Adams','977 Ute Ave','Denver','CO','92323','','2012-01-10 08:59:59.000Z','2014-03-03 23:59:59.000Z','aaaaabbbbbcccccdddd','907-222-3333',';',true,'2000-01-01',true,88,68,2,3)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('230','Larry','Russell','6823 Egret Ave','Tacoma','WA','98826','','2013-02-28 23:00:00.000Z','1923-05-22 12:12:31.000Z','aaaaabbbbbcccccdddd','222-222-3333','business',false,'2000-01-01',true,44,75,8,4)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('240','Forrest','Chambers','3232 Second St','Walla Walla','WA','98826','','2014-03-03 23:59:59.000Z','1969-07-16 20:17:11.000Z','aaaaabbbbbcccccdddd','242-222-3333','cell',true,'2000-01-01',true,16,59,3,5)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('250','Mike','Emerson','9293 Sans St','Boise','ID','91321','','2015-04-23 11:00:23.000Z','2014-03-03 23:59:59.000Z','aaaaabbbbbcccccdddd','144-222-3333',false,'2000-01-01',true,48,48,7,6)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('260','Henry','Talley','2422 Arco Rd','Billings','MT','98232','2015-05-04 00:00:00.000Z','2016-05-29 09:09:09.000Z','2016-05-29 09:09:09.000Z','aaaaabbbbbcccccdddd','555-222-3333','N/A',true,'2000-01-01',true,59,59,5,7)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('270','Aaron','Levy','1st Street','Colorado Springs','CO','92323','','2017-07-31 05:00:00.000Z','1842-07-04 01:31:23.000Z','aaaaabbbbbcccccdddd','907-222-3333','UNKNOWN',false,'2000-01-01',true,75,55,3,8)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('280','Keefer','Cash','9323 Semper Ave','Bellingham','WA','98282','','2018-12-25 11:11:11.000Z','2017-07-31 05:00:00.000Z','aaaaabbbbbcccccdddd','833-222-3333','cell;home;business',true,'2000-01-01',true,55,76,4,9)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('290','Bill','Gregory','224 Null Street','Wenatchee','WA','98826','2018-10-01 00:00:00.000Z','2019-07-04 01:31:23.000Z','2012-01-10 08:59:59.000Z','aaaaabbbbbcccccdddd','443-222-3333','home;business',true,'2000-01-01',true,5,72,1,10)
commit

-- Prior to this the basic data load has completed

-- Attempt update of primary key
update customers_qa set cust_id = null where state = 'ID';@0\ERR:cannot update PK column customers_qa.cust_id

-- Update non-existant table
update puke set yakk = 'yakk' where state = 'ID';@0\ERR:Could not find a DataSource for that table "puke"

-- Update non-existant column
update customers_qa set yakk = 'yakk'  where state = 'ID';@0\ERR:attribute 'yakk' not found 

-- Must provide a predicate
update customers_qa set phoneType = null;@0\ERR:must provide a predicate

select count(*) from customers_qa;@10

-- Set an IntBSI column to a different value
update customers_qa set age = 99 where state = 'ID';@1
commit
select count(*) from customers_qa where age = 99;@1

-- Set an IntBSI column to null
update customers_qa set age = null where state = 'ID';@1
commit
select count(*) from customers_qa where age is null;@1


-- Set an StringHashBSI column to a different value
select count(*) from customers_qa where hashedCustId != null and state = 'CO';@2
update customers_qa set hashedCustId = 'XXX' where state = 'CO';@2
commit
select count(*) from customers_qa where hashedCustId = 'XXX' and state = 'CO';@2

-- Set an StringHashBSI column to null
update customers_qa set hashedCustId = null where state = 'CO';@2
commit
select count(*) from customers_qa where hashedCustId is null and state = 'CO';@2

-- Set a new value on a non-exclusive StringEnum
select count(*) from customers_qa where phoneType = 'cell' and phoneType = 'business';@1
select count(*) from customers_qa where phoneType is null and state = 'ID';@1
update customers_qa set phoneType = 'cell' where state = 'ID';@1
commit
select count(*) from customers_qa where phoneType = 'cell' and state = 'ID';@1
update customers_qa set phoneType = 'business' where state = 'ID';@1
commit
select count(*) from customers_qa where phoneType = 'cell' and phoneType = 'business';@2

-- Set a non-exclusive StringEnum to null
update customers_qa set phoneType = null where state = 'ID';@1
commit
select count(*) from customers_qa where phoneType = 'cell' and phoneType = 'business';@1
select count(*) from customers_qa where phoneType = 'cell';@2
select count(*) from customers_qa where phoneType = 'business';@3

-- Set a boolean to different value
select count(*) from customers_qa where isLegalAge = true;@10
update customers_qa set isLegalAge = false where state = 'ID';@1
commit
select count(*) from customers_qa where isLegalAge = true;@9

-- Set boolean value to null
select count(*) from customers_qa where isLegalAge is not null;@10
update customers_qa set isLegalAge = null where state = 'ID';@1
commit
select count(*) from customers_qa where isLegalAge is not null;@9

-- Set float value to something different
select count(*) from customers_qa where height = 0;@0
update customers_qa set height = 0 where state = 'ID';@1
commit
select count(*) from customers_qa where height != 0;@9

-- Set float value to null
select count(*) from customers_qa where height is null;@0
update customers_qa set height = null where state = 'ID';@1
commit
select count(*) from customers_qa where height is not null;@9


-- DELETE tests

-- Delete non-existant table
delete from puke where state = 'ID';@0\ERR:Could not find a DataSource for that table "puke"

-- Must provide a predicate
delete from customers_qa;@0\ERR:must provide a predicate

-- Empty delete
delete from customers_qa where cust_id is null;@0
commit

-- Simple delete
select count(*) from customers_qa;@10
delete from customers_qa where state = 'ID';@1
commit
select count(*) from customers_qa;@9

-- delete batch
delete from customers_qa where cust_id is not null;@9
commit
select count(*) from customers_qa;@0


