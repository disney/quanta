quanta-admin drop orders_qa
quanta-admin drop customers_qa
quanta-admin create customers_qa

-- Insert primary key only
insert into customers_qa (cust_id) values('3000');
commit
select count(*) from customers_qa where cust_id = '3000';@1
select count(*) from customers_qa where first_name = null and last_name = null and address = null and city = null and state = null and zip = null and createdAtTimestamp != null and timestamp_micro = null and timestamp_millis = null and hashedCustId != null and phone = null and phoneType = null and isActive = 1 and birthdate = null and isLegalAge = false and age = null and height = null and numFamilyMembers = 4 and rownum = null;@1

-- insert pk and booleans
insert into customers_qa (cust_id, isActive, isLegalAge) values('3010', false, true);
commit
select count(*) from customers_qa where cust_id = '3010' and isActive = false and isLegalAge = true;@1
insert into customers_qa (cust_id, isActive, isLegalAge) values('3011', 0, 1);
commit
select count(*) from customers_qa where cust_id = '3011' and isActive = 0 and isLegalAge = 1;@1

-- insert pk and date columns
insert into customers_qa (cust_id, createdAtTimestamp, timestamp_micro, timestamp_millis) values('3020','2010-01-03 00:00:00.000Z','2010-01-01 12:23:34.000Z','2011-01-05 01:02:03.000Z')
commit
select count(*) from customers_qa where cust_id = '3020' and createdAtTimestamp = '2010-01-03 00:00:00.000Z' and timestamp_micro = '2010-01-01 12:23:34.000Z' and timestamp_millis = '2011-01-05 01:02:03.000Z';@1

-- insert pk and numeric columns
insert into customers_qa (cust_id, age, height, numFamilyMembers, rownum) values('3030', 50, 72.55, 4, 3030);
commit
select count(*) from customers_qa where cust_id = '3030' and age = 50 and height = 72.55 and numFamilyMembers = 4 and rownum = 3030;@1

-- insert pk and enum columns
insert into customers_qa (cust_id, city, phoneType) values ('3040', 'Seattle', 'cell;home;business;satellite');
commit
select count(*) from customers_qa where cust_id = '3040' and city = 'Seattle' and phoneType = 'satellite';@1
select count(*) from customers_qa where cust_id = '3040' and city = 'Seattle' and phoneType != 'satellite';@0

-- insert pk and string columns
insert into customers_qa (cust_id, first_name, last_name, address, city, state, zip, phone, phoneType) values ('3050','John','Doe','1231312 Second Ave.', 'Tacoma','Washington','92323','555-222-3333','cell;cell2');
commit
select count(*) from customers_qa where cust_id = '3050' and first_name = 'John' and last_name = 'Doe' and address = '1231312 Second Ave.' and city = 'Tacoma' and zip = '92323' and phone = '555-222-3333' and phoneType = 'cell2';@1

-- insert columns with default values only
insert into customers_qa (cust_id, createdAtTimestamp, hashedCustId, isActive, isLegalAge, age, numFamilyMembers) values ('3060','2023-05-29 00:00:00.000Z', 'aaabbbcccdddeeefffggg', false, true, 1, 2);
commit
select count(*) from customers_qa where cust_id = '3060' and createdAtTimestamp = '2023-05-29 00:00:00.000Z' and hashedCustId = 'aaabbbcccdddeeefffggg' and isActive = 0 and isLegalAge = 1 and age = 1 and numFamilyMembers = 2;@1

-- Insert primary key only and validate the default values exist
insert into customers_qa (cust_id) values('3070');
commit
select count(*) from customers_qa where cust_id = '3070' and createdAtTimestamp != null and hashedCustId != null and isActive = true and isLegalAge = false and age = null and numFamilyMembers = 4;@1

-- Insert into all columns
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('3080','Bob','Madden','313 First Dr','Santa Fe','NM','99887','2010-01-03 00:00:00.000Z','2010-01-01 12:23:34.000Z','2011-01-05 01:02:03.000Z','aaaaabbbbbcccccdddd','887-222-3333','cell;unknown',true,'2003-02-28',true,42,71.99,3,1)
commit
select count(*) from customers_qa where cust_id = '3080' and first_name = 'Bob' and last_name = 'Madden' and address = '313 First Dr' and city = 'Santa Fe' and state = 'NM' and zip = '99887' and createdAtTimestamp = '2010-01-03 00:00:00.000Z' and timestamp_micro = '2010-01-01 12:23:34.000Z' and timestamp_millis = '2011-01-05 01:02:03.000Z' and hashedCustId = 'aaaaabbbbbcccccdddd' and phone = '887-222-3333' and phoneType = 'unknown' and isActive = 1 and birthdate = '2003-02-28' and isLegalAge = true and age =42 and height = 71.99 and numFamilyMembers = 3 and rownum = 1;@1
