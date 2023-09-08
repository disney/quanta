quanta-admin drop orders_qa
quanta-admin drop customers_qa
quanta-admin create customers_qa
-- 10 partial insert statements
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('101','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('102','Abby','234 Main','Tacoma','WA','98011','425-333-2222','cell;business');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('103','Annie','345 Main','Seattle','WA','98072','425-333-2233','cell;home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('104','Axel','456 Main','Everett','WA','98021','425-333-2244','landline;home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('105','Bob','567 Main','Bellingham','WA','98033','425-333-2255','cell;home;business');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('106','Bill','678 Main','Leavenworth','WA','98826','425-333-2666','landline;business');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('107','Bailey','789 Main','Seattle','WA','98072','425-333-2277','cell;home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('108','Bill','777 Main','Wenatchee','WA','98800','425-333-2288','landline;home;business');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('109','Bob','888 Main','Gig Harbor','WA','98444','425-333-2299','home');
insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('110','Carl','999 Main','Spokane','WA','98231','425-333-2211','business');
-- 10 full insert statements
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('200','Bob','Madden','313 First Dr','Santa Fe','NM','99887',,'2010-01-03 00:00:00.000Z','2010-01-01 12:23:34.000Z','2011-01-05 01:02:03.000Z','aaaaabbbbbcccccdddd','887-222-3333','unknown',true,'2000-01-01',true,42,71,4,1)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('210','Iris','Henderson','999 Main Dr','Albequerque','NM','99887','2011-03-01 00:00:00.000Z','2011-01-05 01:02:03.000Z','2010-01-01 12:23:34.000Z','aaaaabbbbbcccccdddd','554-222-3333','',0,'2000-01-01',true,25,62,1,2);
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('220','Ted','Adams','977 Ute Ave','Denver','CO','92323','','2012-01-10 08:59:59.000Z','2014-03-03 23:59:59.000Z','aaaaabbbbbcccccdddd','907-222-3333',';',true,'2000-01-01',true,88,68,2,3)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('230','Larry','Russell','6823 Egret Ave','Tacoma','WA','98826','','2013-02-28 23:00:00.000Z','1923-05-22 12:12:31.000Z','aaaaabbbbbcccccdddd','222-222-3333','business',false,'2000-01-01',true,44,75,8,4)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('240','Forrest','Chambers','3232 Second St','Walla Walla','WA','98826','','2014-03-03 23:59:59.000Z','1969-07-16 20:17:11.000Z','aaaaabbbbbcccccdddd','242-222-3333','cell',true,'2000-01-01',true,16,59,3,5)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('250','Mike','Emerson','9293 Sans St','Boise','ID','91321','','2015-04-23 11:00:23.000Z','2014-03-03 23:59:59.000Z','aaaaabbbbbcccccdddd','144-222-3333','cell;',false,'2000-01-01',true,48,48,7,6)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('260','Henry','Talley','2422 Arco Rd','Billings','MT','98232','2015-05-04 00:00:00.000Z','2016-05-29 09:09:09.000Z','2016-05-29 09:09:09.000Z','aaaaabbbbbcccccdddd','555-222-3333','N/A',true,'2000-01-01',true,59,59,5,7)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('270','Aaron','Levy','1st Street','Colorado Springs','CO','92323','','2017-07-31 05:00:00.000Z','1842-07-04 01:31:23.000Z','aaaaabbbbbcccccdddd','907-222-3333','UNKNOWN',false,'2000-01-01',true,75,55,3,8)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('280','Keefer','Cash','9323 Semper Ave','Bellingham','WA','98282','','2018-12-25 11:11:11.000Z','2017-07-31 05:00:00.000Z','aaaaabbbbbcccccdddd','833-222-3333','cell;home;business',true,'2000-01-01',true,55,76,4,9)
insert into customers_qa (cust_id,first_name,last_name, address, city, state, zip, createdAtTimestamp, timestamp_micro, timestamp_millis, hashedCustId, phone, phoneType, isActive, birthdate, isLegalAge, age, height, numFamilyMembers, rownum) values('290','Bill','Gregory','224 Null Street','Wenatchee','WA','98826','2018-10-01 00:00:00.000Z','2019-07-04 01:31:23.000Z','2012-01-10 08:59:59.000Z','aaaaabbbbbcccccdddd','443-222-3333','home;business',true,'2000-01-01',true,5,72,1,10)

-- 10 random insert statements
insert into customers_qa (cust_id) values('300');
insert into customers_qa (cust_id, age, height, isActive) values('310',59,72.55, false);
insert into customers_qa (cust_id, birthdate) values('320','1962-06-29T00:00:00');
insert into customers_qa (cust_id, first_name, last_name, isLegalAge, isActive, height, zip) values('330', 'Theresa','Araki',1,0,68.25,'99821');
insert into customers_qa (cust_id, first_name, birthdate, isLegalAge, state, last_name) values('340', 'Carry','1964-06-11T00:00:00', true, 'AK','Smith');
insert into customers_qa (cust_id, last_name, timestamp_micro) values('350', 'Akins','1962-06-01T00:00:00');
insert into customers_qa (cust_id, phone, phoneType) values('360', '432-234-9292','cell');
insert into customers_qa (cust_id, phoneType) values('370', 'cell;home');
insert into customers_qa (cust_id, city, birthdate, age) values('380', 'Juneau', '1982-05-12', 41);
insert into customers_qa (cust_id, last_name, first_name, zip, state, city, address) values('390', 'Morris','Kale','99801','AK','Juneau','2424 Glacier Hwy');
commit


