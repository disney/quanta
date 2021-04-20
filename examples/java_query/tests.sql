select @rownum from user360 where gender = 'M' and @timestamp between '2019-04-12T00' and '2019-04-13T00' limit 10;
select count(*) from user360 where gender in ('M', 'F') and @timestamp between '2019-04-12T00' and '2019-04-13T00';
select count(*) from content_activity where story_title like 'nfl' and @timestamp between '2019-04-12T00' and '2019-04-13T00';
select count(*) from user360 as u inner join content_activity as c on u.num_user_id = c.num_user_id where u.gender = 'M' and c.browser = 'Chrome' and @timestamp between '2019-04-12T00' and '2019-04-13T00';
select count(*) from user360 as u inner join content_activity as c on u.num_user_id = c.num_user_id inner join ads_activity as a on u.num_user_id = a.num_user_id where u.gender = 'M' and c.browser = 'Chrome' and a.browser = 'Chrome' and @timestamp between '2019-04-12T00' and '2019-04-13T00';
select count(*) from content_activity where browser = 'Chrome' and @timestamp between '2019-04-12T00' and '2019-04-13T00';
select count(*) from user360 where (gender = 'F' and is_league_manager = true) or age between 18 and 25 and @timestamp between '2019-04-12T00' and '2019-04-13T00';
select count(*) from user360 where gender = 'F' and age >= 55 and @timestamp between '2019-04-12T00' and '2019-04-13T00';
