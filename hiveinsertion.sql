create database myproject5;
use myproject5;
create table word(killed string) stored as textfile;
load data local inpath '/home/hduser/census_records' overwrite into table word;
create table censusdata(age int,education string,maritalstatus string,gender string,taxfilerstatus string,income double
,parents string,countryofbirth string,citizenship string,weeksworked int) stored as textfile;
insert overwrite table censusdata select (trim(get_json_object(killed,"$.Age"))),(trim(get_json_object(killed,"$.Education"))),
(trim(get_json_object(killed,"$.MaritalStatus"))),(trim(get_json_object(killed,"$.Gender"))),(trim(get_json_object(killed,"$.TaxFilerStatus"))),
(trim(get_json_object(killed,"$.Income"))),(trim(get_json_object(killed,"$.Parents"))),(trim(get_json_object(killed,"$.CountryOfBirth"))),
(trim(get_json_object(killed,"$.Citizenship"))),(trim(get_json_object(killed,"$.WeeksWorked")))from word;
create index bookmark on table censusdata(maritalstatus) as 'compact'with deferred rebuild;
alter index bookmark on censusdata rebuild;
create table censuspartition(education string,maritalstatus string,gender string,taxfilerstatus string,income double
,parents string,countryofbirth string,citizenship string,weeksworked int)
partitioned by (age int)
clustered by (income) into 10 buckets
row format delimited
fields terminated by ','
stored as textfile;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.enforce.bucketing=true;
from censusdata a insert overwrite table censuspartition partition(age) select a.education,a.maritalstatus,a.gender,a.taxfilerstatus,a.income,a.parents,a.countryofbirth,a.citizenship,a.weeksworked,a.age distribute by a.age;
create table censusdata1(age int,education string,maritalstatus string,gender string,taxfilerstatus string,income double
,parents string,countryofbirth string,citizenship string,weeksworked int) row format delimited fields terminated by ',' stored as textfile;
insert overwrite table censusdata1 select (trim(get_json_object(killed,"$.Age"))),(trim(get_json_object(killed,"$.Education"))),
(trim(get_json_object(killed,"$.MaritalStatus"))),(trim(get_json_object(killed,"$.Gender"))),(trim(get_json_object(killed,"$.TaxFilerStatus"))),
(trim(get_json_object(killed,"$.Income"))),(trim(get_json_object(killed,"$.Parents"))),(trim(get_json_object(killed,"$.CountryOfBirth"))),
(trim(get_json_object(killed,"$.Citizenship"))),(trim(get_json_object(killed,"$.WeeksWorked")))from word;

