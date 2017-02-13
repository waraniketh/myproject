use myproject5;
drop table education;
drop table educationgrouping;
drop view group1;
drop view group2;
drop view group3;
drop view group4;
//creating table for choosing only columns we required
create table education(ageofpeople int,edcuationstatus string)
row format delimited 
fields terminated by ','
stored as textfile;
//inserting the data into the table
insert overwrite table education select age,education from censusdata where age >14 and age<80;
select ageofpeople,edcuationstatus,count(*) as mark from education where edcuationstatus='Doctorate degree(PhD EdD)' or edcuationstatus='Masters degree(MA MS MEng MEd MSW MBA)' group by edcuationstatus,ageofpeople order by mark desc limit 1;
//creating another table for finding the educated people for age groups 
create table educationgrouping(ageofpeople int,edcuationstatus string,numberofpeople int)
row format delimited 
fields terminated by ','
stored as textfile;
//insertion of data
insert overwrite table educationgrouping select ageofpeople,edcuationstatus,count(*) as mark from education where edcuationstatus='Doctorate degree(PhD EdD)' or edcuationstatus='Masters degree(MA MS MEng MEd MSW MBA)' group by edcuationstatus,ageofpeople;
create view group1 as select sum(numberofpeople) from educationgrouping where ageofpeople>=14 and ageofpeople<20;
create view group2 as select sum(numberofpeople) from educationgrouping where ageofpeople>=20 and ageofpeople<40;
create view group3 as select sum(numberofpeople) from educationgrouping where ageofpeople>=40 and ageofpeople<60;
create view group4 as select sum(numberofpeople) from educationgrouping where ageofpeople>=60 and ageofpeople<80;
select * from group1,group2,group3,group4;
drop view average1;
drop view average2;
drop view average3;
create view average1 as select round(avg(income),2) as testforaverage  from  censusdata where age>=40 and age<60;
create view average2 as select round(avg(income),2) as testforaverage1 from  censusdata where age>=20 and age<40;//FOR INCOME WITHLOWEDUCATED ELIMINATING SENIOR CITIZENS
create view average3 as select round(avg(income),2) as testforaverage2 from  censusdata where education='Less than 1st grade';
select * from average1,average2,average3;
select round((testforaverage2-testforaverage)*100/testforaverage,2) from average1,average3;
/* 
41	Masters degree(MA MS MEng MEd MSW MBA)	715
from this we can see that people with age 41 are people with highest education qualification 
NULL	7186	12249	3549
from this we can infer that people of age group 40 to 60 have more number of people with highest qualification.
1738.46	1811.35	1771.34
from this we can infer that average income of people with highesteducation qualification is less than people who are less educated.
1.89 difference of percentage of income
10th grade
11th grade
12th grade no diploma
1st 2nd 3rd or 4th grade
5th or 6th grade
7th and 8th grade
9th grade
Associates degree-academic program
Associates degree-occup /vocational
Bachelors degree(BA AB BS)
Children
Doctorate degree(PhD EdD)
High school graduate
Less than 1st grade
Masters degree(MA MS MEng MEd MSW MBA)
Prof school degree (MD DDS DVM LLB JD)
Some college but no degree
*/

