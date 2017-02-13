use myproject5;
create table for filtering of data
create table education(ageofpeople int,edcuationstatus string)
row format delimited 
fields terminated by ','
stored as textfile;
inserting the data from original table 
insert overwrite table education select age,education from censusdata where age >14 and age<80;//eliminating childern and selecting required columns
select * from (select education from censuspartition) as a,(select education from censuspartition) as b group by a.edcuation;
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
select * from (select education from censuspartition) as a group by a.education;
select * from education where edcuationstatus='Doctorate degree(PhD EdD)' or edcuationstatus'Masters degree(MA MS MEng MEd MSW MBA)' group bu edcuationstatus,ageofpeople;
select count(*) from education where edcuationstatus='Doctorate degree(PhD EdD)' or edcuationstatus='Masters degree(MA MS MEng MEd MSW MBA)' group by edcuationstatus,ageofpeople;
select ageofpeople,edcuationstatus,count(*) as mark from education where edcuationstatus='Doctorate degree(PhD EdD)' or edcuationstatus='Masters degree(MA MS MEng MEd MSW MBA)' group by edcuationstatus,ageofpeople order by mark desc limit 1;

/ 
select ageofpeople,edcuationstatus,count(*) as mark from education where edcuationstatus='Doctorate degree(PhD EdD)' or edcuationstatus='Masters degree(MA MS MEng MEd MSW MBA)' group by edcuationstatus,ageofpeople order by mark desc limit 1;
people with age 41 has more number of citizens with highest qualification.
/
create table educationgrouping(ageofpeople int,edcuationstatus string,numberofpeople int)
row format delimited 
fields terminated by ','
stored as textfile;
select ageofpeople,edcuationstatus,count(*) as mark from education where edcuationstatus='Doctorate degree(PhD EdD)' or edcuationstatus='Masters degree(MA MS MEng MEd MSW MBA)' group by edcuationstatus,ageofpeople;
insert overwrite table educationgrouping
now do it for age groups like 14 to 20 ,20 to 40,40 to 60, 60 to 80.condition will be a.ageofpeople=b.ageofpeople; 
create view group1 as select sum(numberofpeople) from (select * from educationgrouping where ageofpeople>=14 and ageofpeople<20) d;
create view group2 as select sum(numberofpeople)  from (select * from educationgrouping where ageofpeople>=20 and ageofpeople<40) a;
create view group3 as select sum(numberofpeople)  from (select * from educationgrouping where ageofpeople>=40 and ageofpeople<60) b;
create view group4 as select sum(numberofpeople)  from (select * from educationgrouping where ageofpeople>=60 and ageofpeople<80) c;
select * from group1,group2,group3,group4;
/*
select sum(a.numberofpeople) as for20to40,sum(b.numberofpeople)as for40to60,sum(c.numberofpeople)as for60to80 from (select * from educationgrouping where ageofpeople>=20 and ageofpeople<40) a, (select * from educationgrouping where ageofpeople>=40 and ageofpeople<60) b,
(select * from educationgrouping where ageofpeople>=60 and ageofpeople<80) c;*/
