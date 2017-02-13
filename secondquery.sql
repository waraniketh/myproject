use myproject5;
drop table weeks;
//create table for choosing required data
create table weeks(weeksworking int,incomeofworking double)
row format delimited 
fields terminated by ','
stored as textfile;
//insertion of data
insert overwrite table weeks select weeksworked,income from census;
//caluclating max number of weeks worked from given data
select max(weeksworking) as workload from weeks;
//caluculating max income from people working highest number of weeks
select weeksworked,max(income) as testforweeksworked from censusdata group by weeksworked order by weeksworked desc limit 1;
create view incomeofmaxweek as select weeksworked,max(income) as testforweeksworked from censusdata group by weeksworked order by weeksworked desc limit 1;//52 11465.2
//creating view for selecting highest income from the given data.
create view maxincomeofdata as select max(income) as maximum from censusdata order by maximum desc limit 1;//18656.3
//finding average income for people with highest number of weeks 
select weeksworked,avg(income) from (select max(weeksworking) as workload from weeks) as a,censusdata b where a.workload=b.weeksworked group by weeksworked;//1771.9899385777799
//finding percentage diff from maximum income earned income of person with highest income who is working the most number of weeks
select ((maximum-a.testforweeksworked)*100/a.testforweeksworked) from maxincomeofdata,incomeofmaxweek a;
//selecting top 5 people with highest income from table
select income from censusdata order by income limit 5;
//creating view for caluculaing average income of top5 people
create view averageincomeoftop5 as select avg(income) as incomeoftop5 from (select income from censusdata order by income desc limit 5) a;//17272.04
create view averageincomeofpeoplewithmostworking as select weeksworked,avg(income) as averageincomeofmost from (select max(weeksworking) as workload from weeks) as a,censusdata b where a.workload=b.weeksworked group by weeksworked;
//now finding diffenrence between top5 average and maximum income of most working people
select (incomeoftop5-averageincomeofmost)*100/averageincomeofmost from averageincomeofpeoplewithmostworking a,averageincomeoftop5 b;
