/*loadingdata*/
loadingdata = load '/home/hduser/dataforjava' using PigStorage(',') as (age : int,eductaion : chararray,martialstatus : chararray,gender : chararray,taxfilerstatus : chararray,income : double,parents : chararray,countryofbirth : chararray,citizenship : chararray,weekworked : int);
/*getting age of highest qualification*/
highesteducation = filter loadingdata by eductaion=='Masters degree(MA MS MEng MEd MSW MBA)' or eductaion=='Doctorate degree(PhD EdD)'; 
//generating groups for counting
agegroup1 = filter highesteducation by age>14 and age<20;
agegroup2 = filter highesteducation by age>=20 and age<40; 
agegroup3 = filter highesteducation by age>=40 and age<60; 
agegroup4 = filter highesteducation by age>=60 and age<80; 
countingpeople1 = group agegroup1 all;
countingpeople2 = group agegroup2 all;
countingpeople3 = group agegroup3 all;
countingpeople4 = group agegroup4 all;
//counting numberof people in age groups
generatingcount1 = foreach countingpeople1 generate COUNT(agegroup1) as numberofpeople1; 
generatingcount2 = foreach countingpeople2 generate COUNT(agegroup2) as numberofpeople2; 
generatingcount3 = foreach countingpeople3 generate COUNT(agegroup3) as numberofpeople3;
generatingcount4 = foreach countingpeople4 generate COUNT(agegroup4) as numberofpeople4;  
caluculatingincome = group agegroup3 all;
//average of topeducated group
averageincomeofhighestedu = foreach caluculatingincome generate group as age,ROUND(AVG(agegroup3.income),2) as averageofagegroup;
generatingroup = group caluculatingincome by age;

