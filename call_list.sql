-- 1. Removing the duplicates

create table `customer call list_staging`
like `customer call list`;

select * from `customer call list_staging`;

insert  `customer call list_staging`
select * from `customer call list`;

select *, row_number() over(
partition by customerID) as row_num
from `customer call list_staging`;

with duplicate_cte as (
select *, row_number() over(
partition by customerID) as row_num
from `customer call list_staging`)

select * from duplicate_cte
where row_num > 1;

CREATE TABLE `customer call list_staging2` (
  `CustomerID` int DEFAULT NULL,
  `First_Name` text,
  `Last_Name` text,
  `Phone_Number` text,
  `Address` text,
  `Paying Customer` text,
  `Do_Not_Contact` text,
  `Not_Useful_Column` text,
  `MyUnknownColumn` text,
  `MyUnknownColumn_[0]` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into `customer call list_staging2`
select *, row_number() over(
partition by customerID) as row_num
from `customer call list_staging`;

select * from `customer call list_staging2`
where row_num >1;

delete 
from `customer call list_staging2`
where row_num >1;

-- now duplicates are deleted.

-- 2 . Standardizing the data

select * from `customer call list_staging2`;

select first_name, trim(first_name) from `customer call list_staging2`;
update `customer call list_staging2`
set first_name = trim(first_name);


select * from `customer call list_staging2`
where first_name like '% '; -- removed

select last_name, trim(last_name) from `customer call list_staging2`;

update `customer call list_staging2`
set last_name = trim(last_name);

select * from `customer call list_staging2`
where last_name like ' %'; -- removed

select * from `customer call list_staging2`;

select last_name, trim(leading '/' from  trim(leading '...' from last_name)) from `customer call list_staging2`;

update `customer call list_staging2`
set last_name = trim(leading '/' from  trim(leading '...' from last_name));

select last_name from `customer call list_staging2`;

select last_name, trim(trailing '_' from last_name) from `customer call list_staging2`;

update `customer call list_staging2`
set last_name = trim(trailing '_' from last_name);

select last_name from `customer call list_staging2`;

select * from `customer call list_staging2`;
 
 select `Paying Customer`, do_not_contact,
 case
 when `Paying Customer` like 'N' then 'No'
 when `Paying Customer` like 'Y' then 'Yes'
 else `Paying Customer`
 end as `Paying Customer`,
 case
 when do_not_contact like 'N' then 'No'
 when do_not_contact like 'Y' then 'Yes'
 else do_not_contact
 end as do_not_contact
 from `customer call list_staging2`; -- Replaced N and Y with No and Yes.
 
 update `customer call list_staging2`
 set  `Paying Customer` =  case
 when `Paying Customer` like 'N' then 'No'
 when `Paying Customer` like 'Y' then 'Yes'
 else `Paying Customer`
 end; -- updated the column paying customer
 
 update `customer call list_staging2`
 set do_not_contact=  case
 when do_not_contact like 'N' then 'No'
 when do_not_contact like 'Y' then 'Yes'
 else do_not_contact
 end; -- updated do not contact column.
 
 select * from `customer call list_staging2`;
 
 select phone_number,
 case
 when phone_number='7066950392' then
 concat(
 substr(phone_number, 1,3),'-',
 substr(phone_number,4,3), '-',
 substr(phone_number,7,4))
 else phone_number
 end
 from `customer call list_staging2`;
 
 select phone_number ,
replace(replace(phone_number, '/', '-'), '|', '-')
 from `customer call list_staging2`;
 
 update `customer call list_staging2`
 set phone_number = replace(replace(phone_number, '/', '-'), '|', '-');

 update `customer call list_staging2`
 set phone_number=  case
 when phone_number='7066950392' then
 concat(
 substr(phone_number, 1,3),'-',
 substr(phone_number,4,3), '-',
 substr(phone_number,7,4))
 else phone_number
 end;
 
 select phone_number from `customer call list_staging2`;
 
 select * from `customer call list_staging2`;
 
 select address
 from `customer call list_staging2`;
 
select address,
substring_index(address, ',' , 1),
substring_index(substring_index(address, ',' , 2), ',' , -1),
substring_index(substring_index(address, ',' , -2), ',' , 1)
 from `customer call list_staging2`;
 
 SELECT 
    address,
    substring_index(address, ',', 1) AS part1,
    substring_index(substring_index(address, ',', 2), ',', -1) AS part2,
    substring_index(address, ',', -1) AS part3
FROM 
    `customer call list_staging2`;
    
    SELECT 
    address,
    substring_index(address, ',', 1) AS part1,
    NULLIF(substring_index(substring_index(address, ',', 2), ',', -1), substring_index(address, ',', 1)) AS part2,
    NULLIF(substring_index(address, ',', -1), substring_index(substring_index(address, ',', 2), ',', -1)) AS part3
FROM 
    `customer call list_staging2`;

SELECT 
    substring_index(address, ',', 1) AS Street,
    COALESCE(NULLIF(substring_index(substring_index(address, ',', 2), ',', -1), substring_index(address, ',', 1)), '') AS State,
    COALESCE(NULLIF(substring_index(address, ',', -1), substring_index(substring_index(address, ',', 2), ',', -1)), '') AS Zip_code
FROM 
    `customer call list_staging2`;
 
 -- now to update this table
 
 -- Step 1: Add the new columns
ALTER TABLE `customer call list_staging2`
ADD COLUMN Street VARCHAR(255),
ADD COLUMN State VARCHAR(255),
ADD COLUMN Zip_code VARCHAR(20);

-- Step 2: Update the table with parsed values
UPDATE `customer call list_staging2`
SET 
    Street = substring_index(address, ',', 1),
    State = COALESCE(NULLIF(substring_index(substring_index(address, ',', 2), ',', -1), substring_index(address, ',', 1)), ''),
    Zip_code = COALESCE(NULLIF(substring_index(address, ',', -1), substring_index(substring_index(address, ',', 2), ',', -1)), '');

 -- dropping columns
 
 select phone_number,
 replace(phone_number,'N-a', '')
  from `customer call list_staging2`;
 
 update `customer call list_staging2`
 set phone_number =  replace(phone_number,'N-a', '');
 

 select * from `customer call list_staging2`;
 
 select `Paying Customer`, street,
 replace(`Paying Customer`, 'N/a' , 'No'),
 replace(street, 'N/a' , '')
 from `customer call list_staging2`;
 
 update `customer call list_staging2`
 set `Paying Customer` =  replace(`Paying Customer`, 'N/a' , 'No');
 
 update `customer call list_staging2`
 set street= replace(street, 'N/a' , '');
 
 alter table `customer call list_staging2`
 drop column not_useful_column,
 drop column address,
 drop column myunknowncolumn,
 drop column row_num;
 
 alter table `customer call list_staging2`
 drop column `MyUnknownColumn_[0]`;
 
 
 
 
