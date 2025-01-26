select * from layoffs order by date desc limit 10;

-- Check for duplicates and remove any

create table layoffs_staging as
	(select *,
		row_number() over(partition by company, country, date, total_laid_off, percentage_laid_off, stage) as row_num
	from layoffs);

select * from layoffs_staging ls where row_num > 1;

delete from layoffs_staging where row_num > 1;

-- Standardize data and fix errors

select distinct industry from layoffs_staging ls order by industry;

select * from layoffs_staging ls where industry is null or industry = '' or industry = 'NULL' order by company;

update layoffs_staging 
set industry = null where industry = 'NULL';

select * from layoffs_staging ls where company in (select company from layoffs_staging ls2 where industry is null) order by company;

update layoffs_staging ls1
set industry = (select industry from layoffs_staging ls2 where ls1.company = ls2.company and industry is not null limit 1)
where industry is null;

update layoffs_staging 
set industry = 'Crypto'
where lower(industry) like 'crypto%';

select distinct country from layoffs_staging ls order by country;

update layoffs_staging 
set country = 'United States'
where country = 'United States.';

select distinct stage from layoffs_staging ls order by stage;

select distinct location from layoffs_staging ls order by "location" ;

update layoffs_staging 
set "location" = 'Malmo'
where "location" = 'Malmö';

select date from layoffs_staging ls order by "date" desc;
select * from layoffs_staging ls where "date" is null or "date" = 'NULL';

alter table layoffs_staging add date_formatted DATE;
update layoffs_staging 
set date_formatted = date("date")
where date is not null and "date" <> 'NULL';

select * from layoffs_staging ls limit 5;


-- Remove any columns and rows that are not necessary 

select * from layoffs_staging ls where total_laid_off is null or total_laid_off = 'NULL';
select * from layoffs_staging ls where (total_laid_off is null or total_laid_off = 'NULL') and (percentage_laid_off is null or percentage_laid_off = 'NULL');

delete from layoffs_staging where (total_laid_off is null or total_laid_off = 'NULL') and (percentage_laid_off is null or percentage_laid_off = 'NULL');

alter table layoffs_staging drop column row_num;
alter table layoffs_staging drop column "date";

select country, company, date_formatted, sum(total_laid_off::int)
from layoffs_staging ls 
where total_laid_off <> 'NULL' and country in ('Netherlands', 'Germany', 'Italy')
group by 1, 2, 3 order by 4 desc;