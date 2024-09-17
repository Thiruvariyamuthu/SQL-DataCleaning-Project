-- SQL Project - Data Cleaning

use world_layoffs;
SELECT *
FROM LAYOFFS;

-- First thing we want to do is create a staging table. 
-- This is the one we will work in and clean the data. 
-- We want a table with the raw data in case something happens

CREATE TABLE layoffs_staging_1
LIKE LAYOFFS;
INSERT into layoffs_staging_1
SELECT *
FROM LAYOFFS;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- 1. Remove Duplicates

# First let's check for duplicates

SELECT *
FROM LAYOFFS_STAGING_1;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
industry,total_laid_off,
percentage_laid_off,`date`,stage,
country,funds_raised_millions) as row_num
FROM layoffs_staging_1
)
SELECT *
FROM duplicate_cte
;
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
industry,total_laid_off,
percentage_laid_off,`date`,stage,
country,funds_raised_millions) as row_num
FROM layoffs_staging_1 
)
select *
FROM duplicate_cte
WHERE row_num>1;  

-- one solution, which I think is a good one. 
-- Is to create new table layoff_staging_2 with a new column and add those row numbers in.
-- Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

CREATE TABLE layoffs_staging_2
like layoffs_staging_1;

ALTER TABLE LAYOFFS_STAGING_2
ADD COLUMN row_num int;

INSERT INTO LAYOFFS_STAGING_2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
industry,total_laid_off,
percentage_laid_off,`date`,stage,
country,funds_raised_millions) as row_num
FROM layoffs_staging_1;

SELECT *
FROM LAYOFFS_STAGING_2
where row_num>1;

DELETE
FROM LAYOFFS_STAGING_2
where row_num>1;

SELECT *
FROM LAYOFFS_STAGING_2;  
  
-- 2. Standardizing data

SELECT TRIM(COMPANY)
FROM LAYOFFS_STAGING_2;

UPDATE LAYOFFS_STAGING_2
SET COMPANY=TRIM(COMPANY); 

-- I also noticed the Crypto has multiple different variations. 
-- We need to standardize that - let's say all to Crypto

SELECT DISTINCT(INDUSTRY)
FROM LAYOFFS_STAGING_2
ORDER BY 1;

UPDATE LAYOFFS_STAGING_2
SET INDUSTRY ='Crypto'
WHERE INDUSTRY LIKE 'Crypto%';

SELECT DISTINCT(INDUSTRY)
FROM LAYOFFS_STAGING_2
ORDER BY 1;

-- everything looks good except apparently we have some "United States" and some "United States." 
-- With a period at the end. Let's standardize this.

SELECT DISTINCT(COUNTRY)
FROM LAYOFFS_STAGING_2
ORDER BY COUNTRY;

UPDATE LAYOFFS_STAGING_2
SET COUNTRY=TRIM(TRAILING '.' FROM COUNTRY);

SELECT DISTINCT(COUNTRY)
FROM LAYOFFS_STAGING_2
ORDER BY COUNTRY;

-- converting the data type properly for date
ALTER TABLE LAYOFFS_STAGING_2
change `date` `date` DATE;

SELECT *
FROM LAYOFFS_STAGING_2; 

-- if we look at industry it looks like we have some null and empty rows 
-- let's take a look at these 

SELECT DISTINCT industry
FROM layoffs_staging_2
ORDER BY industry;

SELECT *
FROM layoffs_staging_2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM LAYOFFS_STAGING_2
WHERE COMPANY LIKE 'Airbnb%';
-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with

UPDATE LAYOFFS_STAGING_2
SET INDUSTRY=null
WHERE INDUSTRY =''; 

-- now if we check those are all null

SELECT*
FROM LAYOFFS_STAGING_2
WHERE INDUSTRY IS NULL 
OR INDUSTRY=''
ORDER BY INDUSTRY;

-- now we need to populate those nulls if possible

UPDATE LAYOFFS_STAGING_2 T1
JOIN LAYOFFS_STAGING_2 T2
    ON T1.COMPANY=T2.COMPANY
SET T1.INDUSTRY=T2.INDUSTRY
WHERE T1.INDUSTRY IS NULL
AND T2.INDUSTRY IS NOT NULL;    

-- and if we check it

SELECT *
FROM WORLD_LAYOFFS.LAYOFFS_STAGING_2
WHERE INDUSTRY IS NULL
OR INDUSTRY ='';
-- it looks like Bally's was the only one without a populated row to populate this null values





-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values




-- 4. remove any columns and rows we need to

SELECT*
FROM LAYOFFS_STAGING_2
WHERE TOTAL_LAID_OFF IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM LAYOFFS_STAGING_2
WHERE TOTAL_LAID_OFF IS NULL
AND PERCENTAGE_LAID_OFF IS NULL;

SELECT*
FROM LAYOFFS_STAGING_2;

ALTER TABLE LAYOFFS_STAGING_2
DROP COLUMN row_num;

SELECT*
FROM LAYOFFS_STAGING_2;
