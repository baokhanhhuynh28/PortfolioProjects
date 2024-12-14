SELECT*
FROM layoffs;
---------------------------
-- CREATING A STAGING TABLE
CREATE TABLE layoffs_staging
LIKE layoffs;
  
INSERT layoffs_staging
SELECT*
FROM layoffs;

SELECT*
FROM layoffs_staging;
---------------------------
-- STAGE 1: REMOVE DUPLICATE VALUES
-- BY ADDING ROW NUMER
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT layoffs_staging2
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;
---------------------------
-- STAGE 2: STANDARDIZING DATA
-- 2A: TRIMMING WHITESPACE

-- SEE THE CHANGE BEFORE UPDATE:
SELECT company, TRIM(company)
FROM layoffs_staging2;
-- UPDATE THE TABLE:
UPDATE layoffs_staging2
SET company = TRIM(company);

-- 2B: CORRECT OVERLAPPING VALUES
-- PREVIEW DISTINCT VALUES, I FOUND 'CRYPTO', 'CRYPTO CURRENCY' AND 'CRYPTOCURRENCY' ARE DUPLICATES.
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry ASC;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 2C: CORRECT DATA TYPE FOR 'DATE' VALUES
-- 'DATA' IS CURRENTLY IDENTIFIED AS TEXT, I WILL UPDATE IT TO DATE.

SELECT `date`, STR_TO_DATE('date', '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;

SELECT*
FROM layoffs_staging2;
---------------------------
-- STAGE 3: NULL AND BLANK DATA CLEANUP
-- PREVIEW RESULTS WITH NULL AND BLANK DATA.
SELECT*
FROM layoffs_staging2
WHERE total_laid_off IS NULL
OR industry = "";

-- I WILL CONVERT ALL BLANK DATA TO CARRY NULL SO THEY WILL BE ALL UPDATED AT ONCE.
UPDATE  layoffs_staging2
SET industry = NULL
WHERE industry = "";

-- I WILL USE JOIN FUNCTION TO COVERT NULL DATA TO CARRY DATA SIMILAR TO ITS PEERS.
-- FOR EXAMPLE, COMPANY 'AIRBNB' HAS SOME DATA WITH INDUSTRY = TRAVEL AND SOME ARE NILL.
-- I WANT ALL ITS DATA TO HAVE INDUSTRY = TRAVEL.

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = "";

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- I WILL REMOVE ROWS WITH NULL VALUES IN BOTH COLUMNS 'total_laid_off' and 'percentage__laid_off'
-- AS THEY ARE NO USEFUL AND COULD BE INCORRECT RECORDS THAT COULD MISLEAD THE FINAL RESULT.
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
---------------------------

-- DROP 'row_num' COLUMN AS NO LONGER NEED
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

---------------------------
SELECT*
FROM layoffs_staging2;

-- PROJECT FINISH
-- THANK YOU FOR VIEWING.