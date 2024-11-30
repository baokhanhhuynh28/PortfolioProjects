-- DATA CLEANING
-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. Null Values and Blank Values
-- 4. Remove Any Columns or Rows

SELECT*
FROM layoffs;

-- STAGE 1: CREATING STAGING TABLE
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT*
FROM layoffs;

-- STAGE 2: REMOVING DUPLICATE VALUE
-- STEP I: CREATING STAGE TABLE 2 AND INSERT ROW NUMBER
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
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS ROW_NUM
FROM layoffs_staging;

-- STEP II: REMOVING DUPLICATE VALUE FILTER BY ROW NUMBER > 1
SELECT*
FROM layoffs_staging2
WHERE row_num > 1
;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- STAGE 3: STANDARDIZING
-- STEP I: TRIMMING
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- STEP II: CHECKING FOR OVERLAPPING VALUE BUT NOT DUPLICATED VALUE
-- For example, converting ‘Crypto Currency’ and ‘CryptoCurrency’ to become ‘Crypto’
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- STEP III: WORKING WITH NULL AND BLANK DATA
-- STEP 2: CONVERTING BLANK VALUE > NULL
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- LOOK AND MATCH NULL VALUES IN THE DATASET TO THEIR CORRECT VALUE
SELECT*
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- UPDATE NULL VALUES
UPDATE layoffs_staging2 as t1
JOIN layoffs_staging2 as t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- FINAL: REMOVING ROW NUMBER 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
