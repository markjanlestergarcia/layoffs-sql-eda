-- ==========================================================
-- SQL DATA CLEANING PROJECT: COMPANY LAYOFFS DATASET
-- ==========================================================
-- Goal:
--   1. Remove duplicates
--   2. Standardize the data (companies, industries, countries, dates)
--   3. Handle null or blank values
--   4. Remove irrelevant columns
--   5. Prepare data for analysis
-- ==========================================================


-- =========================================
-- STEP 1: Create a staging table (safe copy)
-- =========================================
CREATE TABLE layoffs_staging LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT COUNT(*) AS raw_row_count
FROM layoffs_staging;



-- =========================================
-- STEP 2: Identify and remove duplicates
-- =========================================
-- Use ROW_NUMBER() to mark duplicate rows
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
                            stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;  -- check duplicates


-- Create a second staging table to safely delete duplicates
CREATE TABLE layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT DEFAULT NULL,
  row_num INT
);

-- Insert data with row_num assigned
INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER(
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
                        stage, country, funds_raised_millions
       ) AS row_num
FROM layoffs_staging;

-- Remove duplicate rows (keep only row_num = 1)
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT COUNT(*) AS after_dedup_row_count
FROM layoffs_staging2;



-- =========================================
-- STEP 3: Standardize text fields
-- =========================================
-- Trim extra spaces from company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Standardize industry values (e.g., "Crypto" vs "Cryptocurrency")
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Fix inconsistent country values (e.g., "United States." -> "United States")
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';



-- =========================================
-- STEP 4: Standardize dates
-- =========================================
-- Convert from text to DATE format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;



-- =========================================
-- STEP 5: Handle null or blank values
-- =========================================
-- Find null or blank industries
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- Example: Airbnb had missing industry -> set to "Travel"
UPDATE layoffs_staging2
SET industry = 'Travel'
WHERE company = 'Airbnb'
  AND (industry IS NULL OR industry = '');

-- Fill null industries using other rows of the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;



-- =========================================
-- STEP 6: Remove irrelevant columns
-- =========================================
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;



-- =========================================
-- FINAL CHECK
-- =========================================
-- View cleaned dataset
SELECT *
FROM layoffs_staging2
LIMIT 20;

-- Row count after full cleaning
SELECT COUNT(*) AS final_row_count
FROM layoffs_staging2;
