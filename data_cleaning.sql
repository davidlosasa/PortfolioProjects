/*
Author: David Losasa
Project: Layoffs Dataset Cleaning
Description: Data cleaning using SQL — removing duplicates, standardizing fields, 
             fixing dates, and handling null or blank values.
*/

/* ============================================================
   1. REMOVING DUPLICATES
   ============================================================ */

/*
Create a staging table that contains all the original data.
This allows us to revert back in case of any manipulation error.
*/

CREATE TABLE layoffs_staging LIKE layoffs;

INSERT INTO layoffs_staging
SELECT * FROM layoffs;


/* ---- Identify duplicate rows using ROW_NUMBER() ---- */

WITH duplicate_cte AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off,
                         percentage_laid_off, `date`, country, funds_raised_millions
        ) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


/*
Create a second staging table where we store the dataset along with the row_num column 
to allow deleting duplicates easily.
*/

CREATE TABLE layoffs_staging_2 (
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


/* Insert all data with row number computed */

INSERT INTO layoffs_staging_2
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off,
                     percentage_laid_off, `date`, country, funds_raised_millions
    ) AS row_num
FROM layoffs_staging;


/* ---- Remove duplicates ---- */

DELETE
FROM layoffs_staging_2
WHERE row_num > 1;


/* ============================================================
   2. STANDARDIZING DATA
   ============================================================ */

/* ---- Standardize company names (remove extra spaces) ---- */

UPDATE layoffs_staging_2
SET company = TRIM(company);


/* ---- Standardize industry names ---- */

/* Example: all 'Crypto...' variations become 'Crypto' */
UPDATE layoffs_staging_2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


/* ---- Standardize country names ---- */

UPDATE layoffs_staging_2
SET country = 'United States'
WHERE country LIKE 'United States%';


/* ---- Convert date strings to proper DATE type ---- */

UPDATE layoffs_staging_2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging_2
MODIFY COLUMN `date` DATE;


/* ============================================================
   3. HANDLING NULL AND BLANK VALUES
   ============================================================ */

/* ---- Identify rows with completely missing key values ---- */

SELECT *
FROM layoffs_staging_2
WHERE percentage_laid_off IS NULL
  AND total_laid_off IS NULL;


/* ---- Identify missing or blank industries ---- */

SELECT *
FROM layoffs_staging_2
WHERE industry IS NULL OR industry = ' ';


/* ---- Example: fix missing industry for Airbnb ---- */

UPDATE layoffs_staging_2
SET industry = 'Travel'
WHERE company = 'Airbnb'
  AND total_laid_off = 30;


/* ---- Fill missing industries using other rows from the same company ---- */

SELECT *
FROM layoffs_staging_2 t1
JOIN layoffs_staging_2 t2 ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = ' ')
  AND t2.industry IS NOT NULL;


/* ---- Remove rows that cannot be recovered ---- */

DELETE FROM layoffs_staging_2
WHERE percentage_laid_off IS NULL
  AND total_laid_off IS NULL;
