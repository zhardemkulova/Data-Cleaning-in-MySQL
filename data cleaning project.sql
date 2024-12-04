# Data cleaning

SELECT * 
FROM layoffs;
#мы в этой части чистим данные чтобы на следующ уроке использ их иследовать
# 1. Remove duplicate if they have
# 2. Standartize the Data
# 3. Null values and blank values
# 4. Remove any Columns (есть случаи где мы должны и не должны, напр если есть столбец не нужный мы можем удалить чтобы в дальн работы было меньше)

 # то исходный материал если мы сделаем не правильно чтото мы не сможем вернуть все поэтому мы создадим копию и будем рабоать с ней
 
 CREATE TABLE layoff_staging
 LIKE layoffs;
 
 SELECT *
 FROM layoff_staging;
 
 INSERT INTO layoff_staging
 SELECT *
 FROM layoffs;
 
 
 #removing duplicate
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoff_staging
order by 10 desc; #теперь этот запрос можем исполь в позапросе или сте

WITH cte_duplicate AS 
(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, industry,location, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) AS row_num
	FROM layoff_staging
)
SELECT *
FROM cte_duplicate
WHERE row_num > 1 ;# В ЭТОМ ЗАПРОСЕ ЕСТЬ КОМПАНИЯ ОДА И МЫ ПРОВЕРИМ ПРАВДА ЛИ ЕСТЬ ДУБЛИКАТ ЭТОЙ КОМПАНИИ

SELECT *
FROM layoff_staging
WHERE company = 'Oda'; # и мы проверили то что тут не дубликаты разные country и funds_raised_millions, у нас ошибка из-за того что мы не сгруппировали все столбцыa

#исправим ошибку и создадим заново сте и сгр все столбцы на верху


 SELECT *
FROM layoff_staging
WHERE company = 'Casper';
 
 
 WITH cte_duplicate AS 
(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, industry,location, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) AS row_num
	FROM layoff_staging
)
DELETE
FROM cte_duplicate
WHERE row_num > 1 ;#МЫ ТАК УДАЛИТЬ НЕ СМОЖЕМ ТАК КАК НЕТ ВОЗМОЖНОСТИ ОБНОВИТЬ, НО В МАЙКРАСОФТ БЫЛО БЫ ЛЕГЧЕ А В МАЙ СКЬЮЭЛЬ ПО ДРУГОМУ

#ДЛЯ ТОГО ЧТОБЫ УДАЛИТЬ МЫ СОЗДАДИМ ЕЩЕ ОДНУ ТАБЛИЦУ И ОТТУДА УДАЛИМ ФАКТИЧЕСКИЙ СТОЛБЕЦ 

CREATE TABLE `layoff_staging3` (
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

 SELECT *
FROM layoff_staging3; 

INSERT INTO layoff_staging3
SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, industry,location, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) AS row_num
	FROM layoff_staging; #нельзя добавлять несколько раззззз! только 1раз ввод
 
 
 #МЫ ТЕПЕРЬ РОУ НАМ СОЗДАЛИ КАК СТОЛБЕЦ И  ТЕПЕРЬ МЫ МОЖЕМ УДАЛИТЬ
 
SELECT *
FROM layoff_staging3
WHERE row_num > 1; 
 
DELETE
FROM layoff_staging3
WHERE row_num > 1; 

SELECT*
FROM layoff_staging3
; 
 
SELECT*
FROM layoff_staging3
;

# Standartizing Data - поиск проблем и их исправление

SELECT*
FROM layoff_staging3;
#увидели пробел в столбце компания давайте удалим

SELECT company, TRIM(company)
FROM layoff_staging3;

UPDATE layoff_staging3
SET company=TRIM(company);

SELECT DISTINCT industry
FROM layoff_staging3
ORDER BY 1;#МЫ ТУТ ВИДИМ ТО ЧТО ИНДУСТРИЯ КРИСТО НАПИСАНО РАЗНЫМИ СЛОВАМИ КАК 'Crypto','Crypto Currency','CryptoCurrency' И МЫ ДОЛЖНЫ НАЗВАТЬ ИХ 1 ИНДУСТРИЕЙ

SELECT *
FROM layoff_staging3
WHERE industry LIKE 'Crypto%';

UPDATE layoff_staging3
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoff_staging3
ORDER BY 1; #вроде все ок теперь след столбец

SELECT DISTINCT country
FROM layoff_staging3
ORDER BY 1;# тут есть 'United States' и 'United States.' то есть с точкой надо точку удалить КРОМЕ = есть другой способ

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) #УДАЛЯЕТ .
FROM layoff_staging3
ORDER BY 1;
UPDATE layoff_staging3
SET country= TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


SELECT*
FROM layoff_staging3; #ТУТ ДАТА УКАЗАНО КАК ТЕКСТ А НЕ ДАТА НУЖНО ЭТО ИСПРАВИТЬ 

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')  #мы указали данные по порядку как указано было в тексте дата то есть сперва мес дата и год нужно писать именно так маленькими буквами а год большой
FROM layoff_staging3;

UPDATE layoff_staging3
SET `date`=STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT*
FROM layoff_staging3; # НО ТУТ ВСЕ ЕЩЕ ДАТА УКАЗАНО КАК ТЕКСТ

ALTER TABLE layoff_staging3
MODIFY `date` DATE; #ЭТО НЕЛЬЗЯ ДЕЛАТЬ В РОУ ТАБЛИЦЕ ТОЛЬКО В СТАЖ И ПОСЛЕ ИЗМЕНЕНИЯ


# 3. Null values and blank values

SELECT*
FROM layoff_staging3
WHERE industry IS NULL OR industry = ''; #ТУТ ВМЕСТО = ИСПОЛЬ IS ТАК КАК НЕ НАЙДЕТ

#МЫ ВИДИМ ЧТО НЕСКОЛЬКО КОМПАНИИ НЕ УКАЗАНО ИНДУСТРИЯ, КАК ЭЙРБНБ МЫ МОЖЕМ ПРОВЕРИТЬ УКАЗАНО ЛИ ИНД В ЭЙРБНБ В ДРУГОЙ СТРОКЕ ЧТОБЫ ЕГО ЗАПОЛНИТЬ
SELECT *
FROM layoff_staging3
WHERE company = 'Carvana'; #И ТУТ ВИДИМ ЧТО В ДРУГНОЙ СТРОКЕ УКАЗАНО ИНД ТРЭВЕЛ И МЫ ЭТО ДОЛЖНЫ ЗАПОЛНИТЬ

SELECT tb1.industry,tb2.industry 
FROM layoff_staging3 tb1
JOIN layoff_staging3 tb2
	ON tb1.company = tb2.company
    AND tb1.location = tb2.location
WHERE (tb1.industry IS NULL OR tb1.industry = '')
AND (tb2.industry IS NOT NULL AND tb2.industry!='');


UPDATE layoff_staging3 tb1
JOIN layoff_staging3 tb2
	ON tb1.company = tb2.company
SET tb1.industry = tb2.industry 
WHERE (tb1.industry IS NULL OR tb1.industry = '')
AND (tb2.industry IS NOT NULL AND tb2.industry!=''); #НЕ РАБОТАЕТ ПОТОМУ ЧТО МЫ МОЖЕМ ИЗМЕНИТЬ ТОЛЬКО НУЛЛ ИЛИ '' ПОЭТ '' ИЗМЕНЯЕМ НА НУЛЛ  И ЗАМЕНЯЕМ ЕГО НА ЗАПОЛ ДАННЫЕ 

UPDATE layoff_staging3
SET industry = NULL
WHERE industry = '';
#ТЕПЕРЬ ТОЛЬКО НУЛЛ ЕСТЬ МОЖЕМ ИЗМЕНИТЬ НА ЗАПОЛ


UPDATE layoff_staging3 tb1
JOIN layoff_staging3 tb2
	ON tb1.company = tb2.company
SET tb1.industry = tb2.industry 
WHERE tb1.industry IS NULL 
AND tb2.industry IS NOT NULL AND tb2.industry!='' ;


SELECT *
FROM layoff_staging3
WHERE company = 'Carvana'; #its work



SELECT *
FROM layoff_staging3;# мы смогли изменить данные там где могли а также есть total_laid_off и   percentage_laid_off где мы не може вручную написать поэм строку где нет данных 
#двоих столбцомв мы удаляем так как при иследовании нам нужны данные в будушем (no ne vsegda nado utochnyat)



# 4. Remove any Columns 
 SELECT *
 FROM layoff_staging3
 WHERE total_laid_off IS NULL 
 AND percentage_laid_off IS NULL;
 
DELETE
FROM layoff_staging3
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


SELECT *
FROM layoff_staging3;#ТЕПЕРЬ НАМ НАДО УДАЛИТЬ СТОЛБЕЦ РОУ НАМ
 
ALTER TABLE layoff_staging3
DROP COLUMN row_num;





