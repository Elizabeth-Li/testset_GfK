DROP TABLE IF EXISTS testset;
CREATE TABLE testset(
productid INT NOT NULL,
brand VARCHAR(20),
RAM_GB INT,
HDD_GB INT,
GHz Decimal(10,2),
price INT,
primary key(productid)
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8/Uploads/B_dupilicate_pid.tsv' 
INTO TABLE testset 
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
#SELECT * FROM testset
SELECT
 productid,
 brand,
 Price,
RANK() OVER(PARTITION BY brand ORDER BY price DESC) AS Price_Rank
FROM testset
ORDER BY brand, price DESC;
SELECT
 MIN(HDD_GB) AS HDD_GB_min, 
 MAX(HDD_GB) AS HDD_GB_max
FROM testset;

SELECT 
    RAM_GB,
	GHz,
    (SELECT 
        COUNT(*)
        FROM
            testset
        WHERE
            a.RAM_GB = RAM_GB) AS total_of_group
FROM
    (SELECT 
        RAM_GB, GHz
    FROM
        testset
    ORDER BY RAM_GB, GHz) AS a;
SET @row_number:=0; 
SET @median_group:='';
SELECT
    @row_number:=CASE
        WHEN @median_group = RAM_GB THEN @row_number + 1
        ELSE 1
	END AS count_of_group,
    @median_group:= RAM_GB AS RAM_GB_group,
    RAM_GB,
    GHz,
    (SELECT
            COUNT(*)
		FROM
			testset
		WHERE
            a.RAM_GB = RAM_GB) AS toal_of_group
FROM
    (SELECT
         RAM_GB, GHz
    FROM
		 testset
	ORDER BY RAM_GB, GHz) AS a;
    
SET @row_number:=0; 
SET @median_group:='';
SELECT
    RAM_GB_group, AVG(GHz) AS GBz_median
FROM
    (SELECT
         @row_number:= CASE
                     WHEN @median_group = RAM_GB THEN @row_number + 1
                     ELSE 1
				END AS count_of_group,
                @median_group:= RAM_GB AS RAM_GB_group,
                RAM_GB,
                GHz,
                (SELECT
                        COUNT(*)
				    FROM
						testset
					WHERE
						a.RAM_GB = RAM_GB) AS total_of_group
    FROM
        (SELECT
		RAM_GB,GHz
	FROM
        testset
	ORDER BY RAM_GB,GHz) AS a) AS b
WHERE
     count_of_group BETWEEN total_of_group / 2.0 AND total_of_group / 2.0 + 1
GROUP BY RAM_GB_group