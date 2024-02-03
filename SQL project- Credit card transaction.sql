-- CREATE DATABASE SQL_PROJECT_1;
USE SQL_PROJECT_1;


-- 1- write a query to print top 5 cities with highest spends and their percentage contribution of total credit card spends 

WITH city_spend AS(
SELECT city, SUM(amount) AS total_spent, dense_rank() OVER(ORDER BY SUM(amount) DESC) AS rnk
FROM credit_card_transcations
GROUP BY city)

SELECT city, total_spent, total_spent *100/(SELECT SUM(amount) FROM credit_card_transcations) AS pct_contributions
FROM city_spend
WHERE rnk<=5;

-- 2- write a query to print highest spend month and amount spent in that month for each card type

WITH highest_stats AS(
SELECT  EXTRACT(MONTH FROM STR_TO_DATE(transaction_date, '%d-%b-%y')) AS highest_paying_month, card_type, SUM(amount) AS total_sales,
dense_rank() OVER(PARTITION BY card_type order by sum(amount) DESC) AS rnk
FROM credit_card_transcations
GROUP BY  EXTRACT(MONTH FROM STR_TO_DATE(transaction_date, '%d-%b-%y')), card_type)

SELECT highest_paying_month, total_sales, card_type
FROM highest_stats
WHERE rnk =1;


-- 3- write a query to print the transaction details(all columns from the table) for each card type when
-- it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)

WITH cumulative_sum AS(
SELECT *, SUM(amount) OVER(PARTITION BY card_type ORDER BY transaction_date,transaction_id) AS cum_sum
FROM credit_card_transcations
ORDER BY card_type,transaction_date,transaction_id),

Cumulative_rank AS(
SELECT *, dense_rank() OVER(PARTITION BY card_type ORDER BY cum_sum ASC) AS rn
FROM cumulative_sum
WHERE cum_sum>=1000000)

SELECT * FROM Cumulative_rank
WHERE rn=1;

-- 4- write a query to find city which had lowest percentage spend for gold card type
WITH percentage_spend AS
( SELECT city, SUM(amount) * 100 / (SELECT SUM(amount) FROM credit_card_transcations) AS percentage_spent
FROM credit_card_transcations
WHERE card_type = 'gold'
GROUP BY city),

percentage_spend_rank AS(
SELECT *, dense_rank() OVER(ORDER BY percentage_spent ASC) AS RNK
FROM percentage_spend )

SELECT * FROM percentage_spend_rank
WHERE RNK =1;


-- 5- write a query to print 3 columns:  city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)

WITH expense_calculation AS(
SELECT city, exp_type, SUM(amount) AS spent
FROM credit_card_transcations
GROUP BY city, exp_type
ORDER BY city),

spent_rank AS(
SELECT *, dense_rank() OVER(PARTITION BY city ORDER BY spent desc) AS rnk_desc,
dense_rank() OVER(PARTITION BY city ORDER BY spent ) AS rnk_asc
FROM expense_calculation)

SELECT city,
   MAX( CASE WHEN rnk_desc = 1 THEN exp_type END) AS highest_expense_type,
    MAX(CASE WHEN rnk_asc = 1 THEN exp_type END) AS lowest_expense_type
FROM spent_rank
WHERE rnk_desc=1 or  rnk_asc=1
GROUP BY city;
 -- CASE WHEN rnk_desc = 1 THEN exp_type END, CASE WHEN rnk_asc = 1 THEN exp_type END;


-- with cte as (
-- select city,exp_type,sum(amount) as total_spend
-- from credit_card_transcations
-- group by city,exp_type)
-- -- order by city,total_spend
-- , cte2 as (
-- select *
-- ,dense_rank() over(partition by city order by total_spend desc) rn_high
-- ,dense_rank() over(partition by city order by total_spend) rn_low
-- from cte)
-- select city
-- , max(case when rn_high=1 then exp_type end) as highest_expense_type
-- , max(case when rn_low=1 then exp_type end) as lowest_expense_type
-- from cte2
-- where rn_high=1 or rn_low=1
-- group by city;



-- 6- write a query to find percentage contribution of spends by females for each expense type
SELECT exp_type, SUM(amount) AS total_spend,
SUM(CASE WHEN gender='F' THEN amount ELSE 0 END) AS female_spend,
sum(CASE WHEN gender='F' THEN amount ELSE 0 END)*1.0/sum(amount)*100 AS female_contribution
FROM credit_card_transcations
GROUP BY exp_type
ORDER BY female_contribution ;

-- 7- which card and expense type combination saw highest month over month growth in Jan-2014

WITH CTE_1 AS(
SELECT card_type, exp_type, SUM(amount) AS total_spend,
EXTRACT(MONTH FROM STR_TO_DATE(transaction_date, '%d-%b-%y')) AS trans_month,
EXTRACT(YEAR FROM STR_TO_DATE(transaction_date, '%d-%b-%y')) AS trans_year
FROM credit_card_transcations
GROUP BY card_type,exp_type, 
EXTRACT(MONTH FROM STR_TO_DATE(transaction_date, '%d-%b-%y')), 
EXTRACT(YEAR FROM STR_TO_DATE(transaction_date, '%d-%b-%y'))
),

CTE_2 AS(
SELECT * , LAG(total_spend,1) OVER(PARTITION BY card_type,exp_type ORDER BY trans_year, trans_month) AS prev_month_spend
FROM CTE_1)

SELECT *, (total_spend-prev_month_spend) AS mom_growth
FROM CTE_2
ORDER BY  mom_growth DESC
LIMIT 1 ;


-- 8- during weekends which city has highest total spend to total no of transcations ratio 
WITH CTE_1 AS(
SELECT city, SUM(amount) AS total_spent, COUNT(transaction_id) AS total_transaction
FROM credit_card_transcations
WHERE  EXTRACT(DAY FROM STR_TO_DATE(transaction_date, '%d-%b-%y')) IN (6,7)
GROUP BY city),

CTE_2 AS(
SELECT city, total_spent, total_transaction, total_spent/total_transaction AS ration, dense_rank() OVER(ORDER BY total_spent/total_transaction DESC) AS rnk
FROM CTE_1)

SELECT city, total_spent, total_transaction, ration
FROM  CTE_2
WHERE rnk=1;


-- 9- which city took least number of days to reach its 500th transaction after the first transaction in that city

-- EXTRACT(DAY FROM STR_TO_DATE(transaction_date, '%d-%b-%y'))

WITH CTE_1 AS (
    SELECT
        city, STR_TO_DATE(transaction_date, '%d-%b-%y') AS formatted_transaction_date,
        row_number() OVER (PARTITION BY city ORDER BY STR_TO_DATE(transaction_date, '%d-%b-%y'), transaction_id) AS count_number
    FROM
        credit_card_transcations
)

SELECT
    city,
    MAX(formatted_transaction_date) AS max_transaction_date,
    MIN(formatted_transaction_date) AS min_transaction_date,
    DATEDIFF(MAX(formatted_transaction_date), MIN(formatted_transaction_date)) AS days_to_500
FROM
    CTE_1
WHERE
    count_number IN (1, 500)
GROUP BY
    city
HAVING
    COUNT(*) = 2
ORDER BY
    days_to_500;


