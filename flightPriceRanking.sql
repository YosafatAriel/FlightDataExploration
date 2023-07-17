WITH destination AS (
SELECT DISTINCT "from", "to",
price
FROM flight_dataset 
),

price_ranking AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY "from", "to" ORDER BY price DESC) AS price_rank
FROM destination),

total_travel_id AS (
SELECT user_id, 
COUNT(DISTINCT travel_id) AS travel_frequency, 
price_rank
FROM flight_dataset AS fd
JOIN price_ranking AS cte_dua
	ON cte_dua."from" = fd."from"
	AND cte_dua."to" = fd."to"
	AND cte_dua.price = fd.price
GROUP BY user_id, price_rank
),

max_travel AS (
SELECT *,
MAX(travel_frequency) OVER(PARTITION BY user_id) AS max_transaction
FROM total_travel_id
)

SELECT u.user_id, u.company, u.name, 
u.gender, u.age, price_rank
FROM user_dataset AS u
JOIN max_travel AS ma
	ON u.user_id = ma.user_id
WHERE max_transaction = travel_frequency
