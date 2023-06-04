-- ANALYSIS OF PRODUCT METRICS "Grocery store"
-- 1.How popular the product is and how it grows over time:
-- DAU (Daily Active Users) 
SELECT time::date as dt, --  This is an expression that converts the time column to a date using the ::date syntax. The result of this expression is aliased as dt, which represents the date portion of the time column.
       COUNT(DISTINCT user_id) AS DAU -- This is an aggregate function that counts the distinct values of the user_id column. It calculates the number of unique users for each date. The result of this aggregation is aliased as DAU, which stands for Daily Active Users.
FROM user_actions -- This specifies the table from which the data is being retrieved. In this case, the table is user_actions.
GROUP BY dt -- This clause groups the result set by the dt (date) column. It ensures that the aggregation function is applied for each unique date in the user_actions table.
            -- The overall purpose of this query is to calculate the Daily Active Users (DAU) by counting the distinct users for each date present in the user_actions table. By grouping the results by date, you can obtain the DAU for each specific date.

-- WAU (Weekly Active Users) 
SELECT date_trunc('week', time) AS week, -- This is a function date_trunc() that truncates the time column to the week level. It converts the timestamp values to the starting date of the respective week. The result is aliased as week, representing the week starting date.
       COUNT(user_id) AS WAU -- This is an aggregate function that counts the number of occurrences of user_id in the user_actions table. It calculates the total number of user actions for each week. The result of this aggregation is aliased as WAU, which stands for Weekly Active Users.
FROM user_actions -- This specifies the table from which the data is being retrieved. In this case, the table is user_actions.
GROUP BY week -- This clause groups the result set by the week column, which represents the week starting date. It ensures that the aggregation function is applied for each unique week in the user_actions table.
              -- The overall purpose of this query is to calculate the Weekly Active Users (WAU) by counting the total number of user actions for each week. By grouping the results by the week starting date, you can obtain the WAU for each specific week.

-- MAU (Monthly Active Users) 
SELECT date_trunc('month',time) as month, -- This is a function date_trunc() that truncates the time column to the month level. It converts the timestamp values to the starting date of the respective month. The result is aliased as month, representing the month starting date.
       COUNT(user_id) as MAU -- This is an aggregate function that counts the number of occurrences of user_id in the user_actions table. It calculates the total number of user actions for each month. The result of this aggregation is aliased as MAU, which stands for Monthly Active Users.
FROM user_actions -- This specifies the table from which the data is being retrieved. In this case, the table is user_actions.
GROUP BY  month -- This clause groups the result set by the month column, which represents the month starting date. It ensures that the aggregation function is applied for each unique month in the user_actions table.
                -- The overall purpose of this query is to calculate the Monthly Active Users (MAU) by counting the total number of user actions for each month. By grouping the results by the month starting date, you can obtain the MAU for each specific month.

--2. Let's start with revenue - the most common indicator that will show how much income our service brings.
-- Compare today's revenue with the previous day:
SELECT date, -- The outer query selects the columns date, revenue, and performs additional calculations.
       revenue,
       sum(revenue) OVER (ORDER BY date) as total_revenue, -- calculates the running total of revenue by applying the window function SUM over the ordered dates.
       round(100 * (revenue - lag(revenue, 1) OVER (ORDER BY date))::decimal / lag(revenue, 1) OVER (ORDER BY date),
             2) as revenue_change -- calculates the percentage change in revenue compared to the previous date by utilizing the LAG window function. It calculates the difference between the current revenue and the previous revenue, divides it by the previous revenue, and multiplies by 100. The result is rounded to two decimal places.
FROM   (
       SELECT creation_time::date as date,
               sum(price) as revenue
        FROM   (SELECT creation_time,
                       unnest(product_ids) as product_id
                FROM   orders
                WHERE  order_id not in (SELECT order_id
                                        FROM   user_actions
                                        WHERE  action = 'cancel_order')
               ) t1 -- This subquery retrieves the creation_time and product_id from the orders table. It excludes the orders that are canceled based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').The results are aliased as t1.
            LEFT JOIN products using (product_id)
        GROUP BY date
        ) t2 -- This subquery builds on the previous subquery (t1) by joining with the products table using the product_id.It groups the data by the creation_time (converted to date using creation_time::date) and calculates the sum of the price as the revenue.The results are aliased as t2.
             -- The purpose of this query is to provide information about the daily revenue and the revenue change for each date, based on the orders data. The running total of revenue and the percentage change in revenue are also calculated to track the overall revenue trend.

-- 3.Based on revenue data, we will calculate several relative indicators that will show how much, on average,consumers are willing to pay for the services of our delivery service. Let's look at the following metrics 
-- Сount up ARPU (Average Revenue Per User), ARPPU (Average Revenue Per Paying User), AOV (Average Order Value):
SELECT date, -- The outer query selects the columns date, arpu, arppu, and aov by performing calculations using the data from the previous subqueries.
       round(revenue::decimal / users, 2) as arpu, -- calculates the Average Revenue Per User (ARPU) by dividing the revenue by the number of distinct users.
       round(revenue::decimal / paying_users, 2) as arppu, -- calculates the Average Revenue Per Paying User (ARPPU) by dividing the revenue by the number of distinct paying users.
       round(revenue::decimal / orders, 2) as aov -- calculates the Average Order Value (AOV) by dividing the revenue by the number of orders.
FROM   (
       SELECT creation_time::date as date,
               count(distinct order_id) as orders,
               sum(price) as revenue
        FROM   (
               SELECT order_id,
                       creation_time,
                       unnest(product_ids) as product_id
                FROM   orders
                WHERE  order_id not in (SELECT order_id
                                        FROM   user_actions
                                        WHERE  action = 'cancel_order')
               ) t1 -- This subquery retrieves the order_id, creation_time, and product_id from the orders table.It excludes the orders that are canceled based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').The results are aliased as t1.
            LEFT JOIN products using(product_id)
        GROUP BY date
       ) t2 -- This subquery builds on the previous subquery (t1) by joining with the products table using the product_id.It groups the data by the creation_time (converted to date using creation_time::date) and calculates the count of distinct order_id as orders and the sum of the price as the revenue.The results are aliased as t2.
    LEFT JOIN (SELECT time::date as date,
                      count(distinct user_id) as users
               FROM   user_actions
               GROUP BY date) t3 using (date) -- This subquery retrieves the time (converted to date using time::date) and calculates the count of distinct user_id as users from the user_actions table.It groups the data by the date.The results are aliased as t3.
    LEFT JOIN (SELECT time::date as date,
                      count(distinct user_id) as paying_users
               FROM   user_actions
               WHERE  order_id not in (SELECT order_id
                                       FROM   user_actions
                                       WHERE  action = 'cancel_order')
               GROUP BY date) t4 using (date) -- This subquery builds on the previous subquery (t3) by further filtering out canceled orders based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').It calculates the count of distinct user_id as paying_users.The results are aliased as t4.
ORDER BY date -- The results are ordered by the date.
             -- The purpose of this query is to calculate and provide insights into various revenue-related metrics, including ARPU, ARPPU, and AOV, based on the order and user activity data. These metrics can help analyze and understand the financial performance and user behavior of the business over time.

--4. Сalculate all the same metrics, but for each day we will take into account the accumulated revenue and all currently available data on the number of users and orders.
-- Thus, we will get dynamic ARPU, ARPPU and AOV and we will be able to track how it has changed over time, taking into account the data we receive.
SELECT date,
       round(sum(revenue) OVER (ORDER BY date)::decimal / sum(new_users) OVER (ORDER BY date),
             2) as running_arpu, -- calculates the running ARPU by dividing the cumulative sum of revenue by the cumulative sum of new users.
       round(sum(revenue) OVER (ORDER BY date)::decimal / sum(new_paying_users) OVER (ORDER BY date),
             2) as running_arppu, -- calculates the running ARPPU by dividing the cumulative sum of revenue by the cumulative sum of new paying users.
       round(sum(revenue) OVER (ORDER BY date)::decimal / sum(orders) OVER (ORDER BY date),
             2) as running_aov -- calculates the running AOV by dividing the cumulative sum of revenue by the cumulative sum of orders.
                               -- results are ordered by the date.
FROM   (
       SELECT creation_time::date as date,
               count(distinct order_id) as orders,
               sum(price) as revenue
        FROM   (
               SELECT order_id,
                       creation_time,
                       unnest(product_ids) as product_id
                FROM   orders
                WHERE  order_id not in (SELECT order_id
                                        FROM   user_actions
                                        WHERE  action = 'cancel_order')
               ) t1 -- This subquery retrieves the order_id, creation_time, and product_id from the orders table.It excludes the canceled orders based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').The results are aliased as t1.
            LEFT JOIN products using(product_id)
        GROUP BY date
        ) t2 -- This subquery builds on the previous subquery (t1) by joining with the products table using the product_id.It groups the data by the creation_time (converted to date using creation_time::date) and calculates the count of distinct order_id as orders and the sum of the price as the revenue.The results are aliased as t2.
    LEFT JOIN (
               SELECT time::date as date,
                      count(distinct user_id) as users
               FROM   user_actions
               GROUP BY date
              ) t3 using (date) -- This subquery retrieves the time (converted to date using time::date) and calculates the count of distinct user_id as users from the user_actions table.It groups the data by the date.The results are aliased as t3.
    LEFT JOIN (
              SELECT time::date as date,
                      count(distinct user_id) as paying_users
               FROM   user_actions
               WHERE  order_id not in (SELECT order_id
                                       FROM   user_actions
                                       WHERE  action = 'cancel_order')
               GROUP BY date
               ) t4 using (date) -- This subquery builds on the previous subquery (t3) by further filtering out canceled orders based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').It calculates the count of distinct user_id as paying_users.The results are aliased as t4.
    LEFT JOIN (
               SELECT date,
                      count(user_id) as new_users
               FROM   (SELECT user_id,
                              min(time::date) as date
                       FROM   user_actions
                       GROUP BY user_id
               ) t5 
               GROUP BY date
     ) t6 using (date) -- This subquery retrieves the date and calculates the count of distinct user_id as new_users.It first finds the earliest date for each user_id from the user_actions table using a subquery (t5), and then counts the distinct user_id per date.The results are grouped by date and aliased as t6.
    LEFT JOIN (
               SELECT date,
                      count(user_id) as new_paying_users
               FROM   (SELECT user_id,
                              min(time::date) as date
                       FROM   user_actions
                       WHERE  order_id not in (SELECT order_id
                                               FROM   user_actions
                                               WHERE  action = 'cancel_order')
                       GROUP BY user_id) t7
               GROUP BY date
              ) t8 using (date) -- This subquery builds on the previous subquery (t6) by further filtering out canceled orders based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').It calculates the count of distinct user_id as new_paying_users.The results are grouped by date and aliased as t8.
              -- The purpose of this query is to calculate and provide insights into running averages for ARPU, ARPPU, and AOV. These metrics help track the trends and performance of revenue and user-related metrics over time, considering the cumulative data as the date increases.
-- 5.Separately, we calculate the daily revenue from the orders of new users of our service.
-- Let's see what share it is in the total revenue from orders of all users - both new and old.
SELECT date, -- The date column from the main query
       revenue, -- The total revenue per date from the orders table
       new_users_revenue, -- The revenue from new users based on their start date
       round(new_users_revenue / revenue * 100, 2) as new_users_revenue_share, -- The percentage of revenue contributed by new users
       100 - round(new_users_revenue / revenue * 100, 2) as old_users_revenue_share -- The percentage of revenue contributed by old users
FROM   
(
       SELECT creation_time::date as date,
               sum(price) as revenue
        FROM   (
               SELECT order_id,
                       creation_time,
                       unnest(product_ids) as product_id
                FROM   orders
                WHERE  order_id not in (SELECT order_id
                                        FROM   user_actions
                                        WHERE  action = 'cancel_order')
                ) t3 -- This subquery retrieves the order_id, creation_time, and product_id from the orders table.It excludes canceled orders based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').The results are aliased as t3.
            LEFT JOIN products using (product_id)
        GROUP BY date
) t1 -- This subquery builds on the previous subquery (t3) by joining with the products table using the product_id.It groups the data by the creation_time (converted to date using creation_time::date) and calculates the sum of price as the total revenue.The results are aliased as t1.
LEFT JOIN  (
            SELECT start_date as date,
                   sum(revenue) as new_users_revenue
            FROM (
                  SELECT t5.user_id,
                         t5.start_date,
                         coalesce(t6.revenue, 0) as revenue
                   FROM  (
                          SELECT user_id,
                                      min(time::date) as start_date
                           FROM   user_actions
                           GROUP BY user_id
                         ) t5 -- This subquery retrieves the user_id and the minimum time::date as the start_date from the user_actions table. It groups the data by user_id. The results are aliased as t5.
                 LEFT JOIN (
                           SELECT user_id,
                                  date,
                                  sum(order_price) as revenue
                           FROM (
                                 SELECT user_id,
                                        time::date as date,
                                        order_id
                                 FROM   user_actions
                                 WHERE  order_id not in (SELECT order_id
                                                         FROM   user_actions
                                                         WHERE  action = 'cancel_order')
                                ) t7 -- This subquery retrieves the user_id, time::date as date, and order_id from the user_actions table.It excludes canceled orders based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').The results are aliased as t7.
                                LEFT JOIN (
                                           SELECT order_id,
                                                  sum(price) as order_price
                                           FROM (
                                                 SELECT order_id,
                                                    unnest(product_ids) as product_id
                                                 FROM   orders
                                                 WHERE  order_id not in (SELECT order_id
                                                                         FROM   user_actions
                                                                         WHERE  action = 'cancel_order'
                                                ) t9 -- This subquery retrieves the order_id and product_id from the orders table.It excludes canceled orders based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').The results are aliased as t9.
                                                LEFT JOIN products using (product_id)
                                           GROUP BY order_id
                                       ) t8 using (order_id) -- This subquery builds on the previous subquery (t9) by joining with the products table using the product_id.It groups the data by order_id and calculates the sum of price as order_price.The results are aliased as t8.
                           GROUP BY user_id, date
                        ) t6 ON t5.user_id = t6.user_id and t5.start_date = t6.date 
                ) t4 -- This subquery joins the previous subquery t5 with the subquery t6 on user_id and start_date. It retrieves the user_id, start_date, and revenue, which represents the revenue from new users based on their start date. The coalesce function is used to handle cases where there is no revenue for a specific user and date combination, setting it to 0. The results are aliased as t4.                  
           GROUP BY start_date -- Groups the data by the start_date
) t2 using (date) -- Joins the main query with the subquery t2 using the date column
        
      
-- Let's see which products are in the greatest demand and bring us the main income.
SELECT product_name,
       sum(revenue) as revenue,
       sum(share_in_revenue) as share_in_revenue
FROM   (
        SELECT case when round(100 * revenue / sum(revenue) OVER (), 2) >= 0.5 then name
                    else 'ДРУГОЕ' end as product_name,
               revenue,
               round(100 * revenue / sum(revenue) OVER (), 2) as share_in_revenue
        FROM   (
               SELECT name,
                       sum(price) as revenue
                FROM   (SELECT order_id,
                               unnest(product_ids) as product_id
                        FROM   orders
                        WHERE  order_id not in (SELECT order_id
                                                FROM   user_actions
                                                WHERE  action = 'cancel_order')
                 ) t1 -- -- This subquery retrieves the order_id and unnested product_id from the orders table.It excludes canceled orders based on the condition order_id not in (SELECT order_id FROM user_actions WHERE action = 'cancel_order').The results are aliased as t1.
                 LEFT JOIN products using(product_id) - This join links the product_id from t1 with the product_id in the products table. The results are aliased as t2.
                 GROUP BY name -- This groups the data by the product name and calculates the sum of the price as revenue. The results are aliased as t2.
                 ) t2
       ) t3
GROUP BY product_name -- This groups the data by the product name.
ORDER BY revenue desc -- This sorts the result by revenue in descending order.
                      -- The purpose of this query is to calculate the revenue and share of revenue for each product. The result is grouped by product_name and ordered by revenue in descending order. 
        
-- Let us take into account the costs with taxes in our calculations and calculate the gross profit,
-- that is, the amount that we actually received as a result of the sale of goods for the period under review.
--1. Revenue received on that day.
--2. Costs generated on this day.
--3. The amount of VAT on the sale of goods on that day.
--4. Gross profit for that day (revenue minus costs and VAT).
--5. Total revenue for the current day.
--6. Total costs for the current day.
--7. Total VAT for the current day.
--8. Total gross profit for the current day.
--9. The share of gross profit in the proceeds for this day (the share of item 4 in item 1).
--10.The share of the total gross profit in the total revenue for the current day (the share of clause 8 in clause 5).
--When calculating VAT, keep in mind that for some goods the tax is 10%, not 20%. List of goods with reduced VAT:
with main as (SELECT date,
                     revenue,
                     costs
              FROM   (SELECT DISTINCT date,
                                      sum(price) OVER (PARTITION BY date) as revenue
                      FROM   (SELECT order_id,
                                     creation_time::date as date ,
                                     unnest(product_ids) as product_id
                              FROM   orders
                              WHERE  order_id not in (SELECT order_id
                                                      FROM   user_actions
                                                      WHERE  action = 'cancel_order')) as price_o
                          LEFT JOIN products using(product_id)) as reven
                  LEFT JOIN (SELECT date,
                                    costs_by_day+costs1 as costs FROM(SELECT date,
                                                                      case when date_part('month', date) = 8 then (c_ord*140)+120000
                                                                           when date_part('month', date) != 8 then (c_ord*115)+150000 end costs_by_day
                                                               FROM   (SELECT creation_time::date as date,
                                                                              count(distinct order_id) as c_ord
                                                                       FROM   orders
                                                                       WHERE  order_id not in (SELECT order_id
                                                                                               FROM   user_actions
                                                                                               WHERE  action = 'cancel_order')
                                                                       GROUP BY creation_time::date) as by_day) as by_d
                                 LEFT JOIN (SELECT DISTINCT date,
                                                            sum(costs2) OVER (PARTITION BY date) as costs1
                                            FROM   (SELECT date,
                                                           count_or,
                                                           case when date_part('month', date) = 8 and
                                                                     count_or >= 5 then (count_or*150)+400
                                                                when date_part('month', date) = 8 and
                                                                     count_or < 5 then (count_or*150)
                                                                when date_part('month', date) != 8 and
                                                                     count_or >= 5 then (count_or*150)+500
                                                                when date_part('month', date) != 8 and
                                                                     count_or < 5 then (count_or*150) end costs2
                                                    FROM   (SELECT DISTINCT time::date as date,
                                                                            courier_id,
                                                                            count(order_id) as count_or
                                                            FROM   courier_actions
                                                            WHERE  action = 'deliver_order'
                                                               and order_id not in (SELECT order_id
                                                                                 FROM   user_actions
                                                                                 WHERE  action = 'cancel_order')
                                                            GROUP BY time::date, courier_id) as cost_courier                 
