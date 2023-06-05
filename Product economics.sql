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
                  -- The query provides insights into the revenue generated per date and the contributions of new and old users to the total revenue.
      
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
SELECT date,
       revenue,
       costs,
       tax,
       gross_profit,
       total_revenue,
       total_costs,
       total_tax,
       total_gross_profit,
       round(gross_profit / revenue * 100, 2) as gross_profit_ratio,
       round(total_gross_profit / total_revenue * 100, 2) as total_gross_profit_ratio
FROM   (SELECT date, -- The date column from the main query.
               revenue, -- The revenue received on that day from the orders table.
               costs, -- The costs calculated based on the month using conditional expressions.
               tax, -- The tax amount calculated based on the product name and price.
               revenue - costs - tax as gross_profit, -- The gross profit for that day, calculated as revenue minus costs and tax.
               sum(revenue) OVER (ORDER BY date) as total_revenue, -- The cumulative total revenue up to that day, calculated as the sum of revenue ordered by date.
               sum(costs) OVER (ORDER BY date) as total_costs, -- The cumulative total costs up to that day, calculated as the sum of costs ordered by date.
               sum(tax) OVER (ORDER BY date) as total_tax, -- The cumulative total tax up to that day, calculated as the sum of tax ordered by date.
               sum(revenue - costs - tax) OVER (ORDER BY date) as total_gross_profit -- The cumulative total gross profit up to that day, calculated as the sum of gross_profit ordered by date.
        FROM   (SELECT date,
                       orders_packed,
                       orders_delivered,
                       couriers_count,
                       revenue,
                       case when date_part('month', date) = 8 then 120000 + 140 * coalesce(orders_packed, 0) + 150 * coalesce(orders_delivered, 0) + 400 * coalesce(couriers_count, 0)
                            when date_part('month', date) = 9 then 150000 + 115 * coalesce(orders_packed, 0) + 150 * coalesce(orders_delivered, 0) + 500 * coalesce(couriers_count, 0) end as costs,
                       tax
                FROM   (SELECT creation_time::date as date,
                               count(distinct order_id) as orders_packed,
                               sum(price) as revenue,
                               sum(tax) as tax
                        FROM   (
                               SELECT order_id,
                                      creation_time,
                                      product_id,
                                      name,
                                      price,
                                      case when name in ('сахар', 'сухарики', 'сушки', 'семечки', 'масло льняное', 'виноград', 'масло оливковое', 
                                                          'арбуз', 'батон', 'йогурт', 'сливки', 'гречка', 'овсянка', 'макароны', 'баранина', 'апельсины',
                                                          'бублики', 'хлеб', 'горох', 'сметана', 'рыба копченая', 'мука', 'шпроты', 'сосиски', 'свинина', 'рис',
                                                          'масло кунжутное', 'сгущенка', 'ананас', 'говядина', 'соль', 'рыба вяленая', 'масло подсолнечное', 'яблоки',
                                                          'груши', 'лепешка', 'молоко', 'курица', 'лаваш', 'вафли', 'мандарины') then round(price/110*10,  
                                            else round(price/120*20, 2) 
                                       end as tax
                                FROM   (
                                       SELECT order_id,
                                              creation_time,
                                              unnest(product_ids) as product_id
                                        FROM  orders
                                        WHERE order_id not in (SELECT order_id
                                                                FROM   user_actions
                                                                WHERE  action = 'cancel_order')
                                         ) t1 -- The subquery that retrieves order and product IDs from the "orders" table where action is 'cancel_order'.
                                  LEFT JOIN products using (product_id)
                                  ) t2 -- The subquery that joins the order and product information from t1 with the "products" table to calculate tax.
                        GROUP BY date
                               ) t3 -- The subquery that groups the data from t2 by date and calculates revenue, tax, and other aggregated values.
                    LEFT JOIN (
                               SELECT time::date as date,
                                      count(distinct order_id) as orders_delivered
                               FROM   courier_actions
                               WHERE  order_id not in (SELECT order_id
                                                       FROM   user_actions
                                                       'cancel_order')
                                      and action = 'deliver_order'
                               GROUP BY date
                              ) t4 using (date) -- The subquery that retrieves the count of delivered orders for each date from the "courier_actions" table, excluding canceled orders.
                    LEFT JOIN (
                               SELECT date,
                                      count(courier_id) as couriers_count
                               FROM   (
                                      SELECT time::date as date,
                                              courier_id,
                                              count(distinct order_id) as orders_delivered
                                       FROM   courier_actions
                                       WHERE  order_id not in (SELECT order_id
                                                               FROM   user_actions
                                                               WHERE  action = 'cancel_order')
                                          and action = 'deliver_order'
                                       GROUP BY date, courier_id having count(distinct order_id) >= 5
                                       ) t5 -- The subquery that groups the data from t4 by date and courier_id, and calculates the count of distinct orders delivered by each courier on each date, considering only those couriers with at least 5 delivered orders.
                                GROUP BY date
                               ) t6 using (date) -- The subquery that groups the data from t5 by date and calculates the count of couriers with at least 5 delivered orders for each date.
                    ) t7 -- The subquery that joins the data from t3, t4, and t6 based on the date column to combine all the calculated values for each date.
            ) t8 -- The subquery that calculates additional financial metrics by subtracting costs and tax from revenue and applying window functions to calculate running totals for revenue, costs, tax, and gross profit over dates.
                 -- Each alias is assigned to a specific subquery to organize and reference the intermediate results in a more readable and manageable manner.
                 -- In summary, the query provides information about revenue, costs, tax, gross profit, and cumulative totals for each day. The calculations consider the month of the date to determine the costs and apply different tax rates based on the product name.
