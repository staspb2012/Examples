-- ANALYSIS OF PRODUCT METRICS "Grocery store"
-- How popular the product is and how it grows over time:
-- DAU (Daily Active Users) 
SELECT time::date as dt,
       COUNT(DISTINCT user_id) AS DAU
FROM user_actions
GROUP BY dt

-- WAU (Weekly Active Users) 
SELECT date_trunc('week', time) AS week,
       COUNT(user_id) AS WAU
FROM user_actions
GROUP BY week

-- MAU (Monthly Active Users) 
SELECT date_trunc('month',time) as month,
       COUNT(user_id) as MAU
FROM user_actions
GROUP BY  month

-- Let's start with revenue - the most common indicator that will show how much income our service brings.
-- Compare today's revenue with the previous day:
SELECT date,
       revenue,
       sum(revenue) OVER (ORDER BY date) as total_revenue,
       round(100 * (revenue - lag(revenue, 1) OVER (ORDER BY date))::decimal / lag(revenue, 1) OVER (ORDER BY date),
             2) as revenue_change
FROM   (SELECT creation_time::date as date,
               sum(price) as revenue
        FROM   (SELECT creation_time,
                       unnest(product_ids) as product_id
                FROM   orders
                WHERE  order_id not in (SELECT order_id
                                        FROM   user_actions
                                        WHERE  action = 'cancel_order')) t1
            LEFT JOIN products using (product_id)
        GROUP BY date) t2
-- Now, based on revenue data, we will calculate several relative indicators that will show how much, on average,
-- consumers are willing to pay for the services of our delivery service. Let's look at the following metrics 
-- Сount up ARPU (Average Revenue Per User), ARPPU (Average Revenue Per Paying User), AOV (Average Order Value):
SELECT date,
       round(revenue::decimal / users, 2) as arpu,
       round(revenue::decimal / paying_users, 2) as arppu,
       round(revenue::decimal / orders, 2) as aov
FROM   (SELECT creation_time::date as date,
               count(distinct order_id) as orders,
               sum(price) as revenue
        FROM   (SELECT order_id,
                       creation_time,
                       unnest(product_ids) as product_id
                FROM   orders
                WHERE  order_id not in (SELECT order_id
                                        FROM   user_actions
                                        WHERE  action = 'cancel_order')) t1
            LEFT JOIN products using(product_id)
        GROUP BY date) t2
    LEFT JOIN (SELECT time::date as date,
                      count(distinct user_id) as users
               FROM   user_actions
               GROUP BY date) t3 using (date)
    LEFT JOIN (SELECT time::date as date,
                      count(distinct user_id) as paying_users
               FROM   user_actions
               WHERE  order_id not in (SELECT order_id
                                       FROM   user_actions
                                       WHERE  action = 'cancel_order')
               GROUP BY date) t4 using (date)
ORDER BY date
-- Сalculate all the same metrics, but for each day we will take into account the accumulated revenue and all currently available data on the number of users and orders.
-- Thus, we will get dynamic ARPU, ARPPU and AOV and we will be able to track how it has changed over time, taking into account the data we receive.
SELECT date,
       round(sum(revenue) OVER (ORDER BY date)::decimal / sum(new_users) OVER (ORDER BY date),
             2) as running_arpu,
       round(sum(revenue) OVER (ORDER BY date)::decimal / sum(new_paying_users) OVER (ORDER BY date),
             2) as running_arppu,
       round(sum(revenue) OVER (ORDER BY date)::decimal / sum(orders) OVER (ORDER BY date),
             2) as running_aov
FROM   (SELECT creation_time::date as date,
               count(distinct order_id) as orders,
               sum(price) as revenue
        FROM   (SELECT order_id,
                       creation_time,
                       unnest(product_ids) as product_id
                FROM   orders
                WHERE  order_id not in (SELECT order_id
                                        FROM   user_actions
                                        WHERE  action = 'cancel_order')) t1
            LEFT JOIN products using(product_id)
        GROUP BY date) t2
    LEFT JOIN (SELECT time::date as date,
                      count(distinct user_id) as users
               FROM   user_actions
               GROUP BY date) t3 using (date)
    LEFT JOIN (SELECT time::date as date,
                      count(distinct user_id) as paying_users
               FROM   user_actions
               WHERE  order_id not in (SELECT order_id
                                       FROM   user_actions
                                       WHERE  action = 'cancel_order')
               GROUP BY date) t4 using (date)
    LEFT JOIN (SELECT date,
                      count(user_id) as new_users
               FROM   (SELECT user_id,
                              min(time::date) as date
                       FROM   user_actions
                       GROUP BY user_id) t5
               GROUP BY date) t6 using (date)
    LEFT JOIN (SELECT date,
                      count(user_id) as new_paying_users
               FROM   (SELECT user_id,
                              min(time::date) as date
                       FROM   user_actions
                       WHERE  order_id not in (SELECT order_id
                                               FROM   user_actions
                                               WHERE  action = 'cancel_order')
                       GROUP BY user_id) t7
               GROUP BY date) t8 using (date)
-- Separately, we calculate the daily revenue from the orders of new users of our service.
-- Let's see what share it is in the total revenue from orders of all users - both new and old.
SELECT date,
       revenue,
       new_users_revenue,
       round(new_users_revenue / revenue * 100, 2) as new_users_revenue_share,
       100 - round(new_users_revenue / revenue * 100, 2) as old_users_revenue_share
FROM   (SELECT creation_time::date as date,
               sum(price) as revenue
        FROM   (SELECT order_id,
                       creation_time,
                       unnest(product_ids) as product_id
                FROM   orders
                WHERE  order_id not in (SELECT order_id
                                        FROM   user_actions
                                        WHERE  action = 'cancel_order')) t3
            LEFT JOIN products using (product_id)
        GROUP BY date) t1
    LEFT JOIN (SELECT start_date as date,
                      sum(revenue) as new_users_revenue
               FROM   (SELECT t5.user_id,
                              t5.start_date,
                              coalesce(t6.revenue, 0) as revenue
                       FROM   (SELECT user_id,
                                      min(time::date) as start_date
                               FROM   user_actions
                               GROUP BY user_id) t5
                           LEFT JOIN (SELECT user_id,
                                             date,
                                             sum(order_price) as revenue
                                      FROM   (SELECT user_id,
                                                     time::date as date,
                                                     order_id
                                              FROM   user_actions
                                              WHERE  order_id not in (SELECT order_id
                                                                      FROM   user_actions
                                                                      WHERE  action = 'cancel_order')) t7
                                          LEFT JOIN (SELECT order_id,
                                                            sum(price) as order_price
                                                     FROM   (SELECT order_id,
                                                                    unnest(product_ids) as product_id
                                                             FROM   orders
                                                             WHERE  order_id not in (SELECT order_id
                                                                                     FROM   user_actions
                                                                                     WHERE  action = 'cancel_order')) t9
                                                         LEFT JOIN products using (product_id)
                                                     GROUP BY order_id) t8 using (order_id)
                                      GROUP BY user_id, date) t6
                               ON t5.user_id = t6.user_id and
                                  t5.start_date = t6.date) t4
               GROUP BY start_date) t2 using (date)
-- Let's see which products are in the greatest demand and bring us the main income.
SELECT product_name,
       sum(revenue) as revenue,
       sum(share_in_revenue) as share_in_revenue
FROM   (SELECT case when round(100 * revenue / sum(revenue) OVER (), 2) >= 0.5 then name
                    else 'ДРУГОЕ' end as product_name,
               revenue,
               round(100 * revenue / sum(revenue) OVER (), 2) as share_in_revenue
        FROM   (SELECT name,
                       sum(price) as revenue
                FROM   (SELECT order_id,
                               unnest(product_ids) as product_id
                        FROM   orders
                        WHERE  order_id not in (SELECT order_id
                                                FROM   user_actions
                                                WHERE  action = 'cancel_order')) t1
                    LEFT JOIN products using(product_id)
                GROUP BY name) t2) t3
GROUP BY product_name
ORDER BY revenue desc
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
           
                         
