--------------------------------------------------------------Exploring Dataset-------------------------------------------------------------


-- 1 - total items and amount for each invoice belong to customer

/*
this query select customerid and invoice from tablereatil table
then it calculates total items the customer purchased and the amount for that invoice
*/
select customer_id , invoice , sum(quantity) as total_items , sum(price*quantity) as amount
from tableretail
group by customer_id , invoice
order by customer_id;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 2 - top 10 customers by total amount 

/*
 the query calculates the total items purchased and the total amount spent for each invoice by each customer, organizing this data in a common table expression (CTE) named 'cte'.
Then, it selects customer IDs, counts the number of invoices for each customer, 
sums up their total items and total amount spent, and assigns a ranking to each customer based on their total spending. 
The 'dense_rank()' function is used to assign rankings without gaps, ordered by the total amount spent in descending order and all are in cte named 'ranking'
then it gets the top 10 only
*/
with cte as (
    select customer_id , invoice , sum(quantity) as total_items , sum(price*quantity) as amount
    from tableretail
    group by customer_id , invoice
    order by customer_id
),

ranking as (
select customer_id , count(invoice) as total_invoices , sum(total_items) total_items , sum (amount) total_amount , dense_rank() over(order by  sum (amount) desc) rank_by_amount
from cte
group by customer_id
)

select customer_id , total_invoices,total_items , total_amount , rank_by_amount
from ranking
where rank_by_amount <=10;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--3 - top 10 customers by total invoices 
/*
It first organizes the data by grouping transactions according to customer ID and invoice. 
Then, it calculates the total number of items purchased and the corresponding total amount spent for each invoice. 
Next, it counts the number of invoices associated with each customer and ranks them accordingly, using the 'dense_rank()' function. 
Finally, it selects the top 10 customers with the highest number of invoices
*/
with cte as (
    select customer_id , invoice , sum(quantity) as total_items , sum(price*quantity) as amount
    from tableretail
    group by customer_id , invoice
    order by customer_id
),

ranking as(
    select customer_id , count(invoice) as total_invoices , sum(total_items) total_items , sum (amount) total_amount , dense_rank() over(order by count(invoice) desc) rank_by_invoices 
    from cte
    group by customer_id
)

select customer_id , total_invoices,total_items , total_amount
from ranking
where rank_by_invoices <=10;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 4 - Top selling product per quantity
/*

This query selects all  stock codes and the total quantity sold for each stock code.

The SUM() function calculates the total quantity sold for each stock code,
and OVER() function partitions the data based on stock code.

Use DISTINCT to avoid redundancy, and ORDER BY total_quantity DESC to get the top sells

*/
select distinct stockcode , sum(quantity) over(partition by stockcode) as total_quantity
from tableretail
order by total_quantity desc;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 5 - top 10 products sold for each month
/*
for CTE 'cte':
-Calculates the total quantity sold for each stock code, partitioned by month.
-Formats the invoice date to extract the month and year.
-Removes duplicate stock codes to ensure each product is counted only once.

Ranking (CTE 'ranking'):
-Assigns a rank to each product based on the total quantity sold within each month, using the 'dense_rank()' function.
-This step ensures that products are ranked within each month separately.

Main Query:
-Selects the month, top-selling stock code, and total quantity sold for each month from the 'ranking' CTE.
-Filters the results to include only the top-ranked product (rank = 1) for each month.
*/
with cte as(
    select distinct stockcode , to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'mm-yyyy') as "month", sum(quantity) over(partition by stockcode , to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'mm-yyyy') ) total_quantity
    from tableretail
    order by stockcode
),

ranking as(
    select stockcode , "month" , total_quantity , dense_rank() over(partition by "month" order by total_quantity desc) "rank"
    from cte
)

select  "month" , stockcode , total_quantity
from ranking
where "rank" =1;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 6 - monthly amount for top 10 customers
/*
Data Preparation (CTE 'cte'):
-Groups data by customer ID and month.
-Calculates the total amount spent by each customer for each month.
-Also calculates the total amount spent by each customer overall.

Identifying Top Customers (CTE 'top10'):
-Ranks customers based on their total amount spent overall.
-Assigns a rank to each customer using the 'dense_rank()' function.

Main Query:
-Selects customer ID, month, monthly spending (amount_per_month), and total spending overall (total_amount) from the 'top10' CTE.
-Filters the results to include only the top 10 customers based on their assigned rank.
*/
with cte as (
    select distinct customer_id ,  to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'mm-yyyy') as "month" , sum(price*quantity) over(partition by customer_id , to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'mm-yyyy')) Amount_per_month ,  
    sum(price*quantity) over(partition by customer_id) total_amount
    from tableretail
),

top10 as (
    select cte.* , dense_rank () over(order by total_amount desc) "rank"
    from cte
)

select customer_id , "month" , amount_per_month , total_amount
from top10
where "rank" between 1 and 10;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 7 - for each month quantity sold and profit
/*
Data Preparation:
-Converts the 'invoicedate' field into a 'month' format (e.g., "mm-yyyy") using the TO_CHAR and TO_DATE functions.
-Partitions the data by year and month to calculate totals.

Calculations:
-Computes the total quantity sold for each month using the SUM function over the partitioned data.
-Calculates the total profit for each month by multiplying quantity sold by price and summing the results over the partitioned data.

Results:
-Retrieves distinct months along with the corresponding quantity sold and profit.
-Orders the results by month.
*/
select distinct  to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'mm-yyyy') as "month" , sum(quantity) over(partition by  to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'yyyy-mm')) as Quantity_Sold, 
    sum(quantity*price) over(partition by  to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'yyyy-mm')) as Profit
from tableretail
order by "month";

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 8 - top 10 products by monthly profit and total profit
/*
Data Preparation (CTE 'cte'):
-Groups data by product code and month.
-Calculates the monthly profit and total profit for each product.

Ranking Products (CTE 'top10products'):
-Ranks products based on their total profit in descending order.

Main Query:
-Selects the product code, month, monthly profit, and total profit from the 'top10products' CTE.
-Filters the results to include only the top 10 products based on their total profit rank.
-Orders the results by total profit and then by month.
*/
with cte as (
    select distinct stockcode , to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'mm-yyyy') as "month" , sum(quantity * price) over(partition by stockcode ,  to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'mm-yyyy')) as monthly_profit ,
    sum(quantity * price) over(partition by stockcode) as total_profit
    from tableretail
),

top10products as(
    select cte.* , dense_rank() over (order by total_profit desc) rnk
    from cte
)

select stockcode , "month" , monthly_profit , total_profit
from top10products
where rnk between 1 and 10
order by total_profit desc , "month";

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------RFM Model------------------------------------------------------------------------------------------


-- implement a Monetary model for customers behavior for product purchasing
with rfm as(
    select customer_id , round((select max(to_date(invoicedate , 'mm/dd/yyyy hh24:mi')) from tableretail ) - max(to_date(invoicedate , 'mm/dd/yyyy hh24:mi'))) as Recency,
            count(distinct invoice) as Frequency,
            sum(quantity * price) as Monetary
    from tableretail
    group by customer_id
),

rfm_score as(
    select customer_id , recency , frequency , monetary,
            ntile(5) over(order by recency desc) r_score,
            ntile(5) over(order by frequency) f_score,
            ntile(5) over(order by monetary) m_score
    from rfm
),


fm_score as (
    select customer_id , recency , frequency , monetary , r_score , ntile(5) over(order by (f_score+m_score)/2) fm_score
    from rfm_score
    group by customer_id , recency , frequency , monetary , r_score , f_score , m_score
)

select customer_id , recency , frequency , monetary , r_score , fm_score,
        CASE
            WHEN r_score = 5 AND fm_score = 5 THEN 'Champions'
            WHEN r_score = 5 AND fm_score = 4 THEN 'Champions'
            WHEN r_score = 5 AND fm_score = 5 THEN 'Champions'

            WHEN r_score = 5 AND fm_score = 2 THEN 'Potential Loyalists'
            WHEN r_score = 4 AND fm_score = 2 THEN 'Potential Loyalists'
            WHEN r_score = 3 AND fm_score = 3 THEN 'Potential Loyalists'
            WHEN r_score = 4 AND fm_score = 3 THEN 'Potential Loyalists'

            WHEN r_score = 5 AND fm_score = 3 THEN 'Loyal Customers'
            WHEN r_score = 4 AND fm_score = 4 THEN 'Loyal Customers'
            WHEN r_score = 3 AND fm_score = 5 THEN 'Loyal Customers'
            WHEN r_score = 3 AND fm_score = 4 THEN 'Loyal Customers'

            WHEN r_score = 5 AND fm_score = 1 THEN 'Recent Customers'

            WHEN r_score = 4 AND fm_score = 1 THEN 'Promising'
            WHEN r_score = 3 AND fm_score = 1 THEN 'Promising'

            WHEN r_score = 3 AND fm_score = 2 THEN 'Customers Needing Attention'
            WHEN r_score = 2 AND fm_score = 3 THEN 'Customers Needing Attention'
            WHEN r_score = 2 AND fm_score = 2 THEN 'Customers Needing Attention'

            WHEN r_score = 2 AND fm_score = 5 THEN 'At Risk'
            WHEN r_score = 2 AND fm_score = 4 THEN 'At Risk'
            WHEN r_score = 1 AND fm_score = 3 THEN 'At Risk'

            WHEN r_score = 1 AND fm_score = 5 THEN 'Cant Lose Them'
            WHEN r_score = 1 AND fm_score = 4 THEN 'Cant Lose Them'

            WHEN r_score = 1 AND fm_score = 2 THEN 'Hibernating'

            WHEN r_score = 1 AND fm_score = 1 THEN 'Lost'

            ELSE 'Uncategorized'
        END AS cust_segment
from fm_score
order by customer_id desc;s

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------Daily Transaction------------------------------------------------------------------------------------------

--What is the maximum number of consecutive days a customer made purchases? 
-- check if lag date is not before the current date
with cte as(
    select cust_id , calendar_dt , lag(calendar_dt) over(partition by cust_id order by calendar_dt) lag_cal , case
                                                when lag(calendar_dt) over(partition by cust_id order by calendar_dt)  + 1 = calendar_dt 
                                                then 0
                                                else 1
                                                end as check_cons_day
    from customers
),

--sum the check cons day partition by cust_id and using order by to use rows between unbounded preceding and current row
flagged_partition as (
    SELECT 
        cte.*,
        SUM(check_cons_day) OVER (PARTITION BY cust_id ORDER BY calendar_dt) AS flag_partition
    FROM cte
)

--counting partition by the flagpartition and then get max of that count
select distinct cust_id , max(temp_consecutive_days) over(partition by cust_id) max_consecutive_number
from(
    select flagged_partition.* , count(*) over(partition by cust_id , flag_partition) temp_consecutive_days
    from flagged_partition
);

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------days/transactions does it take a customer to reach a spent threshold of 250 L.E------------------------------------------------------------------------------------------

--On average, How many days/transactions does it take a customer to reach a spent threshold of 250 L.E?
/*
Common Table Expression (CTE 'cte'):
-Calculates the cumulative sum of transaction amounts (amt_le) for each customer (cust_id) over time (calendar_dt).
-Assigns a row number (rnk) to each transaction within each customer's transaction history, indicating the order in which transactions occur.
-Determines the first transaction date (first_dt) for each customer.

First Query:
-Selects distinct cust_id, calculating the number of transactions (num_transactions) and the number of days (num_days) from the first transaction to the current transaction.
-Filters transactions where the cumulative amount spent (sum_amt) is greater than or equal to 250.
-Orders the results by cust_id.

Second Query (for Average):
-Uses the results from the first query.
Calculates the average number of transactions (avg_num_transactions) and the average number of days (avg_num_days) for customers whose cumulative spending exceeds or equals 250.
*/

with cte as (
    select cust_id ,calendar_dt, amt_le ,sum(amt_le) over(partition by cust_id order by calendar_dt) as sum_amt , row_number() over(partition by cust_id order by calendar_dt) rnk , min(calendar_dt) over(partition by cust_id) first_dt
    from customers
)
/*
-- to get each num trasactions and num days for each customer use this query
select distinct cust_id , min(rnk)over(partition by cust_id) num_transactions, min(calendar_dt - first_dt)over(partition by cust_id) num_days
from cte
where sum_amt >= 250
order by cust_id asc;
*/
-- to get average only use this query

select avg(num_transactions) avg_num_transactions , avg(num_days) avg_num_days
from(
    select distinct cust_id , min(rnk)over(partition by cust_id) num_transactions, min(calendar_dt - first_dt)over(partition by cust_id) num_days
    from cte
    where sum_amt >= 250
    order by cust_id asc
);
