from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("AnalyticsQueries").getOrCreate()

# 1. Highest priced product per country
def highest_price_per_country(products_df, suppliers_df):
    joined_df = products_df.join(suppliers_df, "supplier_id")
    window_spec = Window.partitionBy("country").orderBy(col("price").desc())
    result = joined_df.withColumn("rn", row_number().over(window_spec)) \
                     .filter(col("rn") == 1) \
                     .select("product_name", "price", "country")
    return result

# 2. Total transaction amount per customer for current year
def current_year_transactions(customers_df, transactions_df):
    current_year = year(current_date())
    result = customers_df.join(transactions_df, "customer_id") \
                        .filter(year("transaction_date") == current_year) \
                        .groupBy("customer_name", "customer_id") \
                        .agg(sum("amount").alias("total_amt"))
    return result

# 3. Monthly transactions by country with approved amounts
def monthly_transactions_stats(transactions_df):
    result = transactions_df \
        .withColumn("month", date_format("trans_date", "yyyy-MM")) \
        .groupBy("month", "country") \
        .agg(
            count("*").alias("trans_count"),
            sum(when(col("state") == "approved", 1).otherwise(0)).alias("approved_count"),
            sum("amount").alias("trans_total_amount"),
            sum(when(col("state") == "approved", col("amount")).otherwise(0)).alias("approved_total_amount")
        )
    return result

# 4. Average star rating by product and month
def avg_rating_by_month(reviews_df):
    result = reviews_df \
        .withColumn("month", month("submit_date")) \
        .groupBy("month", "product_id") \
        .agg(round(avg("stars"), 2).alias("avg_rating")) \
        .orderBy("month", "product_id")
    return result

# 5. Users with purchases > $10,000 in last month
def high_value_users(purchases_df):
    last_month_start = date_add(trunc(current_date(), "MM"), -1)
    last_month_end = trunc(current_date(), "MM")
    
    result = purchases_df \
        .filter((col("date_of_purchase") >= last_month_start) & 
                (col("date_of_purchase") < last_month_end)) \
        .groupBy("user_id") \
        .agg(sum("amount_spent").alias("total_spent")) \
        .filter(col("total_spent") > 10000)
    return result

# 6. Average service duration by department
def avg_service_duration(employee_service_df):
    result = employee_service_df \
        .withColumn("end_date", coalesce(col("end_date"), current_date())) \
        .withColumn("service_days", datediff(col("end_date"), col("start_date"))) \
        .groupBy("department") \
        .agg(avg("service_days").alias("avg_duration_days")) \
        .orderBy("department")
    return result

# 7. Top 3 posts by engagement per user
def top_engagement_posts(fb_posts_df):
    window_spec = Window.partitionBy("user_id").orderBy(col("engagement_count").desc())
    
    result = fb_posts_df \
        .withColumn("engagement_count", col("likes") + col("comments")) \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") <= 3) \
        .select("user_id", "post_id", "engagement_count", col("rn").alias("rank"))
    return result

# 8. Users with >2 posts in past week and total likes
def active_users_past_week(posts_df):
    week_ago = date_sub(current_date(), 7)
    
    result = posts_df \
        .filter(col("post_date") >= week_ago) \
        .groupBy("user_id") \
        .agg(
            count("post_id").alias("cnt_post"),
            sum("likes").alias("total_likes")
        ) \
        .filter(col("cnt_post") > 2)
    return result

# 9. Companies with duplicate job postings
def duplicate_jobs(job_listings_df):
    result = job_listings_df \
        .groupBy("company_id", "title", "description") \
        .agg(count("*").alias("total_job")) \
        .filter(col("total_job") > 1) \
        .agg(count("*").alias("cnt_company"))
    return result

# 10. Region with lowest sales for previous month
def lowest_sales_region(sales_df):
    prev_month = add_months(trunc(current_date(), "MM"), -1)
    prev_month_year = year(prev_month)
    prev_month_num = month(prev_month)
    
    result = sales_df \
        .filter((year("saledate") == prev_month_year) & 
                (month("saledate") == prev_month_num)) \
        .groupBy("region") \
        .agg(sum("amount").alias("total_sales")) \
        .orderBy("total_sales") \
        .limit(1)
    return result

# 11. Median of views
def median_views(tittok_df):
    filtered_df = tittok_df.filter(col("views") < 900)
    
    asc_window = Window.orderBy("views")
    desc_window = Window.orderBy(col("views").desc())
    
    result = filtered_df \
        .withColumn("rn_asc", row_number().over(asc_window)) \
        .withColumn("rn_desc", row_number().over(desc_window)) \
        .filter(abs(col("rn_asc") - col("rn_desc")) <= 1) \
        .agg(avg("views").alias("median"))
    return result

# 12. Delayed orders per delivery partner
def delayed_orders(order_details_df):
    result = order_details_df \
        .filter(col("predicted_time") < col("delivery_time")) \
        .groupBy("del_partner") \
        .agg(count("order_id").alias("cnt_delayed_orders"))
    return result

# 13. City with most orders
def city_most_orders(restaurant_orders_df):
    result = restaurant_orders_df \
        .filter((col("city").isin(["Delhi", "Mumbai", "Bangalore", "Hyderabad"])) &
                (col("order_date").between("2025-07-31", "2025-08-30"))) \
        .groupBy("city") \
        .agg(count("order_id").alias("total_orders")) \
        .orderBy(col("total_orders").desc()) \
        .limit(1)
    return result

# 14. Count of distinct non-unique students
def distinct_non_unique_students(student_names_df):
    result = student_names_df \
        .groupBy("name") \
        .agg(count("*").alias("count")) \
        .filter(col("count") == 1) \
        .agg(count("*").alias("distinct_student_cnt"))
    return result

# 15. Pages with zero likes
def pages_zero_likes(pages_df, page_likes_df):
    result = pages_df \
        .join(page_likes_df, "page_id", "left_anti") \
        .select("page_id") \
        .orderBy("page_id")
    return result

# 16. Click-through rate for 2022
def ctr_2022(events_df):
    result = events_df \
        .filter(year("timestamp") == 2022) \
        .groupBy("app_id") \
        .agg(
            round(
                (sum(when(col("event_type") == "click", 1).otherwise(0)) * 100.0) / 
                count("*"), 2
            ).alias("ctr")
        )
    return result

# 17. Cities with customers having >3 orders in Nov 2023
def cities_high_orders(zomato_orders_df):
    result = zomato_orders_df \
        .filter((col("order_date").between("2023-11-01", "2023-11-30"))) \
        .groupBy("city", "customer_id") \
        .agg(count("*").alias("order_count")) \
        .filter(col("order_count") > 3) \
        .groupBy("city") \
        .agg(count("customer_id").alias("total_customer_count"))
    return result

# 18. Top 2 months by revenue per hotel per year
def top_months_hotel_revenue(hotel_revenue_df):
    window_spec = Window.partitionBy("hotel_id", "year").orderBy(col("revenue").desc())
    
    result = hotel_revenue_df \
        .withColumn("drn", dense_rank().over(window_spec)) \
        .filter(col("drn") <= 2) \
        .select("hotel_id", "year", "month", "revenue")
    return result

# 19. Employees with manager names
def employees_with_managers(employees_df):
    result = employees_df.alias("e1") \
        .join(employees_df.alias("e2"), 
              col("e1.manager_id") == col("e2.emp_id"), 
              "left") \
        .select(
            col("e1.emp_id"), 
            col("e1.emp_name"), 
            col("e2.emp_name").alias("manager_name")
        )
    return result

# 20. Salaries greater than average
def salaries_above_avg(employee_df):
    avg_salary = employee_df.agg(avg("salary")).collect()[0][0]
    result = employee_df.filter(col("salary") > avg_salary)
    return result

# 21. Duplicate email addresses
def duplicate_emails(customers_df):
    result = customers_df \
        .groupBy("email") \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1) \
        .select("email")
    return result

# 22. Running total revenue by date and product
def running_total_revenue(orders_df):
    window_spec = Window.partitionBy("product_id").orderBy("date")
    
    result = orders_df \
        .withColumn("running_total", 
                   sum("revenue").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))) \
        .select("date", "product_id", "product_name", "revenue", "running_total") \
        .orderBy("product_id", "date")
    return result

# 23. Top 5 customers by return percentage
def top_return_customers(orders_df, returns_df):
    orders_cte = orders_df.groupBy("customer_id") \
                         .agg(sum("total_items_ordered").alias("total_ordered"))
    
    returns_cte = returns_df.join(orders_df, "order_id") \
                           .groupBy("customer_id") \
                           .agg(sum("returned_items").alias("total_items_returned"))
    
    result = orders_cte.join(returns_cte, "customer_id") \
                      .withColumn("return_percentage", 
                                 round((col("total_items_returned") / 
                                        col("total_ordered")) * 100, 2)) \
                      .orderBy(col("return_percentage").desc()) \
                      .limit(5) \
                      .select("customer_id", "return_percentage")
    return result

# 24. Top 3 sellers with highest sales and lowest returns
def top_sellers(orders_df, returns_df):
    orders_cte = orders_df.groupBy("seller_id") \
                         .agg(sum("sale_amount").alias("total_sales"))
    
    returns_cte = returns_df.groupBy("seller_id") \
                           .agg(sum("return_quantity").alias("total_return_qty"))
    
    result = orders_cte.join(returns_cte, "seller_id", "left") \
                      .fillna(0, ["total_return_qty"]) \
                      .orderBy(col("total_sales").desc(), col("total_return_qty").asc()) \
                      .limit(3) \
                      .select("seller_id", "total_sales", "total_return_qty")
    return result

# 25. First year sales for each product
def first_year_sales(sales_df):
    window_spec = Window.partitionBy("product_id").orderBy("year")
    
    result = sales_df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .select("product_id", col("year").alias("first_year"), "quantity", "price")
    return result

# 26. Salary difference between Marketing and Engineering
def salary_difference(salaries_df):
    result = salaries_df \
        .groupBy() \
        .agg(
            (max(when(col("department") == "Marketing", col("salary"))) -
             max(when(col("department") == "Engineering", col("salary")))).alias("salary_diff")
        )
    return result

# 27. Average order amount by gender
def avg_order_by_gender(customers_df, orders_df):
    result = customers_df.join(orders_df, "customer_id") \
                        .groupBy("gender") \
                        .agg(round(avg("total_amount"), 2).alias("avg_spent"))
    return result

# 28. Monthly sales revenue for 2023
def monthly_revenue_2023(sales_df):
    result = sales_df \
        .filter(year("order_date") == 2023) \
        .withColumn("revenue", col("price_per_unit") * col("quantity")) \
        .groupBy(month("order_date").alias("month_num"), 
                 date_format("order_date", "MMMM").alias("month_name")) \
        .agg(sum("revenue").alias("revenue")) \
        .orderBy("month_num") \
        .select("month_name", "revenue")
    return result

# 29. Third transaction per user
def third_transaction(transactions_df):
    window_spec = Window.partitionBy("user_id").orderBy("transaction_date")
    
    result = transactions_df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 3) \
        .select("user_id", "spend", "transaction_date")
    return result

# 30. Top 5 products with revenue decrease
def revenue_decrease_products(product_revenue_df):
    window_spec = Window.partitionBy("product_name").orderBy("year")
    
    result = product_revenue_df \
        .withColumn("prev_year_revenue", lag("revenue").over(window_spec)) \
        .filter(col("prev_year_revenue").isNotNull()) \
        .withColumn("revenue_decreased", col("prev_year_revenue") - col("revenue")) \
        .withColumn("decreased_ratio_percentage", 
                   round(((col("prev_year_revenue") - col("revenue")) / 
                          col("prev_year_revenue")) * 100, 2)) \
        .filter(col("revenue") < col("prev_year_revenue")) \
        .orderBy(col("revenue_decreased").desc()) \
        .limit(5) \
        .select("product_name", "prev_year_revenue", "revenue", 
                "revenue_decreased", "decreased_ratio_percentage")
    return result

# 31. Laptop vs mobile viewership
def device_viewership(viewership_df):
    result = viewership_df \
        .groupBy() \
        .agg(
            sum(when(col("device_type") == "laptop", col("viewership_count"))
               .otherwise(0)).alias("laptop_views"),
            sum(when(col("device_type").isin(["tablet", "phone"]), col("viewership_count"))
               .otherwise(0)).alias("mobile_views")
        )
    return result

# 32. Top 2 products per category in 2022
def top_products_2022(product_spend_df):
    window_spec = Window.partitionBy("category").orderBy(col("total_spend").desc())
    
    result = product_spend_df \
        .filter(year("transaction_date") == 2022) \
        .groupBy("category", "product") \
        .agg(sum("spend").alias("total_spend")) \
        .withColumn("rn", rank().over(window_spec)) \
        .filter(col("rn") <= 2) \
        .select("category", "product", "total_spend")
    return result

# 33. Tweet histogram for 2022
def tweet_histogram_2022(tweets_df):
    result = tweets_df \
        .filter(year("tweet_date") == 2022) \
        .groupBy("user_id") \
        .agg(count("tweet_id").alias("num_post")) \
        .groupBy("num_post") \
        .agg(count("user_id").alias("num_user")) \
        .orderBy("num_post")
    return result

# 34. Top 3 salaries per department
def top_salaries_per_dept(employee_df, department_df):
    window_spec = Window.partitionBy("department_name").orderBy(col("salary").desc())
    
    result = employee_df.join(department_df, 
                            employee_df.departmentid == department_df.id) \
                       .withColumn("drn", dense_rank().over(window_spec)) \
                       .filter(col("drn") <= 3) \
                       .select("department_name", "name", "salary")
    return result

# 35. Top 10 most popular songs
def top_songs(songs_df, listens_df):
    window_spec = Window.orderBy(col("times_of_listens").desc())
    
    result = songs_df.join(listens_df, "song_id") \
                    .groupBy("song_name") \
                    .agg(count("listen_id").alias("times_of_listens")) \
                    .withColumn("rank", dense_rank().over(window_spec)) \
                    .filter(col("rank") <= 10) \
                    .select("song_name", "times_of_listens") \
                    .orderBy(col("times_of_listens").desc())
    return result

# Main execution function
def execute_all_queries():
    # Load all dataframes (example - you'll need to replace with actual data sources)
    # products_df = spark.read.table("products")
    # suppliers_df = spark.read.table("suppliers")
    # customers_df = spark.read.table("customers")
    # transactions_df = spark.read.table("transactions")
    # etc...
    
    # Execute all queries
    results = {}
    
    # results['q1'] = highest_price_per_country(products_df, suppliers_df)
    # results['q2'] = current_year_transactions(customers_df, transactions_df)
    # ... and so on for all queries
    
    return results

if __name__ == "__main__":
    results = execute_all_queries()
    # Display or save results as needed
    for query_name, result_df in results.items():
        print(f"Results for {query_name}:")
        result_df.show()
        print("\n" + "="*50 + "\n")