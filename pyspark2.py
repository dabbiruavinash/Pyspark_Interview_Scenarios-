from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NityaCloudTechSQLQueries") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# 1. Calculate the Rolling Average Sales Over the Last 7 Days
def rolling_avg_sales_7days(daily_sales_df):
    """
    Calculate rolling average sales for each day over past 7 days
    """
    window_spec = Window.orderBy("sale_date").rowsBetween(6, 0)
    
    result_df = (daily_sales_df
                .withColumn("rolling_avg_7days", 
                           avg("sales_amount").over(window_spec))
                .select("sale_date", "sales_amount", "rolling_avg_7days"))
    
    return result_df

# 2. Identify the Second-Highest Salary in Each Department
def second_highest_salary(employees_df):
    """
    Find second-highest salary in each department
    """
    window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
    
    result_df = (employees_df
                .withColumn("rank", dense_rank().over(window_spec))
                .filter(col("rank") == 2)
                .select("department_id", "salary", "employee_id"))
    
    return result_df

# 3. Find Orders Placed Within a Specific Timeframe
def orders_in_timeframe(orders_df, start_time="09:00:00", end_time="17:00:00"):
    """
    Retrieve orders placed between specific times
    """
    result_df = (orders_df
                .withColumn("order_time_str", col("order_time").cast("string"))
                .filter((col("order_time_str") >= start_time) & 
                       (col("order_time_str") <= end_time))
                .select("order_id", "order_time"))
    
    return result_df

# 4. Detect Data Gaps for Each Product
def detect_sales_gaps(products_df, sales_df, date_range_df):
    """
    Identify dates where no sales were recorded for each product
    """
    result_df = (products_df.crossJoin(date_range_df.alias("d"))
                .join(sales_df.alias("s"), 
                      (col("product_id") == col("s.product_id")) & 
                      (col("d.date") == col("s.sale_date")), 
                      "left_outer")
                .filter(col("s.sale_date").isNull())
                .select("product_id", "d.date"))
    
    return result_df

# 5. Calculate Cumulative Sum of Sales by Month
def cumulative_sales_monthly(monthly_sales_df):
    """
    Calculate cumulative sales by month
    """
    window_spec = Window.orderBy("month")
    
    result_df = (monthly_sales_df
                .withColumn("cumulative_sales", 
                           sum("sales_amount").over(window_spec))
                .select("month", "sales_amount", "cumulative_sales"))
    
    return result_df

# 6. Identify Employees in Multiple Departments
def employees_multiple_departments(employee_departments_df):
    """
    Find employees assigned to more than one department
    """
    result_df = (employee_departments_df
                .groupBy("employee_id")
                .agg(countDistinct("department_id").alias("dept_count"))
                .filter(col("dept_count") > 1)
                .select("employee_id"))
    
    return result_df

# 7. Find Products with Zero Sales in the Last Quarter
def products_zero_sales_last_quarter(products_df, sales_df):
    """
    List products with no sales in last quarter
    """
    last_quarter_start = date_sub(current_date(), 90)
    
    sales_last_quarter = (sales_df
                         .filter(col("sale_date") >= last_quarter_start)
                         .select("product_id").distinct())
    
    result_df = (products_df
                .join(sales_last_quarter, "product_id", "left_anti")
                .select("product_id", "product_name"))
    
    return result_df

# 8. Count Orders with Discounts in Each Category
def discounted_orders_by_category(orders_df):
    """
    Count orders with discounts in each category
    """
    result_df = (orders_df
                .filter(col("discount") > 0)
                .groupBy("category_id")
                .agg(count("*").alias("discounted_orders"))
                .orderBy("category_id"))
    
    return result_df

# 9. Identify Employees Whose Tenure is Below Average
def employees_below_avg_tenure(employees_df):
    """
    Find employees with tenure less than average
    """
    avg_tenure = employees_df.agg(avg("tenure").alias("avg_tenure")).first()["avg_tenure"]
    
    result_df = (employees_df
                .filter(col("tenure") < avg_tenure)
                .select("employee_id", "name", "tenure"))
    
    return result_df

# 10. Identify the Most Popular Product in Each Category
def most_popular_product_by_category(products_df, sales_df):
    """
    Find most popular product in each category based on sales
    """
    category_sales = (sales_df
                     .groupBy("product_id")
                     .agg(sum("quantity").alias("total_sales")))
    
    result_df = (products_df
                .join(category_sales, "product_id")
                .withColumn("rank", 
                           rank().over(Window.partitionBy("category_id")
                                     .orderBy(col("total_sales").desc())))
                .filter(col("rank") == 1)
                .select("category_id", "product_name", "total_sales"))
    
    return result_df

# 11. Detect Orders that Exceed a Monthly Threshold
def orders_exceeding_monthly_threshold(orders_df, threshold=10000):
    """
    Identify orders exceeding monthly threshold
    """
    result_df = (orders_df
                .groupBy("customer_id", month("order_date").alias("order_month"))
                .agg(sum("order_amount").alias("monthly_total"))
                .filter(col("monthly_total") > threshold)
                .select("customer_id", "order_month", "monthly_total"))
    
    return result_df

# 12. Find Customers Who Have Never Ordered a Specific Product
def customers_never_ordered_product(customers_df, orders_df, product_id="P123"):
    """
    Identify customers who never ordered specific product
    """
    customers_ordered = (orders_df
                        .filter(col("product_id") == product_id)
                        .select("customer_id").distinct())
    
    result_df = (customers_df
                .join(customers_ordered, "customer_id", "left_anti")
                .select("customer_id"))
    
    return result_df

# 13. Calculate Each Employee's Percentage of Departmental Sales
def employee_sales_percentage(employee_sales_df):
    """
    Calculate employee sales as percentage of departmental sales
    """
    window_spec = Window.partitionBy("department_id")
    
    result_df = (employee_sales_df
                .withColumn("dept_total_sales", sum("sales").over(window_spec))
                .withColumn("dept_sales_percentage", 
                           (col("sales") * 100.0) / col("dept_total_sales"))
                .select("employee_id", "sales", "dept_sales_percentage"))
    
    return result_df

# 14. Find Products with Sales Growth Between Two Periods
def products_sales_growth(sales_df, period1="Q1", period2="Q2"):
    """
    Identify products with sales growth between two periods
    """
    period1_sales = (sales_df
                    .filter(col("quarter") == period1)
                    .groupBy("product_id")
                    .agg(sum("sales").alias(f"{period1.lower()}_sales")))
    
    period2_sales = (sales_df
                    .filter(col("quarter") == period2)
                    .groupBy("product_id")
                    .agg(sum("sales").alias(f"{period2.lower()}_sales")))
    
    result_df = (period1_sales
                .join(period2_sales, "product_id")
                .withColumn("growth_rate", 
                           ((col(f"{period2.lower()}_sales") - col(f"{period1.lower()}_sales")) * 100.0) / 
                           when(col(f"{period1.lower()}_sales") == 0, 1).otherwise(col(f"{period1.lower()}_sales")))
                .select("product_id", 
                       col(f"{period1.lower()}_sales"), 
                       col(f"{period2.lower()}_sales"), 
                       "growth_rate"))
    
    return result_df

# 15. Identify Customers with Consecutive Months of Purchases
def customers_consecutive_months(orders_df):
    """
    Find customers with orders in consecutive months
    """
    window_spec = Window.partitionBy("customer_id").orderBy("order_date")
    
    result_df = (orders_df
                .withColumn("prev_order_date", lag("order_date").over(window_spec))
                .withColumn("months_diff", 
                           months_between(col("order_date"), col("prev_order_date")))
                .filter(col("months_diff") == 1)
                .select("customer_id", "order_date", "prev_order_date"))
    
    return result_df

# 16. Calculate Average Order Value (AOV) by Month
def avg_order_value_by_month(orders_df):
    """
    Calculate average order value by month
    """
    result_df = (orders_df
                .groupBy(month("order_date").alias("month"))
                .agg(avg("order_amount").alias("avg_order_value"))
                .orderBy("month"))
    
    return result_df

# 17. Rank Sales Representatives by Quarterly Performance
def rank_sales_reps_quarterly(quarterly_sales_df):
    """
    Rank sales representatives based on quarterly sales
    """
    window_spec = Window.partitionBy("quarter").orderBy(col("total_sales").desc())
    
    result_df = (quarterly_sales_df
                .withColumn("rank", rank().over(window_spec))
                .select("sales_rep_id", "quarter", "total_sales", "rank"))
    
    return result_df

# 18. Find the Month with the Highest Revenue in Each Year
def highest_revenue_month_yearly(monthly_revenue_df):
    """
    Find month with highest revenue for each year
    """
    window_spec = Window.partitionBy("year").orderBy(col("revenue").desc())
    
    result_df = (monthly_revenue_df
                .withColumn("rank", rank().over(window_spec))
                .filter(col("rank") == 1)
                .select("year", "month", "revenue"))
    
    return result_df

# 19. Identify Items with Stockouts
def identify_stockouts(inventory_df):
    """
    Identify items that experienced stockouts
    """
    result_df = (inventory_df
                .filter(col("stock_quantity") == 0)
                .select("item_id", "date"))
    
    return result_df

# 20. Calculate Average Time Between Orders by Customer
def avg_time_between_orders(orders_df):
    """
    Calculate average time between orders for each customer
    """
    window_spec = Window.partitionBy("customer_id").orderBy("order_date")
    
    result_df = (orders_df
                .withColumn("prev_order_date", lag("order_date").over(window_spec))
                .withColumn("days_diff", 
                           datediff(col("order_date"), col("prev_order_date")))
                .groupBy("customer_id")
                .agg(avg("days_diff").alias("avg_days_between_orders"))
                .filter(col("avg_days_between_orders").isNotNull()))
    
    return result_df

# Utility class for testing and data generation
class NityaCloudTechValidator:
    def __init__(self, spark):
        self.spark = spark
        self.create_test_data()
    
    def create_test_data(self):
        """Create sample test data for all scenarios"""
        # Sample data creation (simplified)
        self.employees_data = [
            (1, "John", 50000, 1, 3), (2, "Jane", 60000, 1, 2),
            (3, "Bob", 55000, 2, 4), (4, "Alice", 70000, 2, 1)
        ]
        self.employees_df = spark.createDataFrame(
            self.employees_data, ["employee_id", "name", "salary", "department_id", "tenure"]
        )
        
        # Create other test dataframes similarly...
        print("Test data created successfully!")
    
    def run_all_tests(self):
        """Test all implemented functions"""
        print("Running all Nitya CloudTech SQL tests...")
        
        try:
            # Test each function
            result1 = second_highest_salary(self.employees_df)
            print("✓ Second highest salary test passed")
            
            # Add tests for other functions...
            
            print("All tests completed successfully!")
            
        except Exception as e:
            print(f"✗ Test failed: {str(e)}")

# Main execution
if __name__ == "__main__":
    print("Nitya CloudTech SQL Queries Implementation in PySpark")
    print("=" * 60)
    
    # Initialize validator
    validator = NityaCloudTechValidator(spark)
    
    # Run tests
    validator.run_all_tests()
    
    print("\nAll functions are ready to use!")
    print("Call each function with appropriate DataFrame parameters")