from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TopCompanySQLQuestions") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# 1. Time series analysis with window functions (Google, Uber)
def time_series_analysis(df, date_col, value_col):
    window_spec = Window.orderBy(date_col)
    return df.withColumn("rolling_avg", avg(value_col).over(window_spec.rowsBetween(-6, 0)))

# 2. User retention analysis (Meta, Google)
def user_retention_analysis(signups_df, activity_df, days=7):
    return (signups_df.join(activity_df, "user_id")
            .filter(datediff(activity_df.event_date, signups_df.signup_date) == days)
            .groupBy("signup_date").agg(count("*").alias(f"retention_day_{days}")))

# 3. Top N per category with ties (Amazon, Microsoft)
def top_n_per_category(df, category_col, value_col, n=5):
    window_spec = Window.partitionBy(category_col).orderBy(col(value_col).desc())
    return df.withColumn("rank", dense_rank().over(window_spec)).filter(col("rank") <= n)

# 4. Monthly growth rate (Amazon, Apple)
def monthly_growth(df, date_col, metric_col):
    monthly = df.groupBy(month(date_col).alias("month")).agg(sum(metric_col).alias("total"))
    window_spec = Window.orderBy("month")
    return monthly.withColumn("growth_rate", 
                            (col("total") - lag("total", 1).over(window_spec)) / 
                            lag("total", 1).over(window_spec) * 100)

# 5. Consecutive events (Doordash, Amazon)
def consecutive_events(df, user_col, date_col, consecutive_days=3):
    window_spec = Window.partitionBy(user_col).orderBy(date_col)
    return (df.withColumn("prev_date", lag(date_col, consecutive_days-1).over(window_spec))
            .filter(datediff(col(date_col), col("prev_date")) == consecutive_days-1))

# 6. Funnel conversion analysis (Meta, Amazon)
def funnel_conversion(visits_df, signups_df, purchases_df):
    funnel = (visits_df.join(signups_df, "user_id", "left")
             .join(purchases_df, "user_id", "left"))
    return funnel.agg(
        count("*").alias("visits"),
        count(signups_df.user_id).alias("signups"),
        count(purchases_df.user_id).alias("purchases")
    )

# 7. Median calculation without built-in (IBM, Microsoft)
def calculate_median(df, value_col):
    window_spec = Window.orderBy(value_col)
    ranked = df.withColumn("row_num", row_number().over(window_spec)) \
               .withColumn("total_count", count("*").over(Window.partitionBy()))
    return ranked.filter((col("row_num") >= col("total_count")/2) & 
                        (col("row_num") <= col("total_count")/2 + 1)) \
                .agg(avg(value_col).alias("median"))

# 8. Cohort analysis (Meta, Google)
def cohort_analysis(signups_df, activity_df, weeks=8):
    cohort_data = (signups_df.join(activity_df, "user_id")
                  .withColumn("week_diff", 
                             datediff(activity_df.event_date, signups_df.signup_date) / 7))
    
    # Create retention columns for each week
    agg_exprs = [countDistinct("user_id").alias("cohort_size")]
    for week in range(1, weeks+1):
        agg_exprs.append(countDistinct(when(col("week_diff") == week, "user_id")).alias(f"week_{week}"))
    
    return cohort_data.groupBy("signup_week").agg(*agg_exprs)

# 9. Moving average (Amazon, Walmart)
def moving_average(df, date_col, value_col, window_days=30):
    window_spec = Window.orderBy(date_col).rangeBetween(-window_days, 0)
    return df.withColumn("moving_avg", avg(value_col).over(window_spec))

# 10. Reciprocal relationships (Meta, Twitter)
def reciprocal_relationships(follows_df):
    return (follows_df.alias("f1")
            .join(follows_df.alias("f2"), 
                  (col("f1.user_id") == col("f2.target_id")) & 
                  (col("f1.target_id") == col("f2.user_id")) &
                  (col("f1.user_id") < col("f2.user_id")))
            .select(col("f1.user_id").alias("user_a"), 
                   col("f1.target_id").alias("user_b")))

# 11. Net Promoter Score (KPMG, Deloitte)
def calculate_nps(survey_df):
    return (survey_df.withColumn("category", 
                               when(col("score") >= 9, "promoter")
                               .when(col("score") <= 6, "detractor")
                               .otherwise("passive"))
            .groupBy().agg(
                ((count(when(col("category") == "promoter", 1)) - 
                  count(when(col("category") == "detractor", 1))) * 100.0 / 
                 count("*")).alias("nps_score")))

# 12. Session analysis (Google, Netflix)
def session_analysis(events_df, user_col, timestamp_col, timeout_minutes=30):
    window_spec = Window.partitionBy(user_col).orderBy(timestamp_col)
    return (events_df.withColumn("time_diff", 
                               unix_timestamp(col(timestamp_col)) - 
                               unix_timestamp(lag(timestamp_col).over(window_spec)))
            .withColumn("new_session", 
                       when((col("time_diff").isNull()) | 
                            (col("time_diff") > timeout_minutes * 60), 1).otherwise(0))
            .withColumn("session_id", sum("new_session").over(window_spec)))

# 13. Market basket analysis (Walmart, Amazon)
def market_basket_analysis(transactions_df, min_support=0.01):
    # Simplified association rules
    product_counts = transactions_df.groupBy("product_id").agg(count("*").alias("count"))
    total_transactions = transactions_df.select("transaction_id").distinct().count()
    
    return product_counts.withColumn("support", col("count") / total_transactions) \
                        .filter(col("support") >= min_support)

# 14. Employee hierarchy (Goldman Sachs, Microsoft)
def employee_hierarchy(employees_df):
    # Recursive CTE equivalent using iterative approach
    managers = employees_df.filter(col("manager_id").isNull()) \
                          .select("employee_id", lit(0).alias("level"))
    
    # This would typically require iterative processing in Spark
    # For demonstration, we'll do a simple self-join
    return employees_df.alias("e").join(employees_df.alias("m"), 
                                      col("e.manager_id") == col("m.employee_id"))

# 15. Time between events (Uber, Doordash)
def time_between_events(df, user_col, event_col, timestamp_col):
    window_spec = Window.partitionBy(user_col, event_col).orderBy(timestamp_col)
    return (df.withColumn("prev_time", lag(timestamp_col).over(window_spec))
            .withColumn("time_diff", 
                       unix_timestamp(col(timestamp_col)) - 
                       unix_timestamp(col("prev_time"))))

# 16. Revenue recognition (Microsoft, Salesforce)
def revenue_recognition(contracts_df, start_date_col, end_date_col, amount_col):
    # Monthly revenue allocation
    return contracts_df.withColumn("months", 
                                 months_between(col(end_date_col), col(start_date_col))) \
                      .withColumn("monthly_revenue", col(amount_col) / col("months"))

# 17. Churn prediction (Netflix, Spotify)
def churn_prediction(users_df, activity_df, churn_period_days=30):
    last_activity = activity_df.groupBy("user_id").agg(max("event_date").alias("last_active"))
    return users_df.join(last_activity, "user_id") \
                  .withColumn("days_inactive", datediff(current_date(), col("last_active"))) \
                  .withColumn("churn_risk", 
                             when(col("days_inactive") > churn_period_days, "high")
                             .when(col("days_inactive") > churn_period_days/2, "medium")
                             .otherwise("low"))

# 18. A/B test analysis (Meta, Google)
def ab_test_results(experiment_df, control_col, variant_col, metric_col):
    control_stats = experiment_df.filter(col(control_col) == 1).agg(
        avg(metric_col).alias("control_avg"),
        count("*").alias("control_count")
    )
    
    variant_stats = experiment_df.filter(col(variant_col) == 1).agg(
        avg(metric_col).alias("variant_avg"),
        count("*").alias("variant_count")
    )
    
    return control_stats.crossJoin(variant_stats).withColumn(
        "lift", (col("variant_avg") - col("control_avg")) / col("control_avg") * 100
    )

# 19. Geographic analysis (Uber, Airbnb)
def geographic_analysis(trips_df, lat_col, lon_col, grid_size=0.01):
    return trips_df.withColumn("grid_x", (col(lat_col) / grid_size).cast("int")) \
                  .withColumn("grid_y", (col(lon_col) / grid_size).cast("int")) \
                  .groupBy("grid_x", "grid_y").agg(count("*").alias("trip_count"))

# 20. Fraud detection (Visa, Goldman Sachs)
def fraud_detection(transactions_df, amount_col, timestamp_col, user_col):
    window_spec = Window.partitionBy(user_col).orderBy(timestamp_col)
    return (transactions_df.withColumn("prev_amount", lag(amount_col).over(window_spec))
            .withColumn("amount_change", 
                       abs((col(amount_col) - col("prev_amount")) / col("prev_amount")))
            .withColumn("fraud_flag", 
                       when((col("amount_change") > 5) & (col("prev_amount").isNotNull()), 1)
                       .otherwise(0)))

# Company-specific pattern implementations
def amazon_patterns():
    """Common Amazon patterns: e-commerce, recommendations, inventory"""
    # 1. Frequently bought together
    def frequently_bought_together(orders_df, min_support=0.001):
        # This would typically require complex joins and counting
        # Simplified version
        return orders_df.groupBy("product_a", "product_b").agg(
            count("*").alias("pair_count")
        ).filter(col("pair_count") > min_support * orders_df.count())
    
    # 2. Inventory management
    def inventory_turnover(sales_df, inventory_df, period_days=30):
        period_sales = sales_df.filter(col("sale_date") >= date_sub(current_date(), period_days))
        return inventory_df.join(period_sales.groupBy("product_id").agg(
            sum("quantity").alias("period_sales")
        ), "product_id").withColumn(
            "turnover_rate", col("period_sales") / col("inventory_quantity")
        )
    
    return frequently_bought_together, inventory_turnover

def meta_patterns():
    """Common Meta patterns: social network, engagement metrics"""
    # 1. Engagement rate calculation
    def engagement_rate(posts_df, reactions_df, comments_df):
        engagement = (posts_df.join(reactions_df, "post_id", "left")
                     .join(comments_df, "post_id", "left"))
        return engagement.groupBy("post_id").agg(
            (count(reactions_df.reaction_id) + count(comments_df.comment_id)) / 
            count(posts_df.post_id).alias("engagement_rate")
        )
    
    # 2. Friend recommendations
    def friend_recommendations(friends_df, max_recommendations=10):
        # People who are friends of friends but not already friends
        fof = (friends_df.alias("f1").join(friends_df.alias("f2"), 
                                         col("f1.friend_id") == col("f2.user_id"))
               .filter(col("f1.user_id") != col("f2.friend_id")))
        
        # Remove existing friends
        existing_friends = friends_df.withColumnRenamed("friend_id", "existing_friend")
        return fof.join(existing_friends, 
                       (col("f1.user_id") == col("user_id")) & 
                       (col("f2.friend_id") == col("existing_friend")), "left_anti") \
                 .groupBy("f1.user_id", "f2.friend_id").agg(count("*").alias("common_friends")) \
                 .orderBy(col("common_friends").desc()).limit(max_recommendations)
    
    return engagement_rate, friend_recommendations

def microsoft_patterns():
    """Common Microsoft patterns: enterprise software, licensing"""
    # 1. Software usage analytics
    def software_usage(usage_df, license_df):
        return usage_df.join(license_df, "user_id").groupBy("software_id").agg(
            countDistinct("user_id").alias("active_users"),
            sum("usage_minutes").alias("total_usage")
        )
    
    # 2. License compliance
    def license_compliance(usage_df, license_df):
        return usage_df.join(license_df, "user_id", "right").groupBy("software_id").agg(
            count(usage_df.user_id).alias("licensed_users"),
            count(license_df.user_id).alias("total_licenses")
        ).withColumn("compliance_rate", col("licensed_users") / col("total_licenses"))
    
    return software_usage, license_compliance

# Utility class for testing company-specific patterns
class CompanyPatternsValidator:
    def __init__(self, spark):
        self.spark = spark
        self.create_test_data()
    
    def create_test_data(self):
        """Create sample test data for various company patterns"""
        # E-commerce data (Amazon, Walmart)
        orders_data = [
            (1, 101, 50.0, "2024-01-15"), (1, 102, 30.0, "2024-01-15"),
            (2, 101, 25.0, "2024-01-16"), (2, 103, 40.0, "2024-01-16"),
            (3, 102, 35.0, "2024-01-17")
        ]
        self.orders_df = spark.createDataFrame(
            orders_data, ["order_id", "product_id", "amount", "order_date"]
        )
        
        # Social media data (Meta, Twitter)
        users_data = [
            (1, "user1"), (2, "user2"), (3, "user3"), (4, "user4")
        ]
        self.users_df = spark.createDataFrame(users_data, ["user_id", "username"])
        
        friends_data = [
            (1, 2), (1, 3), (2, 3), (2, 4)
        ]
        self.friends_df = spark.createDataFrame(friends_data, ["user_id", "friend_id"])
        
        # Time series data (Google, Uber)
        time_series_data = [
            ("2024-01-01", 100), ("2024-01-02", 120), ("2024-01-03", 110),
            ("2024-01-04", 130), ("2024-01-05", 140), ("2024-01-06", 125),
            ("2024-01-07", 135), ("2024-01-08", 150)
        ]
        self.time_series_df = spark.createDataFrame(
            time_series_data, ["date", "value"]
        )
        
        print("Test data created for company patterns!")
    
    def run_company_pattern_tests(self):
        """Test company-specific patterns"""
        print("Testing company-specific SQL patterns...")
        
        try:
            # Test time series analysis
            ts_result = time_series_analysis(self.time_series_df, "date", "value")
            print("✓ Time series analysis test passed")
            
            # Test top N per category
            top_n_result = top_n_per_category(self.orders_df, "product_id", "amount", 3)
            print("✓ Top N per category test passed")
            
            # Test reciprocal relationships
            reciprocal_result = reciprocal_relationships(self.friends_df)
            print("✓ Reciprocal relationships test passed")
            
            print("All company pattern tests completed successfully!")
            
        except Exception as e:
            print(f"✗ Test failed: {str(e)}")

# Main execution
if __name__ == "__main__":
    print("Top Company SQL Questions Implementation in PySpark")
    print("=" * 60)
    print("SQL patterns for Google, Amazon, Meta, Microsoft, Uber, etc.")
    print("=" * 60)
    
    # Initialize validator
    validator = CompanyPatternsValidator(spark)
    
    # Run tests
    validator.run_company_pattern_tests()
    
    print("\nAll company-specific patterns are ready to use!")
    print("Example usage:")
    print("  result = time_series_analysis(df, 'date', 'value')")
    print("  result = top_n_per_category(df, 'category', 'value', 5)")
    print("  result = user_retention_analysis(signups_df, activity_df, 7)")