from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SQLHardInterviewQuestions") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# 1. 7-Day User Retention Rate
def calculate_7day_retention(signups_df, activity_df):
    """
    Calculate the percentage of users active 7 days after their signup
    """
    retention_df = (signups_df
                   .join(activity_df, 
                         (signups_df.user_id == activity_df.user_id) & 
                         (activity_df.event_date == expr("date_add(signup_date, 7)")),
                         "left")
                   .groupBy("signup_week")
                   .agg(
                       (countDistinct(when(col("activity_df.user_id").isNotNull(), signups_df.user_id)) * 100.0 / 
                        countDistinct(signups_df.user_id)).alias("retention_rate")
                   ))
    return retention_df

# 2. Top 5 Products per Category (with Ties)
def top_5_products_per_category(products_df):
    """
    Rank products within categories by sales, including ties
    """
    window_spec = Window.partitionBy("category").orderBy(col("sales").desc())
    
    ranked_products_df = (products_df
                         .withColumn("rank", dense_rank().over(window_spec))
                         .filter(col("rank") <= 5)
                         .select("category", "product_id", "sales", "rank"))
    
    return ranked_products_df

# 3. Month-over-Month Revenue Growth
def monthly_revenue_growth(orders_df):
    """
    Compute monthly revenue growth percentage, handling missing months
    """
    monthly_revenue_df = (orders_df
                         .groupBy(date_trunc("month", "order_date").alias("month"))
                         .agg(sum("revenue").alias("revenue"))
                         .orderBy("month"))
    
    window_spec = Window.orderBy("month")
    
    growth_df = (monthly_revenue_df
                .withColumn("prev_revenue", lag("revenue", 1).over(window_spec))
                .withColumn("growth", 
                           when(col("prev_revenue").isNotNull() & (col("prev_revenue") != 0),
                                (col("revenue") - col("prev_revenue")) * 100.0 / col("prev_revenue"))
                           .otherwise(None)))
    
    return growth_df

# 4. Consecutive Purchases/Logins
def consecutive_activity_users(activity_df, consecutive_days=3):
    """
    Find users with 3+ consecutive days of activity
    """
    window_spec = Window.partitionBy("user_id").orderBy("event_date")
    
    user_dates_df = (activity_df
                    .withColumn("prev_date_2", lag("event_date", 2).over(window_spec))
                    .withColumn("days_diff", 
                               when(col("prev_date_2").isNotNull(),
                                    datediff(col("event_date"), col("prev_date_2")))
                               .otherwise(None)))
    
    consecutive_users_df = (user_dates_df
                           .filter(col("days_diff") == (consecutive_days - 1))
                           .select("user_id")
                           .distinct())
    
    return consecutive_users_df

# 5. Funnel Conversion Rates
def funnel_conversion_rates(visits_df, signups_df, purchases_df):
    """
    Calculate drop-offs between stages (visit → signup → purchase)
    """
    stages_df = (visits_df
                .join(signups_df, "user_id", "left")
                .join(purchases_df, "user_id", "left")
                .withColumn("signed_up", when(signups_df.user_id.isNotNull(), True).otherwise(False))
                .withColumn("purchased", when(purchases_df.user_id.isNotNull(), True).otherwise(False)))
    
    conversion_rates_df = stages_df.agg(
        count("*").alias("visits"),
        sum(when(col("signed_up"), 1).otherwise(0)).alias("signups"),
        sum(when(col("purchased"), 1).otherwise(0)).alias("purchases"),
        (sum(when(col("signed_up"), 1).otherwise(0)) * 100.0 / count("*")).alias("visit_to_signup_rate"),
        (sum(when(col("purchased"), 1).otherwise(0)) * 100.0 / 
         sum(when(col("signed_up"), 1).otherwise(0))).alias("signup_to_purchase_rate")
    )
    
    return conversion_rates_df

# 6. Median Session Duration
def median_session_duration(sessions_df):
    """
    Compute median session duration without built-in functions
    """
    window_spec = Window.orderBy("duration")
    
    ordered_sessions_df = (sessions_df
                          .withColumn("row_num", row_number().over(window_spec))
                          .withColumn("total_count", count("*").over(Window.partitionBy())))
    
    median_df = (ordered_sessions_df
                .filter((col("row_num") >= col("total_count") / 2) & 
                       (col("row_num") <= col("total_count") / 2 + 1))
                .agg(avg("duration").alias("median_duration")))
    
    return median_df

# 7. Cohort Weekly Retention
def cohort_weekly_retention(signups_df, activity_df, weeks=8):
    """
    Track weekly retention for 8 weeks post-signup
    """
    cohort_data_df = (signups_df
                     .join(activity_df, "user_id", "left")
                     .withColumn("signup_week", date_trunc("week", "signup_date"))
                     .withColumn("event_week", date_trunc("week", "event_date"))
                     .withColumn("week_diff", 
                                when(col("event_week").isNotNull(),
                                     (datediff(col("event_week"), col("signup_week")) / 7).cast("int"))
                                .otherwise(None))))
    
    # Create dynamic columns for each week
    agg_exprs = [countDistinct("user_id").alias("cohort_size")]
    
    for week in range(1, weeks + 1):
        agg_exprs.append(
            countDistinct(when(col("week_diff") == week, "user_id")).alias(f"week_{week}")
        )
    
    retention_df = cohort_data_df.groupBy("signup_week").agg(*agg_exprs)
    
    return retention_df

# 8. 30-Day Moving Average of Sales
def moving_average_sales(daily_sales_df, window_days=30):
    """
    Calculate a rolling average of daily sales
    """
    window_spec = Window.orderBy("date").rangeBetween(-(window_days - 1), 0)
    
    moving_avg_df = (daily_sales_df
                    .withColumn("moving_avg", avg("sales").over(window_spec)))
    
    return moving_avg_df

# 9. Reciprocal User Follows
def reciprocal_follows(follows_df):
    """
    Identify mutual follows (A → B and B → A)
    """
    reciprocal_df = (follows_df.alias("f1")
                    .join(follows_df.alias("f2"),
                          (col("f1.user_id") == col("f2.target_id")) & 
                          (col("f1.target_id") == col("f2.user_id")) & 
                          (col("f1.user_id") < col("f2.user_id")))
                    .select(
                        col("f1.user_id").alias("user_a"),
                        col("f1.target_id").alias("user_b")
                    )
                    .distinct())
    
    return reciprocal_df

# 10. Net Promoter Score (NPS)
def calculate_nps(survey_df):
    """
    Compute NPS from survey scores (0-10)
    """
    nps_categories_df = (survey_df
                        .withColumn("category",
                                   when(col("score") >= 9, "promoter")
                                   .when(col("score") <= 6, "detractor")
                                   .otherwise("passive")))
    
    nps_df = (nps_categories_df
             .agg(
                 ((count(when(col("category") == "promoter", 1)) - 
                   count(when(col("category") == "detractor", 1))) * 100.0 / 
                  count("*")).alias("nps_score")
             ))
    
    return nps_df

# Example usage and data preparation
def main():
    # Sample data creation (replace with your actual data sources)
    # signups_df = spark.read.table("signups")
    # activity_df = spark.read.table("activity")
    # products_df = spark.read.table("products")
    # etc...
    
    print("All PySpark functions are ready to use!")
    print("Call each function with the appropriate DataFrame parameters")

if __name__ == "__main__":
    main()