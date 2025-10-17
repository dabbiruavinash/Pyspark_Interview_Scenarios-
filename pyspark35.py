1. Running Total and Moving Average of Reviews

%python
from pyspark.sql import Window
from pyspark.sql.functions import *

# Calculate running total and 3-month moving average of reviews by city
window_spec_city = Window.partitionBy("city").orderBy("review_date")
window_spec_3month = Window.partitionBy("city").orderBy("review_date").rowsBetween(-2, 0)

query_1 = df.withColumn("row_number", row_number().over(window_spec_city)) \
    .withColumn("running_total_reviews", sum("review_count").over(window_spec_city)) \
    .withColumn("moving_avg_reviews", avg("review_count").over(window_spec_3month)) \
    .withColumn("percent_of_city_total", 
                round((col("review_count") / sum("review_count").over(window_spec_city)) * 100, 2)) \
    .select("business_id", "business_name", "city", "review_date", "review_count", 
            "running_total_reviews", "moving_avg_reviews", "percent_of_city_total")

2. Rank Businesses Within Categories by Multiple Metrics

%python
# Rank businesses within their categories by stars and review count
window_spec_category_rank = Window.partitionBy("categories").orderBy(desc("stars"), desc("review_count"))

query_2 = df.withColumn("dense_rank_stars", dense_rank().over(window_spec_category_rank)) \
    .withColumn("rank_review_count", rank().over(
        Window.partitionBy("categories").orderBy(desc("review_count"))
    )) \
    .withColumn("ntile_group", ntile(4).over(window_spec_category_rank)) \
    .withColumn("prev_business_stars", lag("stars", 1).over(window_spec_category_rank)) \
    .withColumn("next_business_stars", lead("stars", 1).over(window_spec_category_rank)) \
    .filter(col("dense_rank_stars") <= 10)  # Top 10 in each category

3. YoY Growth and Percentage Change

%python
from pyspark.sql.functions import year, lag, when

# Calculate year-over-year growth for businesses
window_spec_business_annual = Window.partitionBy("business_id").orderBy(year("review_date"))

query_3 = df.withColumn("review_year", year("review_date")) \
    .groupBy("business_id", "business_name", "review_year") \
    .agg(sum("review_count").alias("annual_reviews")) \
    .withColumn("prev_year_reviews", lag("annual_reviews", 1).over(window_spec_business_annual)) \
    .withColumn("yoy_growth", 
                when(col("prev_year_reviews").isNull(), 0)
                .otherwise(round(((col("annual_reviews") - col("prev_year_reviews")) / 
                                col("prev_year_reviews")) * 100, 2))) \
    .withColumn("growth_trend", 
                when(col("yoy_growth") > 20, "High Growth")
                .when(col("yoy_growth") > 0, "Moderate Growth")
                .when(col("yoy_growth") == 0, "Stable")
                .otherwise("Declining")) \
    .orderBy(desc("yoy_growth"))

4. Cumulative Distribution and Percent Rank

%python
# Analyze business performance using statistical window functions
window_spec_state = Window.partitionBy("state").orderBy("stars")
window_spec_state_desc = Window.partitionBy("state").orderBy(desc("stars"))

query_4 = df.withColumn("percent_rank", percent_rank().over(window_spec_state)) \
    .withColumn("cume_dist", cume_dist().over(window_spec_state)) \
    .withColumn("state_avg_stars", avg("stars").over(Window.partitionBy("state"))) \
    .withColumn("state_median_stars", 
                percentile_approx("stars", 0.5).over(Window.partitionBy("state"))) \
    .withColumn("performance_quartile", 
                when(col("percent_rank") <= 0.25, "Bottom 25%")
                .when(col("percent_rank") <= 0.5, "25-50%")
                .when(col("percent_rank") <= 0.75, "50-75%")
                .otherwise("Top 25%")) \
    .filter(col("stars") >= 4.0)  # Only high-rated businesses

5. Gap Analysis Between Consecutive Reviews

%python
from pyspark.sql.functions import datediff, min as spark_min, max as spark_max

# Analyze gaps between reviews and identify inactive periods
window_spec_business_reviews = Window.partitionBy("business_id").orderBy("review_date")

query_5 = df.withColumn("prev_review_date", lag("review_date", 1).over(window_spec_business_reviews)) \
    .withColumn("days_between_reviews", 
                when(col("prev_review_date").isNotNull(), 
                     datediff(col("review_date"), col("prev_review_date")))
                .otherwise(0)) \
    .withColumn("avg_gap_business", 
                avg("days_between_reviews").over(Window.partitionBy("business_id"))) \
    .withColumn("max_gap_business", 
                max("days_between_reviews").over(Window.partitionBy("business_id"))) \
    .withColumn("gap_alert", 
                when(col("days_between_reviews") > 90, "Long Gap")
                .when(col("days_between_reviews") > 30, "Moderate Gap")
                .otherwise("Normal")) \
    .filter(col("days_between_reviews") > 0)  # Exclude first review

6. Rolling 30-Day Business Performance

%python
# Calculate rolling 30-day metrics for business performance
window_spec_30day = Window.partitionBy("business_id") \
    .orderBy("review_date") \
    .rangeBetween(-30, 0)

query_6 = df.withColumn("rolling_30day_reviews", sum("review_count").over(window_spec_30day)) \
    .withColumn("rolling_30day_avg_stars", avg("stars").over(window_spec_30day)) \
    .withColumn("min_stars_30day", min("stars").over(window_spec_30day)) \
    .withColumn("max_stars_30day", max("stars").over(window_spec_30day)) \
    .withColumn("trend_direction", 
                when(col("rolling_30day_avg_stars") > lag("rolling_30day_avg_stars", 1)
                     .over(Window.partitionBy("business_id").orderBy("review_date")), "Improving")
                .when(col("rolling_30day_avg_stars") < lag("rolling_30day_avg_stars", 1)
                     .over(Window.partitionBy("business_id").orderBy("review_date")), "Declining")
                .otherwise("Stable")) \
    .filter(col("rolling_30day_reviews") > 10)  # Only businesses with sufficient recent reviews

7. Category-wise Market Share Analysis

%python
# Analyze market share and dominance within categories and cities
window_spec_category_city = Window.partitionBy("categories", "city")
window_spec_city_total = Window.partitionBy("city")

query_7 = df.withColumn("category_city_total_reviews", sum("review_count").over(window_spec_category_city)) \
    .withColumn("city_total_reviews", sum("review_count").over(window_spec_city_total)) \
    .withColumn("market_share_in_category", 
                round((col("review_count") / col("category_city_total_reviews")) * 100, 2)) \
    .withColumn("category_share_in_city", 
                round((col("category_city_total_reviews") / col("city_total_reviews")) * 100, 2)) \
    .withColumn("category_rank_in_city", 
                rank().over(Window.partitionBy("city").orderBy(desc("category_city_total_reviews")))) \
    .withColumn("is_category_leader", 
                when(col("market_share_in_category") >= 20, "Category Leader")
                .when(col("market_share_in_category") >= 10, "Strong Player")
                .otherwise("Small Player")) \
    .filter(col("category_rank_in_city") <= 5)  # Top 5 categories in each city

8. First and Last Review Analysis

%python
# Analyze business lifecycle from first to last review
window_spec_business_lifecycle = Window.partitionBy("business_id")

query_8 = df.withColumn("first_review_date", min("review_date").over(window_spec_business_lifecycle)) \
    .withColumn("last_review_date", max("review_date").over(window_spec_business_lifecycle)) \
    .withColumn("business_lifetime_days", 
                datediff(col("last_review_date"), col("first_review_date"))) \
    .withColumn("first_review_stars", 
                first("stars").over(Window.partitionBy("business_id").orderBy("review_date"))) \
    .withColumn("last_review_stars", 
                last("stars").over(Window.partitionBy("business_id").orderBy("review_date"))) \
    .withColumn("rating_trend", 
                when(col("last_review_stars") > col("first_review_stars"), "Improving")
                .when(col("last_review_stars") < col("first_review_stars"), "Declining")
                .otherwise("Stable")) \
    .withColumn("reviews_per_day", 
                round(col("review_count") / col("business_lifetime_days"), 2)) \
    .filter(col("business_lifetime_days") > 365)  # Only established businesses

9. Advanced Statistical Analysis with Multiple Windows

%python
# Complex statistical analysis using multiple overlapping windows
window_spec_state_stats = Window.partitionBy("state")
window_spec_category_stats = Window.partitionBy("categories")
window_spec_business = Window.partitionBy("business_id").orderBy("review_date")

query_9 = df.withColumn("state_avg", avg("stars").over(window_spec_state_stats)) \
    .withColumn("state_stddev", stddev("stars").over(window_spec_state_stats)) \
    .withColumn("category_avg", avg("stars").over(window_spec_category_stats)) \
    .withColumn("z_score_state", round((col("stars") - col("state_avg")) / col("state_stddev"), 2)) \
    .withColumn("performance_vs_category", round(col("stars") - col("category_avg"), 2)) \
    .withColumn("rolling_volatility", 
                stddev("stars").over(window_spec_business.rowsBetween(-5, 0))) \
    .withColumn("volatility_category", 
                when(col("rolling_volatility").isNull(), "Insufficient Data")
                .when(col("rolling_volatility") > 1.0, "High Volatility")
                .when(col("rolling_volatility") > 0.5, "Moderate Volatility")
                .otherwise("Low Volatility")) \
    .withColumn("consistency_score", 
                when(col("rolling_volatility") <= 0.3, "Very Consistent")
                .when(col("rolling_volatility") <= 0.6, "Consistent")
                .when(col("rolling_volatility") <= 1.0, "Variable")
                .otherwise("Highly Variable")) \
    .filter(col("review_count") >= 50)  # Only businesses with substantial reviews

10. Hierarchical Ranking with Multiple Partitions

%python
# Multi-level hierarchical ranking across different dimensions
window_spec_state_city = Window.partitionBy("state", "city").orderBy(desc("stars"), desc("review_count"))
window_spec_state_category = Window.partitionBy("state", "categories").orderBy(desc("stars"))
window_spec_national = Window.orderBy(desc("stars"), desc("review_count"))

query_10 = df.withColumn("rank_in_city", rank().over(window_spec_state_city)) \
    .withColumn("dense_rank_in_category_state", dense_rank().over(window_spec_state_category)) \
    .withColumn("national_percentile", percent_rank().over(window_spec_national)) \
    .withColumn("city_leader", 
                when(col("rank_in_city") == 1, "City Leader")
                .when(col("rank_in_city") <= 3, "Top 3 in City")
                .when(col("rank_in_city") <= 10, "Top 10 in City")
                .otherwise("Other")) \
    .withColumn("category_leader_state", 
                when(col("dense_rank_in_category_state") == 1, "State Category Leader")
                .when(col("dense_rank_in_category_state") <= 3, "Top 3 in State Category")
                .otherwise("Other")) \
    .withColumn("elite_status", 
                when((col("national_percentile") >= 0.95) & (col("stars") >= 4.5), "Elite")
                .when((col("national_percentile") >= 0.85) & (col("stars") >= 4.0), "Premium")
                .otherwise("Standard")) \
    .filter(col("is_open") == 1)  # Only open businesses

