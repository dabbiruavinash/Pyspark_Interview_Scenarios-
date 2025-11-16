# Customer Lifetime Value with Running Totals

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

customer_liftime_value = (orders.join(customer, "customer_id"))
.withColumn("order_year", F.year("order_date"))
.groupBy("customer_id", "first_name", "last_name", "order_year")
.agg(F.sum("total_amount").alias("yearly_spend"))
.withColumn("total_lifetime_value", F.sum("yearly_spend").over(Window.partitionBy("customer_id").orderBy("order_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
.withColumn("prev_year_spend", F.lag("yearly_spend").over(Window.partitionBy("customer_id").orderBy("order_year")))
.withColumn("growth_rate", (F.col("yearly_spend") - F.col("prev_year_spend"))/F.col("prev_year_spend") * 100))
customer_lifetime_value.show()

# Products with Best/Worst Profit Margins and Reviews

%python
product_analysis = (products.join(order_items, "product_id").join(customer_reviews, "product_id").groupBy("product_id", "product_name", "category", "price", "cost")
.agg(F.avg("rating").alias("avg_rating"),
          F.count('review_id").alias("review_count"),
          F.sum(F.col("quantity") * (F.col("unit_price") - F.col("cost"))).alias("total_profit"),
          F.avg(F.col("unit_price") - F.col("cost")).alias("avg_profit_margin"),
          F.sum("quantity").alias("total_quantity_sold"))
.withColumn("profit_margin_percentage",(F.col("avg_profit_margin")/F.col("price")) * 100)
.withColumn("performance_category", F.when((F.col("avg_rating") >= 4.5) & (F.col("profit_margin_percentage") > 30), "High Performance")
.when((F.col("avg_rating") <= 2.5) & (F.col("profit_margin_percentage") < 10), "Low Performer")
.otherwise("Average")))

product_analysis.show()

# Customer Segmentation using RFM Analysis

%python
from pyspark.sql.functions import datediff, current_date

rfm_analysis = (orders.join(customers, "customer_id").filter(F.col("status") == "Delivered").groupBy("customer_id", "first_name", "last_name", "city").agg(
F.max("order_date").alias("last_order_date"),
F.count("order_id").alias("frequency"),
F.sum("total_amount").alias("monetary"))
.withColumn("recency", datediff(current_date(), F.col("last_order_date")))
.withColumn("r_score", F.when(F.col("recency") <= 30, 5)
                 .when(F.col("recency") <= 60, 4)
                 .when(F.col("recency") <= 90, 3)
                 .when(F.col("recency") <= 180, 2)
                 .otherwise(1))
.withColumn("f_score",
                F.when(F.col("frequency") >= 20, 5)
                 .when(F.col("frequency") >= 10, 4)
                 .when(F.col("frequency") >= 5, 3)
                 .when(F.col("frequency") >= 2, 2)
                 .otherwise(1))
.withColumn("m_score",
                F.when(F.col("monetary") >= 5000, 5)
                 .when(F.col("monetary") >= 2000, 4)
                 .when(F.col("monetary") >= 1000, 3)
                 .when(F.col("monetary") >= 500, 2)
                 .otherwise(1))
.withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
.withColumn("customer_segment",
                F.when(F.col("rfm_score") >= 12, "Champion")
                 .when(F.col("rfm_score") >= 9, "Loyal")
                 .when(F.col("rfm_score") >= 6, "Potential")
                 .when(F.col("rfm_score") >= 4, "At Risk")
                 .otherwise("Lost")))

rfm_analysis.show()

# Monthly Sales Growth with Moving Averages

%python
monthly_sales = ( orders.join(order_items, "order_id")
.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
.groupBy("order_month")
.agg(
F.sum("total_amount").alias("monthly_sales"),
F.countDistinct("customer_id").alias("unique_customers"),
F.sum("quantity").alias("total_units"))
.orderBy("order_month")
.withColumn("prev_month_sales", F.lag("monthly_sales").over(Window.orderBy("order_month")))
.withColumn("sales_growth", F.col("monthly_sales") - F.col("prev_month_sales"))/F.col("prev_month_sales") * 100)
.withColumn("three_month_ma", F.avg("monthly_sales").over(Window.orderBy("order_month").rowBetween(2,0)))
.withColumn("six_month_ma", F.avg("monthly_sales").over(Window.orderBy("order_month").rowsBetween(5,0))))
monthly_sales.show()

# Products Frequently Bought Together (Market Basket Analysis)

%python
product_pairs = (order_items.alias("oil").join(order_items.alias("oi2"), "order_id")
.filter(F.col("oi1.product_id") < F.col("oi2.product_id"))
.groupBy("oi1.product_id", "oi2.product_id")
.agg(F.count("order_id").alias("pair_count"))
.join(products.alias("p1"), F.col("oi1.product_id") == F.col("p1.product_id"))
.join(products.alias("p2"), F.col("oi2.product_id") == F.col("p2.product_id"))
.select(F.col("p1.product_name").alias("product_1"),
        F.col("p2.product_name").alias("product_2"),
        "pair_count",
        F.col("p1.category").alias("category_1"),
        F.col("p2.category").alias("category_2")).orderBy(F.desc("pair_count")))

product_pairs.show()

# Customer Churn Prediction Analysis

%python
churn_analysis = (customers.join(orders, "customer_id", "left").groupBy("customer_id", "first_name", "last_name", "registration_date", "city")
.agg(
F.max("order_date").alias("last_order_date"),
F.count("order_id").alias("total_orders"),
F.sum("total_amount").alias("total_spend"),
F.avg("total_amount").alias("avg_order_value"))
.withColumn("days_since_last_order", F.datediff(F.current_date(), F.col("last_order_date")))
.withColumn("days_since_registration", F.datediff(F.current_date(), F.col("registration_date")))
.withColumn("order_frequency", F.col("days_since_registration") / F.greatest(F.col("total_orders"), 1))
.withColumn("churn_risk", F.when((F.col("days_since_last_order") > 90) & (F.col("total_orders") > 0), "High")
                 .when((F.col("days_since_last_order") > 60) & (F.col("total_orders") > 0), "Medium")
                 .when(F.col("total_orders") == 0, "New/Inactive")
                 .otherwise("Low"))
.withColumn("customer_value_tier", F.when(F.col("total_spent") >= 5000, "Platinum")
                 .when(F.col("total_spent") >= 2000, "Gold")
                 .when(F.col("total_spent") >= 500, "Silver")
                 .otherwise("Bronze")))

churn_analysis.show()

# Hierarchical Supplier Performance Analysis

%python
supplier_performance = (
    suppliers.join(products, "supplier_id")
    .join(order_items, "product_id")
    .groupBy("supplier_id", "supplier_name", "country")
    .agg(
        F.countDistinct("product_id").alias("products_supplied"),
        F.sum(F.col("oi.quantity") * (F.col("oi.unit_price") - F.col("p.cost"))).alias("total_profit_generated"),
        F.avg("reliability_score").alias("avg_reliability"),
        F.sum("quantity").alias("total_units_sold"),
        F.avg(F.col("oi.unit_price") - F.col("p.cost")).alias("avg_profit_margin")
    )
    .withColumn("profit_per_product", 
                F.col("total_profit_generated") / F.col("products_supplied"))
    .withColumn("performance_score",
                (F.col("total_profit_generated") * 0.4 + 
                 F.col("avg_reliability") * 100 * 0.3 +
                 F.col("profit_per_product") * 0.3))
    .withColumn("supplier_tier",
                F.when(F.col("performance_score") >= F.percentile_approx("performance_score", 0.8), "A")
                 .when(F.col("performance_score") >= F.percentile_approx("performance_score", 0.6), "B")
                 .when(F.col("performance_score") >= F.percentile_approx("performance_score", 0.4), "C")
                 .when(F.col("performance_score") >= F.percentile_approx("performance_score", 0.2), "D")
                 .otherwise("E")))

supplier_performance.show()

#  Time-based Cohort Analysis

%python
cohort_analysis = (customers.join(orders, "customer_id")
.withColumn("cohort_month", F.date_format("registration_date", "yyyy-MM"))
.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
.withColumn("cohort_index", F.month_between(F.to_date("order_month", "yyyy-MM"), F.to_date('cohort_month", "yyyy-MM")))
.filter(F.col("cohort_index") >= 0)
.groupBy("cohort_month", "cohort_index")
.agg(F.countDistinct("customer_id").alias("customers"))
.groupBy("cohort_month")
.pivot("cohort_index")
.agg(F.first("customers"))
.orderBy("cohort_month"))

--- calculate retention rates
cohort_retention = (cohort_analysis.select([F.col("cohort_month")] + [F.col(str(i)).isNull(), 0).otherwise(F.round(F.col(str(i)) / F.col("0") * 100, 2)).alias(f"month_{i}") for i in range(0, 13)]))
cohort_retention.show()

# Advanced Product Recommendation Engine

%python
# User-based collaborative filtering
user_product_ratings = (
    orders.join(order_items, "order_id")
    .join(customer_reviews, ["customer_id", "product_id"], "left")
    .groupBy("customer_id", "product_id")
    .agg(
        F.coalesce(F.avg("rating"), F.lit(3.0)).alias("implicit_rating"),
        F.count("order_id").alias("purchase_count"),
        F.sum("quantity").alias("total_quantity")
    )
    .withColumn("weighted_rating", 
                F.col("implicit_rating") * F.log1p(F.col("purchase_count")))
)

# Calculate similarity between products
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

# Pivot data for similarity calculation
product_user_matrix = (
    user_product_ratings
    .groupBy("product_id")
    .pivot("customer_id")
    .agg(F.first("weighted_rating"))
    .fillna(0)
)

# For large datasets, use approximate similarity
similar_products = (
    user_product_ratings.alias("u1")
    .join(user_product_ratings.alias("u2"), "customer_id")
    .filter(F.col("u1.product_id") != F.col("u2.product_id"))
    .groupBy("u1.product_id", "u2.product_id")
    .agg(
        F.sum(F.col("u1.weighted_rating") * F.col("u2.weighted_rating")).alias("dot_product"),
        F.sqrt(F.sum(F.col("u1.weighted_rating") * F.col("u1.weighted_rating"))).alias("norm1"),
        F.sqrt(F.sum(F.col("u2.weighted_rating") * F.col("u2.weighted_rating"))).alias("norm2")
    )
    .withColumn("cosine_similarity", 
                F.when((F.col("norm1") * F.col("norm2")) > 0, 
                       F.col("dot_product") / (F.col("norm1") * F.col("norm2")))
                 .otherwise(0))
    .join(products.alias("p1"), F.col("u1.product_id") == F.col("p1.product_id"))
    .join(products.alias("p2"), F.col("u2.product_id") == F.col("p2.product_id"))
    .select(
        F.col("p1.product_name").alias("product"),
        F.col("p2.product_name").alias("similar_product"),
        F.col("cosine_similarity")).orderBy(F.desc("cosine_similarity")))

similar_products.show()

# Geographic Sales Analysis with Advanced Analytics

%python
from pyspark.sql.functions import expr

geographic_analysis = (
    orders.join(customers, "customer_id")
    .join(order_items, "order_id")
    .join(products, "product_id")
    .groupBy("country", "city", "category")
    .agg(
        F.sum("total_amount").alias("total_sales"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("units_sold"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum(F.col("quantity") * (F.col("unit_price") - F.col("cost"))).alias("total_profit"))
    .withColumn("sales_per_customer", F.col("total_sales") / F.col("unique_customers"))
    .withColumn("profit_margin", (F.col("total_profit") / F.col("total_sales")) * 100))

# Calculate geographic concentration
geo_concentration = (
    geographic_analysis
    .groupBy("country")
    .agg(
        F.sum("total_sales").alias("country_sales"),
        F.countDistinct("city").alias("cities_count"),
        F.sum("unique_customers").alias("total_customers"))
    .withColumn("sales_concentration", F.expr("PERCENT_RANK() OVER (ORDER BY country_sales DESC)"))
    .withColumn("customer_concentration",F.expr("PERCENT_RANK() OVER (ORDER BY total_customers DESC)")))

# Top performing cities by category
top_cities_by_category = (
    geographic_analysis
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("country", "category")
        .orderBy(F.desc("total_sales"))))
    .filter(F.col("rank") <= 3)
    .select("country", "city", "category", "total_sales", "rank"))

top_cities_by_category.show()

# Customer Journey Analysis

%python
customer_journey = (
    orders.join(customers, "customer_id")
    .withColumn("order_sequence", F.row_number().over(
        Window.partitionBy("customer_id").orderBy("order_date")
    ))
    .withColumn("days_since_previous", 
                F.datediff("order_date", 
                          F.lag("order_date").over(
                              Window.partitionBy("customer_id").orderBy("order_date")
                          )))
    .withColumn("order_amount_growth", 
                (F.col("total_amount") - 
                 F.lag("total_amount").over(
                     Window.partitionBy("customer_id").orderBy("order_date")
                 )) / F.lag("total_amount").over(
                     Window.partitionBy("customer_id").orderBy("order_date")
                 ) * 100)
    .withColumn("is_first_order", F.col("order_sequence") == 1)
    .withColumn("is_repeat_customer", F.col("order_sequence") > 1)
    .withColumn("customer_tenure", 
                F.datediff(F.max("order_date").over(
                    Window.partitionBy("customer_id")
                ), F.min("order_date").over(
                    Window.partitionBy("customer_id")
                )))
)

# Calculate customer journey metrics
journey_metrics = (
    customer_journey
    .groupBy("customer_id", "first_name", "last_name")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.count("order_id").alias("total_orders"),
        F.avg("days_since_previous").alias("avg_days_between_orders"),
        F.stddev("days_since_previous").alias("stddev_days_between_orders"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum("total_amount").alias("lifetime_value"),
        F.max("order_sequence").alias("max_order_sequence")
    )
    .withColumn("customer_tenure_days", 
                F.datediff("last_order_date", "first_order_date"))
    .withColumn("order_frequency", 
                F.col("customer_tenure_days") / F.greatest(F.col("total_orders") - 1, 1))
    .withColumn("customer_stage",
                F.when(F.col("total_orders") == 1, "New")
                 .when(F.col("total_orders") <= 3, "Developing")
                 .when(F.col("total_orders") <= 10, "Established")
                 .otherwise("Loyal")))

journey_metrics.show()

# Advanced Inventory Analysis

%python
inventory_analysis = (
    products.join(order_items, "product_id", "left")
    .join(suppliers, "supplier_id")
    .groupBy("product_id", "product_name", "category", "supplier_name")
    .agg(
        F.sum("quantity").alias("total_sold"),
        F.avg("unit_price").alias("avg_selling_price"),
        F.first("price").alias("current_price"),
        F.first("cost").alias("unit_cost"),
        F.countDistinct("order_id").alias("orders_count"),
        F.avg("rating").alias("avg_rating")
    )
    .withColumn("total_revenue", F.col("total_sold") * F.col("avg_selling_price"))
    .withColumn("total_cost", F.col("total_sold") * F.col("unit_cost"))
    .withColumn("total_profit", F.col("total_revenue") - F.col("total_cost"))
    .withColumn("profit_margin", F.when(F.col("total_revenue") > 0, 
                       F.col("total_profit") / F.col("total_revenue") * 100).otherwise(0))
    .withColumn("turnover_rate", F.col("total_sold") / F.greatest(F.col("orders_count"), 1)))

# ABC Analysis (Pareto Analysis)
abc_analysis = (
    inventory_analysis
    .withColumn("cumulative_revenue", 
                F.sum("total_revenue").over(Window.orderBy(F.desc("total_revenue"))))
    .withColumn("total_revenue_sum", F.sum("total_revenue").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
    .withColumn("revenue_percentage", F.col("cumulative_revenue") / F.col("total_revenue_sum") * 100)
    .withColumn("abc_class",
                F.when(F.col("revenue_percentage") <= 80, "A")
                 .when(F.col("revenue_percentage") <= 95, "B")
                 .otherwise("C")).select("product_id", "product_name", "category", "total_revenue", "revenue_percentage", "abc_class"))

abc_analysis.show()

# Customer Behavior Clustering Preparation

%python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# Prepare features for clustering
customer_features = (
    customers.join(orders, "customer_id", "left")
    .join(order_items, "order_id", "left")
    .groupBy("customer_id", "first_name", "last_name", "city", "country", "registration_date")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_spent"),
        F.avg("total_amount").alias("avg_order_value"),
        F.datediff(F.current_date(), F.max("order_date")).alias("days_since_last_order"),
        F.datediff(F.current_date(), F.min("order_date")).alias("days_since_first_order"),
        F.sum("quantity").alias("total_items_purchased"),
        F.avg(F.col("unit_price") - F.col("cost")).alias("avg_profit_margin"),
        F.countDistinct("product_id").alias("unique_products")
    )
    .fillna(0)
    .withColumn("order_frequency", 
                F.when(F.col("days_since_first_order") > 0, 
                       F.col("order_count") / F.col("days_since_first_order") * 30)
                 .otherwise(0))
    .withColumn("avg_items_per_order", 
                F.col("total_items_purchased") / F.greatest(F.col("order_count"), 1))
)

# Feature vector assembly
feature_columns = ["order_count", "total_spent", "avg_order_value", "days_since_last_order", 
                   "total_items_purchased", "avg_profit_margin", "unique_products", "order_frequency"]

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

customer_features_vector = assembler.transform(customer_features)
scaler_model = scaler.fit(customer_features_vector)
customer_features_scaled = scaler_model.transform(customer_features_vector)

# K-means clustering
kmeans = KMeans(featuresCol="scaled_features", k=5, seed=42)
model = kmeans.fit(customer_features_scaled)

clustered_customers = model.transform(customer_features_scaled)

# Analyze clusters
cluster_analysis = (
    clustered_customers
    .groupBy("prediction")
    .agg(
        F.count("customer_id").alias("customer_count"),
        F.avg("order_count").alias("avg_orders"),
        F.avg("total_spent").alias("avg_spent"),
        F.avg("days_since_last_order").alias("avg_recency"),
        F.avg("order_frequency").alias("avg_frequency")).orderBy("prediction"))

cluster_analysis.show()

# Advanced Time Series Forecasting Preparation

%python
from pyspark.sql.functions import date_add, last_day

# Create comprehensive time series data
time_series_data = (
    orders.join(order_items, "order_id")
    .join(products, "product_id")
    .withColumn("order_date", F.date_trunc("day", "order_date"))
    .groupBy("order_date", "category")
    .agg(
        F.sum("total_amount").alias("daily_sales"),
        F.sum("quantity").alias("daily_units"),
        F.countDistinct("customer_id").alias("daily_customers"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum(F.col("quantity") * (F.col("unit_price") - F.col("cost"))).alias("daily_profit")
    )
    .withColumn("day_of_week", F.dayofweek("order_date"))
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
    .withColumn("month", F.month("order_date"))
    .withColumn("year", F.year("order_date"))
    .withColumn("quarter", F.quarter("order_date"))
)

# Create lag features for time series analysis
lagged_features = (
    time_series_data
    .withColumn("prev_day_sales", F.lag("daily_sales").over(
        Window.partitionBy("category").orderBy("order_date")
    ))
    .withColumn("prev_week_sales", F.lag("daily_sales", 7).over(
        Window.partitionBy("category").orderBy("order_date")
    ))
    .withColumn("rolling_7d_avg", F.avg("daily_sales").over(
        Window.partitionBy("category")
        .orderBy("order_date")
        .rowsBetween(6, 0)
    ))
    .withColumn("rolling_30d_avg", F.avg("daily_sales").over(
        Window.partitionBy("category")
        .orderBy("order_date")
        .rowsBetween(29, 0)
    ))
    .withColumn("sales_growth", 
                (F.col("daily_sales") - F.col("prev_day_sales")) / F.col("prev_day_sales") * 100)
    .withColumn("yoy_growth", 
                (F.col("daily_sales") - F.lag("daily_sales", 365).over(
                    Window.partitionBy("category").orderBy("order_date")
                )) / F.lag("daily_sales", 365).over(
                    Window.partitionBy("category").orderBy("order_date")
                ) * 100)
)

# Calculate seasonality components
seasonality_analysis = (
    lagged_features
    .filter(F.col("order_date") >= F.date_add(F.current_date(), -730))  # Last 2 years
    .groupBy("category", "month", "day_of_week")
    .agg(
        F.avg("daily_sales").alias("avg_sales_seasonal"),
        F.stddev("daily_sales").alias("std_sales_seasonal"),
        F.count("order_date").alias("data_points"))
    .withColumn("seasonal_index", F.col("avg_sales_seasonal") / F.avg("avg_sales_seasonal").over(Window.partitionBy("category"))))

seasonality_analysis.show()

# Multi-dimensional Customer Value Scoring

%python
customer_value_scoring = (
    customers.join(orders, "customer_id", "left")
    .join(order_items, "order_id", "left")
    .join(products, "product_id", "left")
    .groupBy("customer_id", "first_name", "last_name", "city", "country", "registration_date")
    .agg(
        # Monetary metrics
        F.sum("total_amount").alias("total_spent"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum(F.col("quantity") * (F.col("unit_price") - F.col("cost"))).alias("total_profit_contributed"),
        
        # Frequency metrics
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("product_id").alias("unique_products"),
        
        # Recency metrics
        F.datediff(F.current_date(), F.max("order_date")).alias("days_since_last_order"),
        F.datediff(F.current_date(), F.min("order_date")).alias("days_since_first_order"),
        
        # Behavioral metrics
        F.avg("rating").alias("avg_rating_given"),
        F.sum("helpful_votes").alias("total_helpful_votes")
    )
    .fillna(0)
    .withColumn("customer_tenure", F.col("days_since_first_order"))
    .withColumn("order_frequency", F.col("order_count") / F.greatest(F.col("customer_tenure") / 30.0, 1))
    .withColumn("avg_profit_per_order", F.col("total_profit_contributed") / F.greatest(F.col("order_count"), 1))
    
    # Calculate z-scores for normalization
    .withColumn("monetary_z", 
                (F.col("total_spent") - F.avg("total_spent").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) 
                / F.stddev("total_spent").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
    .withColumn("frequency_z", 
                (F.col("order_frequency") - F.avg("order_frequency").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) 
                / F.stddev("order_frequency").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
    .withColumn("recency_z", 
                (-F.col("days_since_last_order") - F.avg(-F.col("days_since_last_order")).over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) 
                / F.stddev(-F.col("days_since_last_order")).over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
    
    # Calculate composite score
    .withColumn("composite_score", 
                F.col("monetary_z") * 0.4 + 
                F.col("frequency_z") * 0.3 + 
                F.col("recency_z") * 0.3)
    
    # Segment customers
    .withColumn("value_segment",
                F.when(F.col("composite_score") >= 1.0, "VIP")
                 .when(F.col("composite_score") >= 0.5, "Premium")
                 .when(F.col("composite_score") >= -0.5, "Standard")
                 .when(F.col("composite_score") >= -1.0, "Opportunity")
                 .otherwise("At Risk"))
    
    # Calculate percentile ranks
    .withColumn("monetary_percentile", F.percent_rank().over(Window.orderBy("total_spent")))
    .withColumn("frequency_percentile", F.percent_rank().over(Window.orderBy("order_frequency")))
    .withColumn("value_percentile", F.percent_rank().over(Window.orderBy("composite_score"))))

customer_value_scoring.show()

# Cross-sell and Up-sell Opportunity Analysis

%python
# Identify cross-sell opportunities
cross_sell_analysis = (
    orders.join(order_items, "order_id")
    .join(products, "product_id")
    .groupBy("customer_id", "category")
    .agg(F.sum("quantity").alias("category_quantity"))
    
    # Pivot to get customer-category matrix
    .groupBy("customer_id")
    .pivot("category")
    .agg(F.first("category_quantity"))
    .fillna(0)
)

# Calculate category affinity scores
category_affinity = (
    order_items.join(products, "product_id")
    .join(orders, "order_id")
    .groupBy("customer_id", "category")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("unit_price").alias("avg_spent")
    )
    .withColumn("affinity_score", 
                F.col("order_count") * F.col("total_quantity") * F.col("avg_spent"))
)

# Find customers who bought in one category but not in related categories
cross_sell_opportunities = (
    category_affinity.alias("ca1")
    .join(category_affinity.alias("ca2"), "customer_id")
    .filter(F.col("ca1.category") != F.col("ca2.category"))
    .groupBy("ca1.customer_id", "ca1.category", "ca2.category")
    .agg(
        F.avg("ca1.affinity_score").alias("source_affinity"),
        F.avg("ca2.affinity_score").alias("target_affinity"))
    .withColumn("opportunity_score", 
                F.col("source_affinity") * (1 / (F.col("target_affinity") + 1)))
    .join(customers, "customer_id")
    .select("customer_id", "first_name", "last_name", 
            "ca1.category", "ca2.category", "opportunity_score")
    .orderBy(F.desc("opportunity_score")))

cross_sell_opportunities.show()

# Advanced Supplier Risk Assessment

%python
supplier_risk_assessment = (
    suppliers.join(products, "supplier_id")
    .join(order_items, "product_id")
    .join(orders, "order_id")
    .groupBy("supplier_id", "supplier_name", "country", "reliability_score")
    .agg(
        # Volume metrics
        F.countDistinct("product_id").alias("products_supplied"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("total_revenue"),
        
        # Financial metrics
        F.avg(F.col("unit_price") - F.col("cost")).alias("avg_profit_margin"),
        F.stddev(F.col("unit_price") - F.col("cost")).alias("margin_volatility"),
        
        # Timing metrics
        F.datediff(F.max("order_date"), F.min("order_date")).alias("supplier_activity_period"),
        F.avg(F.datediff("order_date", F.lag("order_date").over(
            Window.partitionBy("supplier_id").orderBy("order_date")
        ))).alias("avg_days_between_orders"),
        
        # Risk metrics
        F.countDistinct("customer_id").alias("customer_base"),
        F.approx_count_distinct("country").alias("geographic_diversity")
    )
    .fillna(0)
    .withColumn("revenue_concentration", 
                F.col("total_revenue") / F.sum("total_revenue").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing")))
    .withColumn("dependency_risk", 
                F.when(F.col("revenue_concentration") > 0.1, "High")
                 .when(F.col("revenue_concentration") > 0.05, "Medium")
                 .otherwise("Low"))
    .withColumn("margin_stability",
                F.when(F.col("margin_volatility") / F.greatest(F.col("avg_profit_margin"), 0.01) > 0.5, "High Volatility")
                 .when(F.col("margin_volatility") / F.greatest(F.col("avg_profit_margin"), 0.01) > 0.2, "Medium Volatility")
                 .otherwise("Stable"))
    .withColumn("business_continuity_risk",
                F.when(F.col("supplier_activity_period") < 90, "New Supplier")
                 .when(F.col("avg_days_between_orders") > 30, "Intermittent")
                 .otherwise("Regular"))
    .withColumn("overall_risk_score",
                F.when(F.col("dependency_risk") == "High", 3)
                 .when(F.col("dependency_risk") == "Medium", 2)
                 .otherwise(1) +
                F.when(F.col("margin_stability") == "High Volatility", 3)
                 .when(F.col("margin_stability") == "Medium Volatility", 2)
                 .otherwise(1) +
                F.when(F.col("business_continuity_risk") == "New Supplier", 3)
                 .when(F.col("business_continuity_risk") == "Intermittent", 2)
                 .otherwise(1))
    .withColumn("risk_category",
                F.when(F.col("overall_risk_score") >= 7, "High Risk")
                 .when(F.col("overall_risk_score") >= 5, "Medium Risk")
                 .otherwise("Low Risk")))

supplier_risk_assessment.show()

# Customer Sentiment and Review Analysis

%python
sentiment_analysis = (
    customer_reviews.join(customers, "customer_id")
    .join(products, "product_id")
    .groupBy("product_id", "product_name", "category")
    .agg(
        # Basic review metrics
        F.avg("rating").alias("avg_rating"),
        F.count("review_id").alias("review_count"),
        F.sum("helpful_votes").alias("total_helpful_votes"),
        
        # Rating distribution
        F.sum(F.when(F.col("rating") == 5, 1).otherwise(0)).alias("five_star_reviews"),
        F.sum(F.when(F.col("rating") == 4, 1).otherwise(0)).alias("four_star_reviews"),
        F.sum(F.when(F.col("rating") == 3, 1).otherwise(0)).alias("three_star_reviews"),
        F.sum(F.when(F.col("rating") == 2, 1).otherwise(0)).alias("two_star_reviews"),
        F.sum(F.when(F.col("rating") == 1, 1).otherwise(0)).alias("one_star_reviews"),
        
        # Temporal metrics
        F.datediff(F.current_date(), F.max("review_date")).alias("days_since_last_review"),
        F.datediff(F.max("review_date"), F.min("review_date")).alias("review_period_days")
    )
    .withColumn("helpfulness_ratio", 
                F.col("total_helpful_votes") / F.greatest(F.col("review_count"), 1))
    .withColumn("positive_review_ratio", 
                (F.col("five_star_reviews") + F.col("four_star_reviews")) / F.col("review_count"))
    .withColumn("negative_review_ratio", 
                (F.col("one_star_reviews") + F.col("two_star_reviews")) / F.col("review_count"))
    .withColumn("review_velocity", 
                F.col("review_count") / F.greatest(F.col("review_period_days") / 30.0, 1))
    .withColumn("sentiment_score",
                (F.col("avg_rating") * 0.4 +
                 F.col("positive_review_ratio") * 0.3 +
                 F.col("helpfulness_ratio") * 0.2 +
                 F.col("review_velocity") * 0.1))
    .withColumn("sentiment_category",
                F.when(F.col("sentiment_score") >= 4.5, "Excellent")
                 .when(F.col("sentiment_score") >= 4.0, "Very Good")
                 .when(F.col("sentiment_score") >= 3.5, "Good")
                 .when(F.col("sentiment_score") >= 3.0, "Average")
                 .when(F.col("sentiment_score") >= 2.5, "Below Average")
                 .otherwise("Poor"))
)

# Identify review patterns over time
review_trends = (
    customer_reviews.join(products, "product_id")
    .withColumn("review_month", F.date_format("review_date", "yyyy-MM"))
    .groupBy("product_id", "product_name", "review_month")
    .agg(
        F.avg("rating").alias("monthly_avg_rating"),
        F.count("review_id").alias("monthly_review_count"),
        F.avg("helpful_votes").alias("monthly_avg_helpful")
    )
    .withColumn("rating_trend", 
                F.avg("monthly_avg_rating").over(
                    Window.partitionBy("product_id")
                    .orderBy("review_month")
                    .rowsBetween(2, 0)))
    .withColumn("review_momentum",
                (F.col("monthly_review_count") - 
                 F.lag("monthly_review_count").over(
                     Window.partitionBy("product_id").orderBy("review_month"))) / F.lag("monthly_review_count").over(
                     Window.partitionBy("product_id").orderBy("review_month")) * 100))

sentiment_analysis.show()

# Comprehensive Business Health Dashboard

%python
# Create a comprehensive business health dashboard
business_health_dashboard = (
    orders.join(customers, "customer_id")
    .join(order_items, "order_id")
    .join(products, "product_id")
    .join(suppliers, "supplier_id")
    .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    .withColumn("order_year", F.year("order_date"))
    .groupBy("order_year", "order_month")
    .agg(
        # Sales metrics
        F.sum("total_amount").alias("monthly_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("active_customers"),
        
        # Customer metrics
        F.countDistinct(F.when(F.col("order_sequence") == 1, "customer_id")).alias("new_customers"),
        F.avg("total_amount").alias("avg_order_value"),
        
        # Product metrics
        F.countDistinct("product_id").alias("unique_products_sold"),
        F.sum("quantity").alias("total_units_sold"),
        
        # Financial metrics
        F.sum(F.col("quantity") * (F.col("unit_price") - F.col("cost"))).alias("gross_profit"),
        F.avg(F.col("unit_price") - F.col("cost")).alias("avg_profit_margin"),
        
        # Supplier metrics
        F.countDistinct("supplier_id").alias("active_suppliers"),
        F.avg("reliability_score").alias("avg_supplier_reliability")
    )
    .withColumn("revenue_growth", 
                (F.col("monthly_revenue") - F.lag("monthly_revenue").over(
                    Window.orderBy("order_year", "order_month")
                )) / F.lag("monthly_revenue").over(
                    Window.orderBy("order_year", "order_month")
                ) * 100)
    .withColumn("customer_growth", 
                (F.col("active_customers") - F.lag("active_customers").over(
                    Window.orderBy("order_year", "order_month")
                )) / F.lag("active_customers").over(
                    Window.orderBy("order_year", "order_month")
                ) * 100)
    .withColumn("profit_margin_percentage", 
                (F.col("gross_profit") / F.col("monthly_revenue")) * 100)
    .withColumn("health_score",
                (F.col("revenue_growth") * 0.25 +
                 F.col("customer_growth") * 0.25 +
                 F.col("profit_margin_percentage") * 0.3 +
                 F.col("avg_supplier_reliability") * 0.2))
    .withColumn("business_health",
                F.when(F.col("health_score") >= 20, "Excellent")
                 .when(F.col("health_score") >= 10, "Good")
                 .when(F.col("health_score") >= 0, "Stable")
                 .when(F.col("health_score") >= -10, "Concerning")
                 .otherwise("Critical"))
)

# Calculate KPIs for dashboard
current_period = business_health_dashboard.filter(
    F.col("order_year") == 2024 & F.col("order_month") == "2024-12"
)

previous_period = business_health_dashboard.filter(
    F.col("order_year") == 2024 & F.col("order_month") == "2024-11"
)

kpi_comparison = (
    current_period.alias("curr")
    .join(previous_period.alias("prev"), 
          (F.col("curr.order_year") == F.col("prev.order_year")) & 
          (F.col("curr.order_month") != F.col("prev.order_month")))
    .select(
        F.col("curr.monthly_revenue").alias("current_revenue"),
        F.col("prev.monthly_revenue").alias("previous_revenue"),
        F.col("curr.active_customers").alias("current_customers"),
        F.col("prev.active_customers").alias("previous_customers"),
        F.col("curr.gross_profit").alias("current_profit"),
        F.col("prev.gross_profit").alias("previous_profit"),
        F.col("curr.health_score").alias("current_health"),
        F.col("prev.health_score").alias("previous_health")
    )
    .withColumn("revenue_growth_pct", 
                (F.col("current_revenue") - F.col("previous_revenue")) / F.col("previous_revenue") * 100)
    .withColumn("customer_growth_pct", 
                (F.col("current_customers") - F.col("previous_customers")) / F.col("previous_customers") * 100)
    .withColumn("profit_growth_pct", 
                (F.col("current_profit") - F.col("previous_profit")) / F.col("previous_profit") * 100))

business_health_dashboard.show()
kpi_comparison.show()
