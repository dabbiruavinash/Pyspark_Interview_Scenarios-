# Peak Hour Analysis with Window Functions
# Identify peak ordering hours with ranking and time-based aggregations

%python
from pyspark.sql.window import Window
import pyspark.sql.functions as F

peak_hours = (order_initial
.withColumn("order_hour", F.hour("OrderDate"))
.groupBy("order_hour", F.date_trunc("day", "OrderDate").alias("order_day"))
.agg(F.count("*").alias("order_count"))
.withColumn("hourly_rank", F.rank().over(Window.partitionBy("order_day").orderBy(F.desc("order_count"))))
.filter(F.col("hourly_rank") <= 3))

# Customer Lifetime Value Calculation
# Calculate CLV using complex aggregations and date functions:

customer_clv = (order_initial
.groupBy("CustomerID")
.agg(
F.countDistinct("OrderID").alias("total_orders"),
F.sum("TotalAmount").alias("total_spent"),
F.datediff(F.max("OrderDate"), F.min("OrderDate")).alias("customer_tenure_days"),
F.avg("TotalAmount").alias("avg_order_value"),
F.max("OrderDate").alias("last_order_date"))
.withColumn("avg_order_frequency", F.when(F.col("customer_tenure_days") > 0,
F.col("total_orders") / F.col("customer_tenure_days")).otherwise(F.lit(0)))
.withColumn("clv_score", F.col("avg_order_value") * F.col("avg_order_frequency") * 365))

# Restaurant Performance Dashboard
# Multi-dimensional restaurant analysis with complex joins:

restaurant_performance = (restaurant
.join(order_intital, "RestaurantID", "left")
.join(menu, "Restaurant", left")
.filter(F.col("OrderDate") >= F.date_sub(F.current_date(), 30))
.groupBy("ResturantID", "Name", "CuisineType", "Locality")
.agg(
F.countDistinct("OrderID").alias("orders_last_30_days"),
F.sum("TotalAmount").alias("revenue_last_30_days"),
F.countDistinct("MenuID").alias("unique_items_sold"),
F.avg("Price").alias("avg_item_price"),
F.avg(F.when(order_initial.status == "Delivered",1).otherwise(0)).alias("delivery_success_rate"))
.withColumn("revenue_per_item", F.col("revenue_last_30_days") / F.col("unique_items_sold"))
.withColumn("preformace_tier" F.when(F.col("orders_last_30_days") > 100, "High")
.when(F.col("orders_last_30_days") > 50, "Medium").otherwise("Low")))

# Delivery Route Optimization Analysis
# Analyze delivery patterns using geospatial functions:

from pyspark.sql.functions import radians, sin, cos, atan2, sqrt, asin

delivery_analysis = (delivery_initial_load
    .join(restaurant, delivery_initial_load.RestaurantID == restaurant.RestaurantID)
    .withColumn("delivery_lat", F.split(F.col("DeliveryAddress"), ",").getItem(0).cast("double"))
    .withColumn("delivery_lon", F.split(F.col("DeliveryAddress"), ",").getItem(1).cast("double"))
    .withColumn("distance_km", 
                F.lit(6371) * F.asin(
                    F.sqrt(
                        F.sin(F.radians(F.col("delivery_lat") - F.col("Latitude"))/2)**2 +
                        F.cos(F.radians(F.col("Latitude"))) * 
                        F.cos(F.radians(F.col("delivery_lat"))) *
                        F.sin(F.radians(F.col("delivery_lon") - F.col("Longitude"))/2)**2)) * 2)
    .groupBy("DeliveryAgentID", F.date_trunc("day", "DeliveryDate").alias("delivery_day"))
    .agg(
        F.count("*").alias("total_deliveries"),
        F.sum("distance_km").alias("total_distance"),
        F.avg("distance_km").alias("avg_distance_per_delivery"),
        F.stddev("distance_km").alias("distance_variability"))
.withColumn("efficiency_score", F.col("total_deliveries") / F.col("total_distance")))

# Customer Segmentation by Order Patterns
# Segment customers using behavioral clustering logic:

customer_segments = (order_intital
.join(order_intital_item, "OrderID")
.join(menu, "MenuID")
.groupBy("CustomerID", "Category")
.pivot("Category")
agg(F.sum("Quantity").alias("total_quantity"))
.fillna(0)
.withColumn("total_items", sum(F.col(c) for c in ["Main course", "Dessert", "Beverage", "Appetizer"]))
.withColumn("segment",
                F.when((F.col("Main Course") / F.col("total_items") > 0.7), "Main Course Lover")
                 .when((F.col("Dessert") / F.col("total_items") > 0.5), "Dessert Enthusiast")
                 .when((F.col("Beverage") / F.col("total_items") > 0.6), "Beverage Fanatic")
                 .when(F.col("total_items") > 50, "Heavy User")
                 .when(F.col("total_items") > 20, "Regular User")
                 .otherwise("Light User")))

# Real-time Inventory Alert System
# Monitor menu item availability with complex conditions:

inventory_alerts = (menu
    .join(restaurant, "RestaurantID")
    .join(order_initial_item
          .filter(F.col("CreatedDate") >= F.date_sub(F.current_date(), 7))
          .groupBy("MenuID")
          .agg(F.sum("Quantity").alias("last_7_days_sales")),
          "MenuID", "left")
    .withColumn("sales_trend",
                F.when(F.col("last_7_days_sales") > F.col("Availability") * 0.8, "High Demand")
                 .when(F.col("last_7_days_sales") > F.col("Availability") * 0.5, "Moderate Demand")
                 .otherwise("Low Demand"))
    .withColumn("alert_level",
                F.when((F.col("Availability") < 10) & (F.col("sales_trend") == "High Demand"), "CRITICAL")
                 .when((F.col("Availability") < 20) & (F.col("sales_trend") == "High Demand"), "HIGH")
                 .when(F.col("Availability") == 0, "OUT_OF_STOCK")
                 .otherwise("NORMAL")))

# Cohort Analysis for Customer Retention
# Monthly cohort analysis with retention rates:

cohort_analysis = (order_initial
    .withColumn("cohort_month", F.date_trunc("month", F.min("OrderDate").over(Window.partitionBy("CustomerID"))))
    .withColumn("order_month", F.date_trunc("month", "OrderDate"))
    .withColumn("months_since_cohort", 
                F.months_between("order_month", "cohort_month"))
    .groupBy("cohort_month", "months_since_cohort")
    .agg(F.countDistinct("CustomerID").alias("customers"))
    .groupBy("cohort_month")
    .pivot("months_since_cohort")
    .agg(F.first("customers"))
    .withColumn("month_0_retention", F.col("0") / F.col("0"))
    .withColumn("month_1_retention", F.col("1") / F.col("0"))
    .withColumn("month_2_retention", F.col("2") / F.col("0")))

#Cross-selling Analysis
# Identify frequently bought together items using self-joins:

frequently_bought_together = (order_initial_item
    .alias("oi1")
    .join(order_initial_item.alias("oi2"), 
          (F.col("oi1.OrderID") == F.col("oi2.OrderID")) & 
          (F.col("oi1.MenuID") < F.col("oi2.MenuID")))
    .join(menu.alias("m1"), F.col("oi1.MenuID") == F.col("m1.MenuID"))
    .join(menu.alias("m2"), F.col("oi2.MenuID") == F.col("m2.MenuID"))
    .groupBy(F.col("m1.ItemName").alias("item1"), F.col("m2.ItemName").alias("item2"))
    .agg(F.count("*").alias("pair_count"))
    .filter(F.col("pair_count") > 10)
    .orderBy(F.desc("pair_count")))

# Delivery Time SLA Compliance
# Analyze delivery performance against SLAs:
 
delivery_sla = (delivery_initial_load
    .join(order_initial, "OrderID")
    .withColumn("estimated_delivery_time", 
                F.from_unixtime(F.unix_timestamp("OrderDate") + F.col("EstimatedTime") * 60))
    .withColumn("actual_delivery_time", "DeliveryDate")
    .withColumn("delivery_time_diff_minutes", 
                (F.unix_timestamp("actual_delivery_time") - 
                 F.unix_timestamp("estimated_delivery_time")) / 60)
    .withColumn("sla_breach",
                F.when(F.col("delivery_time_diff_minutes") > 15, "Late")
                 .when(F.col("delivery_time_diff_minutes") < -10, "Early")
                 .otherwise("On Time"))
    .groupBy("DeliveryAgentID", F.date_format("DeliveryDate", "yyyy-MM-dd").alias("delivery_date"))
    .agg(
        F.count("*").alias("total_deliveries"),
        F.avg("delivery_time_diff_minutes").alias("avg_delay_minutes"),
        F.sum(F.when(F.col("sla_breach") == "Late", 1).otherwise(0)).alias("late_deliveries"),
        F.sum(F.when(F.col("sla_breach") == "Early", 1).otherwise(0)).alias("early_deliveries"))
    .withColumn("on_time_percentage", (F.col("total_deliveries") - F.col("late_deliveries")) / F.col("total_deliveries") * 100))

# Revenue Forecasting using Time Series
# Time series decomposition without ML:

revenue_forecast = (order_initial
    .groupBy(F.date_trunc("day", "OrderDate").alias("order_day"))
    .agg(F.sum("TotalAmount").alias("daily_revenue"))
    .withColumn("day_of_week", F.date_format("order_day", "E"))
    .withColumn("day_of_month", F.dayofmonth("order_day"))
    .withColumn("is_weekend", F.when(F.col("day_of_week").isin(["Sat", "Sun"]), 1).otherwise(0))
    .groupBy("day_of_week", "is_weekend")
    .agg(
        F.avg("daily_revenue").alias("avg_daily_revenue"),
        F.stddev("daily_revenue").alias("revenue_stddev"),
        F.min("daily_revenue").alias("min_revenue"),
        F.max("daily_revenue").alias("max_revenue"))
    .withColumn("revenue_variability", F.col("revenue_stddev") / F.col("avg_daily_revenue"))
    .withColumn("forecast_range_lower", F.col("avg_daily_revenue") - 1.96 * F.col("revenue_stddev"))
    .withColumn("forecast_range_upper", F.col("avg_daily_revenue") + 1.96 * F.col("revenue_stddev")))

# Customer Churn Prediction Logic
# Identify at-risk customers using business rules:

customer_churn_risk = (order_initial
    .groupBy("CustomerID")
    .agg(
        F.max("OrderDate").alias("last_order_date"),
        F.count("*").alias("total_orders"),
        F.avg("TotalAmount").alias("avg_order_value"),
        F.stddev("TotalAmount").alias("order_value_stddev"))
    .withColumn("days_since_last_order", F.datediff(F.current_date(), "last_order_date"))
    .withColumn("churn_risk_score",
                (F.col("days_since_last_order") * 0.4) +
                (F.when(F.col("total_orders") < 5, 30).otherwise(0) * 0.3) +
                (F.when(F.col("order_value_stddev") > F.col("avg_order_value") * 0.5, 20).otherwise(0) * 0.3))
    .withColumn("churn_risk_tier",
                F.when(F.col("churn_risk_score") > 50, "High Risk")
                 .when(F.col("churn_risk_score") > 30, "Medium Risk")
                 .otherwise("Low Risk")))

# Menu Price Optimization Analysis
# Analyze pricing strategies and elasticity:

price_optimization = (menu
    .join(order_initial_item, "MenuID")
    .groupBy("MenuID", "ItemName", "Price", "Category", "RestaurantID")
    .agg(
        F.sum("Quantity").alias("total_quantity_sold"),
        F.countDistinct("OrderID").alias("order_frequency"),
        F.avg("Quantity").alias("avg_quantity_per_order"))
    .withColumn("price_percentile", F.percent_rank().over(Window.partitionBy("Category", "RestaurantID").orderBy("Price")))
    .withColumn("demand_elasticity",F.when(F.col("price_percentile") < 0.25, 
                       F.col("total_quantity_sold") / F.col("Price")).otherwise(F.lit(None)))
    .withColumn("pricing_recommendation",
                F.when((F.col("price_percentile") > 0.75) & (F.col("total_quantity_sold") > 100), "Consider Price Reduction")
                  .when((F.col("price_percentile") < 0.25) & (F.col("total_quantity_sold") < 50), "Consider Price Increase")
                 .otherwise("Optimal Pricing")))

# Multi-location Restaurant Performance Comparison
# Compare restaurants across different locations:

location_performance = (restaurant
    .join(locations, "LocationID")
    .join(order_initial, "RestaurantID")
    .filter(F.col("OrderDate") >= F.date_sub(F.current_date(), 90))
    .groupBy("City", "State", "Locality", "RestaurantID", "Name")
    .agg(
        F.countDistinct("OrderID").alias("order_count"),
        F.sum("TotalAmount").alias("total_revenue"),
        F.avg("TotalAmount").alias("avg_order_value"),
        F.countDistinct(F.date_trunc("day", "OrderDate")).alias("active_days"))
    .withColumn("daily_revenue", F.col("total_revenue") / F.col("active_days"))
    .withColumn("city_rank", F.rank().over(Window.partitionBy("City").orderBy(F.desc("daily_revenue"))))
    .withColumn("locality_performance_ratio",F.col("daily_revenue") / F.avg("daily_revenue").over(Window.partitionBy("Locality"))))

# Customer Address Analysis for Delivery Optimization
# Analyze delivery addresses for clustering opportunities:

address_clustering_analysis = (customer_address
    .join(order_initial, "CustomerID")
    .withColumn("coordinates_array", F.split(F.col("Coordinates"), ","))
    .withColumn("latitude", F.element_at(F.col("coordinates_array"), 1).cast("double"))
    .withColumn("longitude", F.element_at(F.col("coordinates_array"), 2).cast("double"))
    .groupBy("Pincode", "Locality", "City")
    .agg(
        F.countDistinct("CustomerID").alias("unique_customers"),
        F.countDistinct("OrderID").alias("total_orders"),
        F.avg("latitude").alias("avg_latitude"),
        F.avg("longitude").alias("avg_longitude"),
        F.collect_set("Building").alias("buildings"))
    .withColumn("building_count", F.size("buildings"))
    .withColumn("density_score", F.col("unique_customers") / F.col("building_count"))
    .withColumn("delivery_hub_recommendation",
                F.when(F.col("density_score") > 10, "High Priority Hub")
                 .when(F.col("density_score") > 5, "Medium Priority Hub")
                 .otherwise("Low Priority Hub")))

# Seasonal Menu Analysis
# Analyze seasonal patterns in menu item sales:

seasonal_analysis = (order_initial_item
    .join(order_initial, "OrderID")
    .join(menu, "MenuID")
    .withColumn("order_month", F.month("OrderDate"))
    .withColumn("season",
                F.when(F.col("order_month").between(3, 5), "Spring")
                 .when(F.col("order_month").between(6, 8), "Summer")
                 .when(F.col("order_month").between(9, 11), "Fall")
                 .otherwise("Winter"))
    .groupBy("MenuID", "ItemName", "Category", "season")
    .agg(
        F.sum("Quantity").alias("seasonal_quantity"),
        F.countDistinct("OrderID").alias("seasonal_orders"))
    .groupBy("MenuID", "ItemName")
    .pivot("season")
    .agg(F.first("seasonal_quantity"))
    .fillna(0)
    .withColumn("seasonal_variation",
                (F.greatest("Spring", "Summer", "Fall", "Winter") - 
                 F.least("Spring", "Summer", "Fall", "Winter")) / 
                F.least("Spring", "Summer", "Fall", "Winter"))
    .withColumn("peak_season",
                F.when(F.col("Summer") == F.greatest("Spring", "Summer", "Fall", "Winter"), "Summer")
                 .when(F.col("Winter") == F.greatest("Spring", "Summer", "Fall", "Winter"), "Winter")
                 .when(F.col("Spring") == F.greatest("Spring", "Summer", "Fall", "Winter"), "Spring")
                 .otherwise("Fall")))

# Payment Method Analysis
# Analyze payment patterns and fraud indicators:

payment_analysis = (order_initial
    .groupBy("PaymentMethod", F.date_trunc("month", "OrderDate").alias("order_month"))
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("TotalAmount").alias("total_amount"),
        F.avg("TotalAmount").alias("avg_transaction_value"),
        F.stddev("TotalAmount").alias("transaction_stddev"))
    .withColumn("month_over_month_growth",
                (F.col("transaction_count") - 
                 F.lag("transaction_count", 1).over(Window.partitionBy("PaymentMethod").orderBy("order_month"))) / 
                F.lag("transaction_count", 1).over(Window.partitionBy("PaymentMethod").orderBy("order_month")) * 100)
    .withColumn("suspicious_pattern",
                F.when((F.col("transaction_stddev") > F.col("avg_transaction_value") * 2) & (F.col("transaction_count") < 10), "High Variance")
                 .when(F.col("month_over_month_growth") > 200, "Sudden Spike")
                 .otherwise("Normal"))
    .withColumn("payment_method_share",
                F.col("transaction_count") / 
                F.sum("transaction_count").over(Window.partitionBy("order_month")) * 100))

# Customer Preference Analysis
# Analyze customer preferences from multiple sources:

customer_preferences = (customer
    .join(order_initial, "CustomerID", "left")
    .join(order_initial_item, "OrderID", "left")
    .join(menu, "MenuID", "left")
    .groupBy("CustomerID", "Name", "Gender", "Preferences")
    .agg(
        F.collect_set("Category").alias("ordered_categories"),
        F.collect_set("CuisineType").alias("ordered_cuisines"),
        F.avg("Price").alias("avg_item_price_preference"),
        F.countDistinct("RestaurantID").alias("unique_restaurants"))
    .withColumn("preference_match_score",F.when(F.array_contains(F.split(F.col("Preferences"), ","), F.col("ordered_cuisines")[0]), 100)
                 .otherwise(50))
    .withColumn("food_adventurousness",F.size("ordered_cuisines") / F.col("unique_restaurants"))
    .withColumn("preference_insights",
                F.concat(F.lit("Prefers "), 
                        F.array_join(F.slice("ordered_categories", 1, 3), ", "),
                        F.lit(" from "),
                        F.array_join(F.slice("ordered_cuisines", 1, 2), ", "))))

# Delivery Agent Performance Ranking
# Rank delivery agents using multiple KPIs:

agent_performance = (delivery_agent_intial
    .join(delivery_initial_load, "DeliveryAgentID", "left")
    .join(order_initial, "OrderID", "left")
    .filter(F.col("DeliveryDate") >= F.date_sub(F.current_date(), 30))
    .groupBy("DeliveryAgentID", "Name", "VehicleType", "Rating")
    .agg(
        F.count("*").alias("total_deliveries"),
        F.avg("TotalAmount").alias("avg_order_value"),
        F.sum(F.when(F.col("DeliveryStatus") == "Delivered", 1).otherwise(0)).alias("successful_deliveries"),
        F.avg((F.unix_timestamp("DeliveryDate") -  F.unix_timestamp("OrderDate")) / 60).alias("avg_delivery_time_minutes"),
        F.stddev("TotalAmount").alias("order_value_variability"))
    .withColumn("success_rate", F.col("successful_deliveries") / F.col("total_deliveries") * 100)
    .withColumn("efficiency_score",
                (F.col("total_deliveries") * 0.3) +
                (F.col("success_rate") * 0.3) +
                (F.col("Rating") * 20 * 0.2) +
                ((60 - F.least(F.col("avg_delivery_time_minutes"), 60)) * 0.2))
    .withColumn("performance_percentile",F.percent_rank().over(Window.orderBy(F.desc("efficiency_score"))))
    .withColumn("performance_tier",
                F.when(F.col("performance_percentile") > 0.8, "Top Performer")
                 .when(F.col("performance_percentile") > 0.5, "Above Average")
                 .when(F.col("performance_percentile") > 0.2, "Average")
                 .otherwise("Needs Improvement")))

# Restaurant Capacity Utilization
# Analyze restaurant capacity and utilization:

restaurant_capacity = (order_initial
    .join(restaurant, "RestaurantID")
    .withColumn("order_hour", F.hour("OrderDate"))
    .groupBy("RestaurantID", "Name", "OperatingHours", "order_hour")
    .agg(
        F.count("*").alias("order_count"),
        F.avg("TotalAmount").alias("avg_order_value")
    )
    .withColumn("peak_hours", 
                F.when(F.col("order_count") > 
                       F.avg("order_count").over(Window.partitionBy("RestaurantID")) * 1.5, 
                       "Peak")
                 .otherwise("Non-Peak"))
    .groupBy("RestaurantID", "Name")
    .agg(
        F.sum(F.when(F.col("peak_hours") == "Peak", F.col("order_count")).otherwise(0))
          .alias("peak_hour_orders"),
        F.sum("order_count").alias("total_orders"),
        F.collect_list(F.struct("order_hour", "order_count", "peak_hours")).alias("hourly_distribution"))
    .withColumn("capacity_utilization", F.col("peak_hour_orders") / F.col("total_orders"))
    .withColumn("capacity_recommendation",
                F.when(F.col("capacity_utilization") > 0.7, "Increase Capacity")
                 .when(F.col("capacity_utilization") < 0.3, "Reduce Hours")
                 .otherwise("Optimal Capacity")))

# Comprehensive Business Health Dashboard
# Create a complete business health monitoring system:

business_health = {
    "financial_health": (
        order_initial
        .filter(F.col("OrderDate") >= F.date_sub(F.current_date(), 30))
        .agg(
            F.sum("TotalAmount").alias("monthly_revenue"),
            F.avg("TotalAmount").alias("avg_order_value"),
            F.countDistinct("CustomerID").alias("active_customers"),
            F.countDistinct("RestaurantID").alias("active_restaurants")
        )
    ),
    
    "operational_health": (
        delivery_initial_load
        .filter(F.col("DeliveryDate") >= F.date_sub(F.current_date(), 30))
        .agg(
            F.avg((F.unix_timestamp("DeliveryDate") - 
                   F.unix_timestamp("CreatedDate")) / 60).alias("avg_delivery_time"),
            F.avg(F.when(F.col("DeliveryStatus") == "Delivered", 1).otherwise(0)).alias("delivery_success_rate"),
            F.countDistinct("DeliveryAgentID").alias("active_agents")
        )
    ),
    
    "customer_health": (
        order_initial
        .filter(F.col("OrderDate") >= F.date_sub(F.current_date(), 30))
        .groupBy(F.date_trunc("day", "OrderDate").alias("order_day"))
        .agg(F.countDistinct("CustomerID").alias("daily_active_customers"))
        .agg(
            F.avg("daily_active_customers").alias("avg_daily_active_customers"),
            F.stddev("daily_active_customers").alias("dau_volatility")
        )
    ),
    
    "restaurant_health": (
        restaurant
        .join(order_initial, "RestaurantID", "left")
        .filter(F.col("OrderDate") >= F.date_sub(F.current_date(), 30))
        .groupBy("RestaurantID")
        .agg(F.count("*").alias("monthly_orders"))
        .agg(
            F.avg("monthly_orders").alias("avg_restaurant_orders"),
            F.sum(F.when(F.col("monthly_orders") == 0, 1).otherwise(0)).alias("inactive_restaurants")
        )
    )
}

# Combine all metrics
health_dashboard = (
    business_health["financial_health"]
    .crossJoin(business_health["operational_health"])
    .crossJoin(business_health["customer_health"])
    .crossJoin(business_health["restaurant_health"])
    .withColumn("overall_health_score",
                (F.col("monthly_revenue") / 10000 * 0.25) +
                (F.col("delivery_success_rate") * 100 * 0.25) +
                (F.col("avg_daily_active_customers") * 0.25) +
                (F.col("avg_restaurant_orders") * 0.25))
    .withColumn("health_status",
                F.when(F.col("overall_health_score") > 80, "Excellent")
                 .when(F.col("overall_health_score") > 60, "Good")
                 .when(F.col("overall_health_score") > 40, "Fair")
                 .otherwise("Needs Attention")))