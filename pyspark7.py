# Meta/Facebook (Common Pattern: Social media engagement)

%sql
SELECT 
    user_id,
    COUNT(DISTINCT post_id) as total_posts,
    SUM(like_count) as total_likes,
    SUM(share_count) as total_shares,
    SUM(comment_count) as total_comments,
    AVG(like_count) as avg_likes_per_post,
    COUNT(DISTINCT follower_id) as total_followers,
    ROUND((SUM(like_count) + SUM(share_count) * 2 + SUM(comment_count) * 1.5) / 
          NULLIF(COUNT(DISTINCT post_id), 0), 2) as engagement_score,
    SUM(CASE WHEN post_type = 'video' THEN 1 ELSE 0 END) as video_posts,
    RANK() OVER (ORDER BY (SUM(like_count) + SUM(share_count) * 2 + SUM(comment_count) * 1.5) DESC) as engagement_rank
FROM user_posts
WHERE post_date >= SYSDATE - 30
GROUP BY user_id
HAVING COUNT(DISTINCT post_id) >= 5
ORDER BY engagement_score DESC;

%python
window_spec = Window.orderBy(F.desc("engagement_score"))

result = (df.filter(df.post_date >= F.current_date() - 30)
          .groupBy("user_id")
          .agg(F.countDistinct("post_id").alias("total_posts"),
               F.sum("like_count").alias("total_likes"),
               F.sum("share_count").alias("total_shares"),
               F.sum("comment_count").alias("total_comments"),
               F.avg("like_count").alias("avg_likes_per_post"),
               F.countDistinct("follower_id").alias("total_followers"),
               F.sum(F.when(df.post_type == "video", 1).otherwise(0)).alias("video_posts"))
          .withColumn("engagement_score", 
                     (F.col("total_likes") + F.col("total_shares") * 2 + F.col("total_comments") * 1.5) / 
                     F.nullif(F.col("total_posts"), 0))
          .withColumn("engagement_rank", F.rank().over(window_spec))
          .filter(F.col("total_posts") >= 5)
          .orderBy(F.desc("engagement_score")))

# Amazon (Common Pattern: Order fulfillment analytics)

%sql
SELECT 
    warehouse_id,
    product_category,
    COUNT(*) as total_orders,
    AVG(processing_time_hours) as avg_processing_time,
    SUM(order_value) as total_order_value,
    SUM(CASE WHEN processing_time_hours > 48 THEN 1 ELSE 0 END) as delayed_orders,
    SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) as returned_orders,
    ROUND(SUM(CASE WHEN processing_time_hours > 48 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delay_rate,
    ROUND(SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as return_rate
FROM orders
WHERE order_date >= TRUNC(SYSDATE) - 30
GROUP BY warehouse_id, product_category
HAVING COUNT(*) >= 50
ORDER BY delay_rate DESC;

%python
result = (df.filter(df.order_date >= F.current_date() - 30)
          .groupBy("warehouse_id", "product_category")
          .agg(F.count("*").alias("total_orders"),
               F.avg("processing_time_hours").alias("avg_processing_time"),
               F.sum("order_value").alias("total_order_value"),
               F.sum(F.when(df.processing_time_hours > 48, 1).otherwise(0)).alias("delayed_orders"),
               F.sum(F.when(df.order_status == "returned", 1).otherwise(0)).alias("returned_orders"))
          .withColumn("delay_rate", (F.col("delayed_orders") * 100.0 / F.col("total_orders")))
          .withColumn("return_rate", (F.col("returned_orders") * 100.0 / F.col("total_orders")))
          .filter(F.col("total_orders") >= 50)
          .orderBy(F.desc("delay_rate")))

# Google (Common Pattern: Search engine metrics)

%sql
SELECT 
    search_query_type,
    COUNT(DISTINCT session_id) as total_searches,
    AVG(result_count) as avg_results,
    AVG(CASE WHEN click_position IS NOT NULL THEN 1 ELSE 0 END) as overall_ctr,
    AVG(CASE WHEN click_position <= 3 THEN 1 ELSE 0 END) as top3_ctr,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY click_position) as median_click_position,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(CASE WHEN click_position IS NULL THEN 1 ELSE 0 END) as no_click_searches,
    ROUND(SUM(CASE WHEN click_position IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as no_click_rate
FROM search_logs
WHERE search_timestamp >= SYSDATE - 7
GROUP BY search_query_type
HAVING COUNT(DISTINCT session_id) >= 1000
ORDER BY total_searches DESC;

%python
result = (df.filter(df.search_timestamp >= F.current_date() - 7)
          .groupBy("search_query_type")
          .agg(F.countDistinct("session_id").alias("total_searches"),
               F.avg("result_count").alias("avg_results"),
               F.avg(F.when(df.click_position.isNotNull(), 1).otherwise(0)).alias("overall_ctr"),
               F.avg(F.when(df.click_position <= 3, 1).otherwise(0)).alias("top3_ctr"),
               F.countDistinct("user_id").alias("unique_users"),
               F.sum(F.when(df.click_position.isNull(), 1).otherwise(0)).alias("no_click_searches"))
          .withColumn("no_click_rate", (F.col("no_click_searches") * 100.0 / F.col("total_searches")))
          .filter(F.col("total_searches") >= 1000)
          .orderBy(F.desc("total_searches")))

# Uber (Common Pattern: Ride-sharing analytics)

%sql
SELECT 
    driver_id,
    city,
    COUNT(*) as total_rides,
    AVG(fare_amount) as avg_fare,
    AVG(rating) as avg_rating,
    SUM(fare_amount * surge_multiplier) as total_earnings,
    AVG(wait_time_minutes) as avg_wait_time,
    SUM(CASE WHEN surge_multiplier > 1.0 THEN 1 ELSE 0 END) as surge_rides,
    SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) as five_star_rides,
    ROUND(SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as five_star_rate
FROM rides
WHERE ride_date >= TRUNC(SYSDATE) - 7
GROUP BY driver_id, city
HAVING COUNT(*) >= 10
ORDER BY total_earnings DESC;

%python
result = (df.filter(df.ride_date >= F.current_date() - 7)
          .groupBy("driver_id", "city")
          .agg(F.count("*").alias("total_rides"),
               F.avg("fare_amount").alias("avg_fare"),
               F.avg("rating").alias("avg_rating"),
               F.sum(df.fare_amount * df.surge_multiplier).alias("total_earnings"),
               F.avg("wait_time_minutes").alias("avg_wait_time"),
               F.sum(F.when(df.surge_multiplier > 1.0, 1).otherwise(0)).alias("surge_rides"),
               F.sum(F.when(df.rating == 5, 1).otherwise(0)).alias("five_star_rides"))
          .withColumn("five_star_rate", (F.col("five_star_rides") * 100.0 / F.col("total_rides")))
          .filter(F.col("total_rides") >= 10)
          .orderBy(F.desc("total_earnings")))

# Microsoft (Common Pattern: Software usage analytics)

%sql
SELECT 
    product_name,
    version,
    COUNT(DISTINCT user_id) as active_users,
    AVG(session_minutes) as avg_session_length,
    SUM(feature_usage_count) as total_feature_usage,
    COUNT(DISTINCT feature_name) as unique_features_used,
    AVG(crash_count) as avg_crashes,
    SUM(CASE WHEN is_premium = 1 THEN 1 ELSE 0 END) as premium_users,
    ROUND(SUM(CASE WHEN is_premium = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT user_id), 2) as premium_rate,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY session_minutes) as p95_session_length
FROM software_usage
WHERE usage_date >= ADD_MONTHS(SYSDATE, -3)
GROUP BY product_name, version
HAVING COUNT(DISTINCT user_id) >= 1000
ORDER BY active_users DESC;

%python
result = (df.filter(df.usage_date >= F.add_months(F.current_date(), -3))
          .groupBy("product_name", "version")
          .agg(F.countDistinct("user_id").alias("active_users"),
               F.avg("session_minutes").alias("avg_session_length"),
               F.sum("feature_usage_count").alias("total_feature_usage"),
               F.countDistinct("feature_name").alias("unique_features_used"),
               F.avg("crash_count").alias("avg_crashes"),
               F.sum(F.when(df.is_premium == 1, 1).otherwise(0)).alias("premium_users"),
               F.expr("percentile_approx(session_minutes, 0.95)").alias("p95_session_length"))
          .withColumn("premium_rate", (F.col("premium_users") * 100.0 / F.col("active_users")))
          .filter(F.col("active_users") >= 1000)
          .orderBy(F.desc("active_users")))

# Airbnb (Common Pattern: Hospitality analytics)

%sql
SELECT 
    host_id,
    COUNT(DISTINCT listing_id) as total_listings,
    AVG(price_per_night) as avg_price,
    AVG(review_score) as avg_rating,
    COUNT(*) as total_bookings,
    SUM(booking_value) as total_revenue,
    AVG(DATEDIFF(check_out_date, check_in_date)) as avg_stay_length,
    SUM(CASE WHEN cancellation_date IS NULL THEN booking_value ELSE 0 END) as realized_revenue,
    ROUND(SUM(CASE WHEN cancellation_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as completion_rate,
    COUNT(DISTINCT guest_country) as unique_guest_countries
FROM bookings
WHERE check_in_date >= ADD_MONTHS(SYSDATE, -12)
GROUP BY host_id
HAVING COUNT(*) >= 5
ORDER BY total_revenue DESC;

%python
SELECT 
    host_id,
    COUNT(DISTINCT listing_id) as total_listings,
    AVG(price_per_night) as avg_price,
    AVG(review_score) as avg_rating,
    COUNT(*) as total_bookings,
    SUM(booking_value) as total_revenue,
    AVG(DATEDIFF(check_out_date, check_in_date)) as avg_stay_length,
    SUM(CASE WHEN cancellation_date IS NULL THEN booking_value ELSE 0 END) as realized_revenue,
    ROUND(SUM(CASE WHEN cancellation_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as completion_rate,
    COUNT(DISTINCT guest_country) as unique_guest_countries
FROM bookings
WHERE check_in_date >= ADD_MONTHS(SYSDATE, -12)
GROUP BY host_id
HAVING COUNT(*) >= 5
ORDER BY total_revenue DESC;

# IBM (Common Pattern: IT system monitoring)

%sql
SELECT 
    server_id,
    application_name,
    AVG(cpu_utilization) as avg_cpu_usage,
    AVG(memory_utilization) as avg_memory_usage,
    AVG(disk_utilization) as avg_disk_usage,
    SUM(CASE WHEN cpu_utilization > 90 THEN 1 ELSE 0 END) as high_cpu_events,
    SUM(CASE WHEN memory_utilization > 85 THEN 1 ELSE 0 END) as high_memory_events,
    MAX(response_time_ms) as max_response_time,
    AVG(response_time_ms) as avg_response_time,
    COUNT(DISTINCT error_type) as unique_errors,
    SUM(CASE WHEN error_type IS NOT NULL THEN 1 ELSE 0 END) as total_errors
FROM server_metrics
WHERE metric_timestamp >= SYSDATE - 1
GROUP BY server_id, application_name
HAVING COUNT(*) >= 100
ORDER BY avg_cpu_usage DESC;

%python
result = (df.filter(df.metric_timestamp >= F.current_date() - 1)
          .groupBy("server_id", "application_name")
          .agg(F.avg("cpu_utilization").alias("avg_cpu_usage"),
               F.avg("memory_utilization").alias("avg_memory_usage"),
               F.avg("disk_utilization").alias("avg_disk_usage"),
               F.sum(F.when(df.cpu_utilization > 90, 1).otherwise(0)).alias("high_cpu_events"),
               F.sum(F.when(df.memory_utilization > 85, 1).otherwise(0)).alias("high_memory_events"),
               F.max("response_time_ms").alias("max_response_time"),
               F.avg("response_time_ms").alias("avg_response_time"),
               F.countDistinct("error_type").alias("unique_errors"),
               F.sum(F.when(df.error_type.isNotNull(), 1).otherwise(0)).alias("total_errors"))
          .filter(F.count("*") >= 100)
          .orderBy(F.desc("avg_cpu_usage")))

# Tesla (Common Pattern: Vehicle telematics)

%sql
SELECT 
    vehicle_id,
    AVG(battery_level) as avg_battery_level,
    SUM(miles_driven) as total_miles,
    AVG(energy_consumption_kwh) as avg_energy_consumption,
    COUNT(DISTINCT charging_session_id) as charging_sessions,
    AVG(charging_duration_minutes) as avg_charging_time,
    SUM(CASE WHEN battery_level < 20 THEN 1 ELSE 0 END) as low_battery_events,
    MAX(odometer) as total_odometer,
    AVG(temperature_c) as avg_temperature,
    CORR(miles_driven, energy_consumption_kwh) as mileage_efficiency_correlation
FROM vehicle_metrics
WHERE metric_date >= SYSDATE - 7
GROUP BY vehicle_id
HAVING SUM(miles_driven) >= 100
ORDER BY total_miles DESC;

%python
result = (df.filter(df.metric_date >= F.current_date() - 7)
          .groupBy("vehicle_id")
          .agg(F.avg("battery_level").alias("avg_battery_level"),
               F.sum("miles_driven").alias("total_miles"),
               F.avg("energy_consumption_kwh").alias("avg_energy_consumption"),
               F.countDistinct("charging_session_id").alias("charging_sessions"),
               F.avg("charging_duration_minutes").alias("avg_charging_time"),
               F.sum(F.when(df.battery_level < 20, 1).otherwise(0)).alias("low_battery_events"),
               F.max("odometer").alias("total_odometer"),
               F.avg("temperature_c").alias("avg_temperature"),
               F.corr("miles_driven", "energy_consumption_kwh").alias("mileage_efficiency_correlation"))
          .filter(F.col("total_miles") >= 100)
          .orderBy(F.desc("total_miles")))

# Netflix (Common Pattern: Content streaming analytics)

%sql
SELECT 
    content_id,
    content_type,
    COUNT(DISTINCT user_id) as unique_viewers,
    SUM(watch_minutes) as total_watch_time,
    AVG(watch_minutes) as avg_watch_time,
    COUNT(*) as total_views,
    SUM(CASE WHEN watch_minutes >= content_duration * 0.8 THEN 1 ELSE 0 END) as completed_views,
    ROUND(SUM(CASE WHEN watch_minutes >= content_duration * 0.8 THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(*), 2) as completion_rate,
    AVG(user_rating) as avg_rating,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY watch_minutes) as p90_watch_time
FROM streaming_data
WHERE watch_date >= SYSDATE - 30
GROUP BY content_id, content_type
HAVING COUNT(*) >= 100
ORDER BY total_watch_time DESC;

%python
result = (df.filter(df.watch_date >= F.current_date() - 30)
          .groupBy("content_id", "content_type")
          .agg(F.countDistinct("user_id").alias("unique_viewers"),
               F.sum("watch_minutes").alias("total_watch_time"),
               F.avg("watch_minutes").alias("avg_watch_time"),
               F.count("*").alias("total_views"),
               F.sum(F.when(df.watch_minutes >= df.content_duration * 0.8, 1).otherwise(0)).alias("completed_views"),
               F.avg("user_rating").alias("avg_rating"),
               F.expr("percentile_approx(watch_minutes, 0.9)").alias("p90_watch_time"))
          .withColumn("completion_rate", (F.col("completed_views") * 100.0 / F.col("total_views")))
          .filter(F.col("total_views") >= 100)
          .orderBy(F.desc("total_watch_time")))

# Amazon (Common Pattern: E-commerce customer behavior)

%sql
SELECT 
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(order_amount) as lifetime_value,
    AVG(order_amount) as avg_order_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    COUNT(DISTINCT product_category) as categories_purchased,
    ROUND((MAX(order_date) - MIN(order_date)) / NULLIF(COUNT(DISTINCT order_id) - 1, 0), 2) as avg_days_between_orders,
    SUM(CASE WHEN product_category = 'Electronics' THEN 1 ELSE 0 END) as electronics_orders,
    RANK() OVER (ORDER BY SUM(order_amount) DESC) as spending_rank
FROM orders
WHERE order_date >= ADD_MONTHS(SYSDATE, -12)
GROUP BY customer_id
HAVING COUNT(DISTINCT order_id) >= 3
ORDER BY lifetime_value DESC;

%python
window_spec = Window.orderBy(F.desc("lifetime_value"))

result = (df.filter(df.order_date >= F.add_months(F.current_date(), -12))
          .groupBy("customer_id")
          .agg(F.countDistinct("order_id").alias("total_orders"),
               F.sum("order_amount").alias("lifetime_value"),
               F.avg("order_amount").alias("avg_order_value"),
               F.min("order_date").alias("first_order_date"),
               F.max("order_date").alias("last_order_date"),
               F.countDistinct("product_category").alias("categories_purchased"),
               F.sum(F.when(df.product_category == "Electronics", 1).otherwise(0)).alias("electronics_orders"))
          .withColumn("avg_days_between_orders", 
                     F.when(F.col("total_orders") > 1, 
                           F.datediff("last_order_date", "first_order_date") / (F.col("total_orders") - 1))
                      .otherwise(None))
          .withColumn("spending_rank", F.rank().over(window_spec))
          .filter(F.col("total_orders") >= 3)
          .orderBy(F.desc("lifetime_value")))

# Nvidia, Microsoft (Common Pattern: GPU/Software usage)

%sql
SELECT 
    gpu_model,
    application_name,
    COUNT(DISTINCT user_id) as active_users,
    AVG(gpu_utilization) as avg_gpu_usage,
    AVG(memory_utilization) as avg_memory_usage,
    AVG(temperature_c) as avg_temperature,
    SUM(render_time_seconds) as total_render_time,
    COUNT(DISTINCT render_job_id) as total_jobs,
    AVG(render_time_seconds) as avg_job_time,
    SUM(CASE WHEN render_status = 'completed' THEN 1 ELSE 0 END) as completed_jobs,
    ROUND(SUM(CASE WHEN render_status = 'completed' THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(DISTINCT render_job_id), 2) as success_rate
FROM gpu_usage_metrics
WHERE usage_timestamp >= SYSDATE - 30
GROUP BY gpu_model, application_name
HAVING COUNT(DISTINCT user_id) >= 5
ORDER BY total_render_time DESC;

%python
result = (df.filter(df.usage_timestamp >= F.current_date() - 30)
          .groupBy("gpu_model", "application_name")
          .agg(F.countDistinct("user_id").alias("active_users"),
               F.avg("gpu_utilization").alias("avg_gpu_usage"),
               F.avg("memory_utilization").alias("avg_memory_usage"),
               F.avg("temperature_c").alias("avg_temperature"),
               F.sum("render_time_seconds").alias("total_render_time"),
               F.countDistinct("render_job_id").alias("total_jobs"),
               F.avg("render_time_seconds").alias("avg_job_time"),
               F.sum(F.when(df.render_status == "completed", 1).otherwise(0)).alias("completed_jobs"))
          .withColumn("success_rate", 
                     (F.col("completed_jobs") * 100.0 / F.col("total_jobs")))
          .filter(F.col("active_users") >= 5)
          .orderBy(F.desc("total_render_time")))

# LinkedIn, Dropbox (Common Pattern: User engagement)

%sql
SELECT 
    user_id,
    COUNT(DISTINCT session_id) as total_sessions,
    AVG(session_duration) as avg_session_minutes,
    SUM(file_uploads) as total_uploads,
    SUM(file_downloads) as total_downloads,
    SUM(storage_used_mb) as total_storage_used,
    COUNT(DISTINCT connected_user_id) as total_connections,
    AVG(connection_strength) as avg_connection_strength,
    SUM(CASE WHEN is_premium_user = 1 THEN 1 ELSE 0 END) as premium_days,
    ROUND(SUM(CASE WHEN is_premium_user = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT activity_date), 2) as premium_usage_rate
FROM user_activities
WHERE activity_date >= ADD_MONTHS(SYSDATE, -3)
GROUP BY user_id
HAVING COUNT(DISTINCT session_id) >= 10
ORDER BY total_storage_used DESC;

%python
result = (df.filter(df.activity_date >= F.add_months(F.current_date(), -3))
          .groupBy("user_id")
          .agg(F.countDistinct("session_id").alias("total_sessions"),
               F.avg("session_duration").alias("avg_session_minutes"),
               F.sum("file_uploads").alias("total_uploads"),
               F.sum("file_downloads").alias("total_downloads"),
               F.sum("storage_used_mb").alias("total_storage_used"),
               F.countDistinct("connected_user_id").alias("total_connections"),
               F.avg("connection_strength").alias("avg_connection_strength"),
               F.sum(F.when(df.is_premium_user == 1, 1).otherwise(0)).alias("premium_days"),
               F.countDistinct("activity_date").alias("active_days"))
          .withColumn("premium_usage_rate", 
                     (F.col("premium_days") * 100.0 / F.col("active_days")))
          .filter(F.col("total_sessions") >= 10)
          .orderBy(F.desc("total_storage_used")))

# Expedia, Airbnb (Common Pattern: Travel booking patterns)

%sql
SELECT 
    destination_city,
    accommodation_type,
    COUNT(*) as total_bookings,
    AVG(booking_value) as avg_booking_value,
    AVG(DATEDIFF(check_out_date, check_in_date)) as avg_stay_length,
    SUM(CASE WHEN cancellation_date IS NULL THEN booking_value ELSE 0 END) as realized_revenue,
    COUNT(CASE WHEN cancellation_date IS NOT NULL THEN 1 END) as cancellations,
    ROUND(COUNT(CASE WHEN cancellation_date IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as cancellation_rate,
    AVG(review_score) as avg_review_score
FROM travel_bookings
WHERE booking_date >= ADD_MONTHS(SYSDATE, -6)
GROUP BY destination_city, accommodation_type
HAVING COUNT(*) >= 10
ORDER BY total_bookings DESC;

%python
result = (df.filter(df.booking_date >= F.add_months(F.current_date(), -6))
          .groupBy("destination_city", "accommodation_type")
          .agg(F.count("*").alias("total_bookings"),
               F.avg("booking_value").alias("avg_booking_value"),
               F.avg(F.datediff("check_out_date", "check_in_date")).alias("avg_stay_length"),
               F.sum(F.when(df.cancellation_date.isNull(), df.booking_value).otherwise(0)).alias("realized_revenue"),
               F.sum(F.when(df.cancellation_date.isNotNull(), 1).otherwise(0)).alias("cancellations"),
               F.avg("review_score").alias("avg_review_score"))
          .withColumn("cancellation_rate", 
                     (F.col("cancellations") * 100.0 / F.col("total_bookings")))
          .filter(F.col("total_bookings") >= 10)
          .orderBy(F.desc("total_bookings")))

# Amazon, Salesforce (Common Pattern: CRM analytics)

%sql
SELECT 
    sales_rep_id,
    COUNT(DISTINCT opportunity_id) as total_opportunities,
    SUM(deal_size) as total_deal_size,
    AVG(deal_size) as avg_deal_size,
    SUM(CASE WHEN stage = 'closed_won' THEN deal_size ELSE 0 END) as won_deal_size,
    COUNT(CASE WHEN stage = 'closed_won' THEN 1 END) as won_opportunities,
    ROUND(COUNT(CASE WHEN stage = 'closed_won' THEN 1 END) * 100.0 / 
          COUNT(DISTINCT opportunity_id), 2) as win_rate,
    AVG(DATEDIFF(close_date, created_date)) as avg_sales_cycle_days
FROM sales_opportunities
WHERE created_date >= ADD_MONTHS(SYSDATE, -12)
GROUP BY sales_rep_id
HAVING COUNT(DISTINCT opportunity_id) >= 5
ORDER BY win_rate DESC, total_deal_size DESC;

%python
result = (df.filter(df.created_date >= F.add_months(F.current_date(), -12))
          .groupBy("sales_rep_id")
          .agg(F.countDistinct("opportunity_id").alias("total_opportunities"),
               F.sum("deal_size").alias("total_deal_size"),
               F.avg("deal_size").alias("avg_deal_size"),
               F.sum(F.when(df.stage == "closed_won", df.deal_size).otherwise(0)).alias("won_deal_size"),
               F.sum(F.when(df.stage == "closed_won", 1).otherwise(0)).alias("won_opportunities"),
               F.avg(F.datediff("close_date", "created_date")).alias("avg_sales_cycle_days"))
          .withColumn("win_rate", 
                     (F.col("won_opportunities") * 100.0 / F.col("total_opportunities")))
          .filter(F.col("total_opportunities") >= 5)
          .orderBy(F.desc("win_rate"), F.desc("total_deal_size")))

# Google (Common Pattern: Search analytics)

%sql
SELECT 
    search_query,
    COUNT(DISTINCT session_id) as total_searches,
    AVG(CASE WHEN click_position IS NOT NULL THEN 1 ELSE 0 END) as click_through_rate,
    AVG(result_count) as avg_results,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY click_position) as median_click_position,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(CASE WHEN click_position <= 3 THEN 1 ELSE 0 END) as top3_clicks,
    ROUND(SUM(CASE WHEN click_position <= 3 THEN 1 ELSE 0 END) * 100.0 / 
          SUM(CASE WHEN click_position IS NOT NULL THEN 1 ELSE 0 END), 2) as top3_click_rate
FROM search_logs
WHERE search_timestamp >= SYSDATE - 7
GROUP BY search_query
HAVING COUNT(DISTINCT session_id) >= 100
ORDER BY total_searches DESC;

%python
result = (df.filter(df.search_timestamp >= F.current_date() - 7)
          .groupBy("search_query")
          .agg(F.countDistinct("session_id").alias("total_searches"),
               F.avg(F.when(df.click_position.isNotNull(), 1).otherwise(0)).alias("click_through_rate"),
               F.avg("result_count").alias("avg_results"),
               F.countDistinct("user_id").alias("unique_users"),
               F.sum(F.when(df.click_position <= 3, 1).otherwise(0)).alias("top3_clicks"),
               F.sum(F.when(df.click_position.isNotNull(), 1).otherwise(0)).alias("total_clicks"))
          .withColumn("top3_click_rate", 
                     (F.col("top3_clicks") * 100.0 / F.nullif(F.col("total_clicks"), 0)))
          .filter(F.col("total_searches") >= 100)
          .orderBy(F.desc("total_searches")))

# JP Morgan (Common Pattern: Financial portfolio analysis)

%sql
SELECT 
    portfolio_id,
    client_id,
    COUNT(DISTINCT asset_id) as unique_assets,
    SUM(current_value) as total_portfolio_value,
    SUM(initial_investment) as total_investment,
    (SUM(current_value) - SUM(initial_investment)) as total_gain_loss,
    ROUND((SUM(current_value) - SUM(initial_investment)) * 100.0 / SUM(initial_investment), 2) as return_percentage,
    AVG(volatility) as avg_volatility,
    SUM(CASE WHEN current_value > initial_investment THEN 1 ELSE 0 END) as profitable_assets,
    COUNT(*) as total_assets
FROM portfolio_holdings
WHERE as_of_date = TRUNC(SYSDATE)
GROUP BY portfolio_id, client_id
HAVING COUNT(*) >= 5
ORDER BY return_percentage DESC;

%python
result = (df.filter(df.as_of_date == F.current_date())
          .groupBy("portfolio_id", "client_id")
          .agg(F.countDistinct("asset_id").alias("unique_assets"),
               F.sum("current_value").alias("total_portfolio_value"),
               F.sum("initial_investment").alias("total_investment"),
               F.avg("volatility").alias("avg_volatility"),
               F.sum(F.when(df.current_value > df.initial_investment, 1).otherwise(0)).alias("profitable_assets"),
               F.count("*").alias("total_assets"))
          .withColumn("total_gain_loss", F.col("total_portfolio_value") - F.col("total_investment"))
          .withColumn("return_percentage", 
                     (F.col("total_gain_loss") * 100.0 / F.col("total_investment")))
          .filter(F.col("total_assets") >= 5)
          .orderBy(F.desc("return_percentage")))

# Uber (Common Pattern: Ride analytics)

%sql
SELECT 
    city,
    vehicle_type,
    COUNT(*) as total_rides,
    AVG(fare_amount) as avg_fare,
    AVG(distance_miles) as avg_distance,
    AVG(rating) as avg_rating,
    SUM(CASE WHEN surge_multiplier > 1.0 THEN 1 ELSE 0 END) as surge_rides,
    AVG(wait_time_seconds) as avg_wait_time,
    SUM(fare_amount * surge_multiplier) as total_revenue,
    ROUND(SUM(CASE WHEN surge_multiplier > 1.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as surge_rate
FROM rides
WHERE ride_date >= TRUNC(SYSDATE) - 7
GROUP BY city, vehicle_type
HAVING COUNT(*) >= 100
ORDER BY total_revenue DESC;

%python
result = (df.filter(df.ride_date >= F.current_date() - 7)
          .groupBy("city", "vehicle_type")
          .agg(F.count("*").alias("total_rides"),
               F.avg("fare_amount").alias("avg_fare"),
               F.avg("distance_miles").alias("avg_distance"),
               F.avg("rating").alias("avg_rating"),
               F.sum(F.when(df.surge_multiplier > 1.0, 1).otherwise(0)).alias("surge_rides"),
               F.avg("wait_time_seconds").alias("avg_wait_time"),
               F.sum(df.fare_amount * df.surge_multiplier).alias("total_revenue"))
          .withColumn("surge_rate", (F.col("surge_rides") * 100.0 / F.col("total_rides")))
          .filter(F.col("total_rides") >= 100)
          .orderBy(F.desc("total_revenue")))

# Amazon, Doordash (Common Pattern: Delivery performance)

%sql
SELECT 
    driver_id,
    COUNT(*) as total_deliveries,
    AVG(delivery_time - pickup_time) as avg_delivery_duration,
    AVG(CASE WHEN delivery_time - order_time <= 30 THEN 1 ELSE 0 END) as on_time_rate,
    SUM(order_value) as total_order_value,
    AVG(tip_amount) as avg_tip,
    SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) as five_star_ratings,
    ROUND(SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as five_star_rate,
    AVG(distance_miles) as avg_distance
FROM deliveries
WHERE order_date >= TRUNC(SYSDATE) - 30
GROUP BY driver_id
HAVING COUNT(*) >= 20
ORDER BY on_time_rate DESC, five_star_rate DESC;

%python
result = (df.filter(df.order_date >= F.current_date() - 30)
          .withColumn("delivery_duration", 
                     F.unix_timestamp("delivery_time") - F.unix_timestamp("pickup_time"))
          .withColumn("is_on_time", 
                     F.when(F.unix_timestamp("delivery_time") - F.unix_timestamp("order_time") <= 1800, 1)
                      .otherwise(0))
          .groupBy("driver_id")
          .agg(F.count("*").alias("total_deliveries"),
               F.avg("delivery_duration").alias("avg_delivery_duration"),
               F.avg("is_on_time").alias("on_time_rate"),
               F.sum("order_value").alias("total_order_value"),
               F.avg("tip_amount").alias("avg_tip"),
               F.sum(F.when(df.rating == 5, 1).otherwise(0)).alias("five_star_ratings"),
               F.avg("distance_miles").alias("avg_distance"))
          .withColumn("five_star_rate", (F.col("five_star_ratings") * 100.0 / F.col("total_deliveries")))
          .filter(F.col("total_deliveries") >= 20)
          .orderBy(F.desc("on_time_rate"), F.desc("five_star_rate")))

# Walmart (Common Pattern: Retail inventory optimization)

%sql
SELECT 
    store_id,
    department,
    AVG(daily_sales) as avg_daily_sales,
    AVG(inventory_level) as avg_inventory,
    AVG(inventory_level) / NULLIF(AVG(daily_sales), 0) as days_of_supply,
    SUM(CASE WHEN inventory_level = 0 THEN 1 ELSE 0 END) as out_of_stock_days,
    SUM(lost_sales) as total_lost_sales,
    ROUND(SUM(CASE WHEN inventory_level = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as out_of_stock_rate,
    CORR(daily_sales, inventory_level) as sales_inventory_correlation
FROM store_inventory
WHERE date >= TRUNC(SYSDATE) - 90
GROUP BY store_id, department
HAVING COUNT(*) >= 60
ORDER BY out_of_stock_rate DESC;

%python
result = (df.filter(df.date >= F.current_date() - 90)
          .groupBy("store_id", "department")
          .agg(F.avg("daily_sales").alias("avg_daily_sales"),
               F.avg("inventory_level").alias("avg_inventory"),
               F.sum(F.when(df.inventory_level == 0, 1).otherwise(0)).alias("out_of_stock_days"),
               F.sum("lost_sales").alias("total_lost_sales"),
               F.count("*").alias("total_days"),
               F.corr("daily_sales", "inventory_level").alias("sales_inventory_correlation"))
          .withColumn("days_of_supply", F.col("avg_inventory") / F.nullif(F.col("avg_daily_sales"), 0))
          .withColumn("out_of_stock_rate", (F.col("out_of_stock_days") * 100.0 / F.col("total_days")))
          .filter(F.col("total_days") >= 60)
          .orderBy(F.desc("out_of_stock_rate")))

# Apple, Microsoft (Common Pattern: Product adoption & usage)

%sql
SELECT 
    product_version,
    COUNT(DISTINCT user_id) as active_users,
    AVG(session_duration) as avg_session_minutes,
    SUM(feature_usage_count) as total_feature_uses,
    COUNT(DISTINCT feature_name) as unique_features_used,
    AVG(crash_count) as avg_crashes_per_session,
    SUM(CASE WHEN is_premium_user = 1 THEN 1 ELSE 0 END) as premium_users,
    ROUND(SUM(CASE WHEN is_premium_user = 1 THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(DISTINCT user_id), 2) as premium_conversion_rate,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY session_duration) as p95_session_duration
FROM product_usage
WHERE usage_date >= ADD_MONTHS(SYSDATE, -3)
GROUP BY product_version
HAVING COUNT(DISTINCT user_id) >= 1000
ORDER BY active_users DESC;

%python
result = (df.filter(df.usage_date >= F.add_months(F.current_date(), -3))
          .groupBy("product_version")
          .agg(F.countDistinct("user_id").alias("active_users"),
               F.avg("session_duration").alias("avg_session_minutes"),
               F.sum("feature_usage_count").alias("total_feature_uses"),
               F.countDistinct("feature_name").alias("unique_features_used"),
               F.avg("crash_count").alias("avg_crashes_per_session"),
               F.sum(F.when(df.is_premium_user == 1, 1).otherwise(0)).alias("premium_users"),
               F.expr("percentile_approx(session_duration, 0.95)").alias("p95_session_duration"))
          .withColumn("premium_conversion_rate", 
                     (F.col("premium_users") * 100.0 / F.col("active_users")))
          .filter(F.col("active_users") >= 1000)
          .orderBy(F.desc("active_users")))

# Microsoft (Common Pattern: Software usage analytics)

%sql
SELECT 
    product_name,
    version,
    COUNT(DISTINCT user_id) as active_users,
    AVG(session_duration_minutes) as avg_session_duration,
    SUM(feature_usage_count) as total_feature_usage,
    COUNT(DISTINCT feature_name) as unique_features_used,
    AVG(crash_count) as avg_crash_rate,
    SUM(CASE WHEN is_premium = 1 THEN 1 ELSE 0 END) as premium_users,
    ROUND(SUM(CASE WHEN is_premium = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT user_id), 2) as premium_conversion_rate
FROM software_usage
WHERE usage_date >= ADD_MONTHS(SYSDATE, -3)
GROUP BY product_name, version
HAVING COUNT(DISTINCT user_id) >= 100
ORDER BY active_users DESC;

%python
result = (df.filter(df.usage_date >= F.add_months(F.current_date(), -3))
          .groupBy("product_name", "version")
          .agg(F.countDistinct("user_id").alias("active_users"),
               F.avg("session_duration_minutes").alias("avg_session_duration"),
               F.sum("feature_usage_count").alias("total_feature_usage"),
               F.countDistinct("feature_name").alias("unique_features_used"),
               F.avg("crash_count").alias("avg_crash_rate"),
               F.sum(F.when(df.is_premium == 1, 1).otherwise(0)).alias("premium_users"))
          .withColumn("premium_conversion_rate", 
                     (F.col("premium_users") * 100.0 / F.col("active_users")))
          .filter(F.col("active_users") >= 100)
          .orderBy(F.desc("active_users")))

# Walmart, Paypal (Common Pattern: Payment processing analytics)

%sql
SELECT 
    payment_method,
    merchant_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_volume,
    AVG(amount) as avg_transaction_size,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_transactions,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_transactions,
    SUM(CASE WHEN is_chargeback = 1 THEN 1 ELSE 0 END) as chargebacks,
    ROUND(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate,
    ROUND(SUM(CASE WHEN is_chargeback = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as chargeback_rate
FROM payment_transactions
WHERE transaction_date >= TRUNC(SYSDATE) - 30
GROUP BY payment_method, merchant_id
HAVING COUNT(*) >= 100
ORDER BY total_volume DESC;

%python
result = (df.filter(df.transaction_date >= F.current_date() - 30)
          .groupBy("payment_method", "merchant_id")
          .agg(F.count("*").alias("transaction_count"),
               F.sum("amount").alias("total_volume"),
               F.avg("amount").alias("avg_transaction_size"),
               F.sum(F.when(df.status == "success", 1).otherwise(0)).alias("successful_transactions"),
               F.sum(F.when(df.status == "failed", 1).otherwise(0)).alias("failed_transactions"),
               F.sum(F.when(df.is_chargeback == 1, 1).otherwise(0)).alias("chargebacks"))
          .withColumn("success_rate", (F.col("successful_transactions") * 100.0 / F.col("transaction_count")))
          .withColumn("chargeback_rate", (F.col("chargebacks") * 100.0 / F.col("transaction_count")))
          .filter(F.col("transaction_count") >= 100)
          .orderBy(F.desc("total_volume")))

# Oracle (Common Pattern: Database performance metrics)

%sql
SELECT 
    database_name,
    tablespace_name,
    AVG(tablespace_used_percent) as avg_used_percent,
    MAX(tablespace_used_percent) as max_used_percent,
    AVG(active_sessions) as avg_active_sessions,
    MAX(active_sessions) as peak_active_sessions,
    AVG(cpu_utilization) as avg_cpu_usage,
    SUM(CASE WHEN tablespace_used_percent > 90 THEN 1 ELSE 0 END) as high_usage_events,
    SUM(CASE WHEN active_sessions > 50 THEN 1 ELSE 0 END) as high_session_events,
    COUNT(DISTINCT wait_event) as unique_wait_events
FROM database_metrics
WHERE metric_time >= SYSDATE - 7
GROUP BY database_name, tablespace_name
HAVING COUNT(*) >= 100
ORDER BY avg_used_percent DESC;

%python
result = (df.filter(df.metric_time >= F.current_date() - 7)
          .groupBy("database_name", "tablespace_name")
          .agg(F.avg("tablespace_used_percent").alias("avg_used_percent"),
               F.max("tablespace_used_percent").alias("max_used_percent"),
               F.avg("active_sessions").alias("avg_active_sessions"),
               F.max("active_sessions").alias("peak_active_sessions"),
               F.avg("cpu_utilization").alias("avg_cpu_usage"),
               F.sum(F.when(df.tablespace_used_percent > 90, 1).otherwise(0)).alias("high_usage_events"),
               F.sum(F.when(df.active_sessions > 50, 1).otherwise(0)).alias("high_session_events"),
               F.countDistinct("wait_event").alias("unique_wait_events"))
          .filter(F.count("*") >= 100)
          .orderBy(F.desc("avg_used_percent")))

# Amazon (Common Pattern: Supply chain optimization)

%sql
SELECT 
    warehouse_id,
    product_category,
    AVG(daily_demand) as avg_daily_demand,
    AVG(current_inventory) as avg_inventory_level,
    AVG(lead_time_days) as avg_lead_time,
    SUM(CASE WHEN current_inventory = 0 THEN 1 ELSE 0 END) as stockout_days,
    ROUND(SUM(CASE WHEN current_inventory = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stockout_rate,
    AVG(current_inventory) / NULLIF(AVG(daily_demand), 0) as days_of_supply,
    SUM(lost_sales) as total_lost_sales
FROM inventory_data
WHERE date >= TRUNC(SYSDATE) - 90
GROUP BY warehouse_id, product_category
HAVING COUNT(*) >= 60
ORDER BY stockout_rate DESC;

%python
result = (df.filter(df.date >= F.current_date() - 90)
          .groupBy("warehouse_id", "product_category")
          .agg(F.avg("daily_demand").alias("avg_daily_demand"),
               F.avg("current_inventory").alias("avg_inventory_level"),
               F.avg("lead_time_days").alias("avg_lead_time"),
               F.sum(F.when(df.current_inventory == 0, 1).otherwise(0)).alias("stockout_days"),
               F.count("*").alias("total_days"),
               F.sum("lost_sales").alias("total_lost_sales"))
          .withColumn("stockout_rate", (F.col("stockout_days") * 100.0 / F.col("total_days")))
          .withColumn("days_of_supply", F.col("avg_inventory_level") / F.nullif(F.col("avg_daily_demand"), 0))
          .filter(F.col("total_days") >= 60)
          .orderBy(F.desc("stockout_rate")))

# American Express (Common Pattern: Credit card spending patterns)

%sql
SELECT 
    cardholder_id,
    merchant_category,
    COUNT(*) as transaction_count,
    SUM(amount) as total_spent,
    AVG(amount) as avg_transaction_size,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction,
    COUNT(DISTINCT merchant_id) as unique_merchants,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as fraud_rate,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) as p95_transaction_size
FROM card_transactions
WHERE transaction_date >= ADD_MONTHS(SYSDATE, -6)
GROUP BY cardholder_id, merchant_category
HAVING COUNT(*) >= 3
ORDER BY total_spent DESC;

%python
result = (df.filter(df.transaction_date >= F.add_months(F.current_date(), -6))
          .groupBy("cardholder_id", "merchant_category")
          .agg(F.count("*").alias("transaction_count"),
               F.sum("amount").alias("total_spent"),
               F.avg("amount").alias("avg_transaction_size"),
               F.min("transaction_date").alias("first_transaction"),
               F.max("transaction_date").alias("last_transaction"),
               F.countDistinct("merchant_id").alias("unique_merchants"),
               F.sum(F.when(df.is_fraud == 1, 1).otherwise(0)).alias("fraud_count"),
               F.expr("percentile_approx(amount, 0.95)").alias("p95_transaction_size"))
          .withColumn("fraud_rate", (F.col("fraud_count") * 100.0 / F.col("transaction_count")))
          .filter(F.col("transaction_count") >= 3)
          .orderBy(F.desc("total_spent")))

# LinkedIn (Common Pattern: Professional network analytics)

%sql
SELECT 
    user_id,
    COUNT(DISTINCT connection_id) as total_connections,
    COUNT(DISTINCT company) as unique_companies,
    COUNT(DISTINCT industry) as unique_industries,
    AVG(connection_strength) as avg_connection_strength,
    SUM(CASE WHEN is_premium_connection = 1 THEN 1 ELSE 0 END) as premium_connections,
    COUNT(DISTINCT skill) as unique_skills,
    ROUND(SUM(CASE WHEN is_premium_connection = 1 THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(DISTINCT connection_id), 2) as premium_connection_rate
FROM user_connections
WHERE connection_date >= ADD_MONTHS(SYSDATE, -12)
GROUP BY user_id
HAVING COUNT(DISTINCT connection_id) >= 50
ORDER BY total_connections DESC;

# python
result = (df.filter(df.connection_date >= F.add_months(F.current_date(), -12))
          .groupBy("user_id")
          .agg(F.countDistinct("connection_id").alias("total_connections"),
               F.countDistinct("company").alias("unique_companies"),
               F.countDistinct("industry").alias("unique_industries"),
               F.avg("connection_strength").alias("avg_connection_strength"),
               F.sum(F.when(df.is_premium_connection == 1, 1).otherwise(0)).alias("premium_connections"),
               F.countDistinct("skill").alias("unique_skills"))
          .withColumn("premium_connection_rate", 
                     (F.col("premium_connections") * 100.0 / F.col("total_connections")))
          .filter(F.col("total_connections") >= 50)
          .orderBy(F.desc("total_connections")))

#  GoldmanSachs, Deloitte (Common Pattern: Financial transaction analysis)

%sql
SELECT 
    account_id,
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_transaction_size,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction,
    SUM(CASE WHEN amount > 10000 THEN 1 ELSE 0 END) as large_transactions,
    ROUND(SUM(CASE WHEN amount > 10000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as large_transaction_percentage,
    STDDEV(amount) as amount_volatility
FROM transactions
WHERE transaction_date >= ADD_MONTHS(SYSDATE, -3)
GROUP BY account_id, transaction_type
HAVING COUNT(*) >= 5
ORDER BY total_amount DESC;

%python
result = (df.filter(df.transaction_date >= F.add_months(F.current_date(), -3))
          .groupBy("account_id", "transaction_type")
          .agg(F.count("*").alias("transaction_count"),
               F.sum("amount").alias("total_amount"),
               F.avg("amount").alias("avg_transaction_size"),
               F.min("transaction_date").alias("first_transaction"),
               F.max("transaction_date").alias("last_transaction"),
               F.sum(F.when(df.amount > 10000, 1).otherwise(0)).alias("large_transactions"),
               F.stddev("amount").alias("amount_volatility"))
          .withColumn("large_transaction_percentage", 
                     (F.col("large_transactions") * 100.0 / F.col("transaction_count")))
          .filter(F.col("transaction_count") >= 5)
          .orderBy(F.desc("total_amount")))

# Meta (Common Pattern: Social media engagement)

%sql
SELECT 
    user_id,
    COUNT(DISTINCT post_id) as total_posts,
    SUM(like_count) as total_likes,
    SUM(share_count) as total_shares,
    SUM(comment_count) as total_comments,
    AVG(like_count) as avg_likes_per_post,
    COUNT(DISTINCT follower_id) as total_followers,
    ROUND(SUM(like_count + share_count * 2 + comment_count * 1.5) / NULLIF(COUNT(DISTINCT post_id), 0), 2) as engagement_score,
    RANK() OVER (ORDER BY SUM(like_count + share_count * 2 + comment_count * 1.5) DESC) as engagement_rank
FROM user_engagement
WHERE activity_date >= SYSDATE - 30
GROUP BY user_id
HAVING COUNT(DISTINCT post_id) >= 5
ORDER BY engagement_score DESC;

%python
window_spec = Window.orderBy(F.desc("engagement_score"))

result = (df.filter(df.activity_date >= F.current_date() - 30)
          .groupBy("user_id")
          .agg(F.countDistinct("post_id").alias("total_posts"),
               F.sum("like_count").alias("total_likes"),
               F.sum("share_count").alias("total_shares"),
               F.sum("comment_count").alias("total_comments"),
               F.avg("like_count").alias("avg_likes_per_post"),
               F.countDistinct("follower_id").alias("total_followers"))
          .withColumn("engagement_score", 
                     (F.col("total_likes") + F.col("total_shares") * 2 + F.col("total_comments") * 1.5) / 
                     F.nullif(F.col("total_posts"), 0))
          .withColumn("engagement_rank", F.rank().over(window_spec))
          .filter(F.col("total_posts") >= 5)
          .orderBy(F.desc("engagement_score")))

# Cisco (Common Pattern: Network device monitoring)

%sql
SELECT 
    device_id,
    device_type,
    AVG(cpu_utilization) as avg_cpu_usage,
    AVG(memory_utilization) as avg_memory_usage,
    AVG(network_throughput) as avg_network_throughput,
    SUM(CASE WHEN cpu_utilization > 90 THEN 1 ELSE 0 END) as high_cpu_events,
    SUM(CASE WHEN memory_utilization > 85 THEN 1 ELSE 0 END) as high_memory_events,
    MAX(last_heartbeat) as last_online,
    CASE 
        WHEN MAX(last_heartbeat) < SYSDATE - INTERVAL '5' MINUTE THEN 'Offline'
        ELSE 'Online'
    END as status
FROM device_metrics
WHERE metric_timestamp >= SYSDATE - 1
GROUP BY device_id, device_type
HAVING COUNT(*) >= 10
ORDER BY avg_cpu_usage DESC;

%python
result = (df.filter(df.metric_timestamp >= F.current_date() - 1)
          .groupBy("device_id", "device_type")
          .agg(F.avg("cpu_utilization").alias("avg_cpu_usage"),
               F.avg("memory_utilization").alias("avg_memory_usage"),
               F.avg("network_throughput").alias("avg_network_throughput"),
               F.sum(F.when(df.cpu_utilization > 90, 1).otherwise(0)).alias("high_cpu_events"),
               F.sum(F.when(df.memory_utilization > 85, 1).otherwise(0)).alias("high_memory_events"),
               F.max("last_heartbeat").alias("last_online"))
          .withColumn("status",
                     F.when(F.col("last_online") < F.current_timestamp() - F.expr("INTERVAL 5 MINUTES"), "Offline")
                      .otherwise("Online"))
          .filter(F.count("*") >= 10)
          .orderBy(F.desc("avg_cpu_usage")))

# Amazon (Common Pattern: E-commerce customer analytics)

%sql
SELECT 
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(order_amount) as total_spent,
    AVG(order_amount) as avg_order_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    COUNT(DISTINCT product_category) as categories_purchased,
    ROUND((MAX(order_date) - MIN(order_date)) / NULLIF(COUNT(DISTINCT order_id) - 1, 0), 2) as avg_days_between_orders,
    CASE 
        WHEN COUNT(DISTINCT order_id) >= 10 THEN 'Platinum'
        WHEN COUNT(DISTINCT order_id) >= 5 THEN 'Gold'
        WHEN COUNT(DISTINCT order_id) >= 2 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_tier
FROM orders
WHERE order_date >= ADD_MONTHS(SYSDATE, -12)
GROUP BY customer_id
HAVING COUNT(DISTINCT order_id) >= 1
ORDER BY total_spent DESC;

%python
result = (df.filter(df.order_date >= F.add_months(F.current_date(), -12))
          .groupBy("customer_id")
          .agg(F.countDistinct("order_id").alias("total_orders"),
               F.sum("order_amount").alias("total_spent"),
               F.avg("order_amount").alias("avg_order_value"),
               F.min("order_date").alias("first_order_date"),
               F.max("order_date").alias("last_order_date"),
               F.countDistinct("product_category").alias("categories_purchased"))
          .withColumn("avg_days_between_orders", 
                     F.when(F.col("total_orders") > 1, 
                           F.datediff("last_order_date", "first_order_date") / (F.col("total_orders") - 1))
                      .otherwise(None))
          .withColumn("customer_tier",
                     F.when(F.col("total_orders") >= 10, "Platinum")
                      .when(F.col("total_orders") >= 5, "Gold")
                      .when(F.col("total_orders") >= 2, "Silver")
                      .otherwise("Bronze"))
          .orderBy(F.desc("total_spent")))

# Spotify (Common Pattern: Music streaming analytics)

%sql
SELECT 
    artist_id,
    COUNT(DISTINCT track_id) as total_tracks,
    SUM(stream_count) as total_streams,
    AVG(stream_duration_seconds) as avg_stream_duration,
    COUNT(DISTINCT user_id) as unique_listeners,
    SUM(CASE WHEN stream_duration_seconds >= track_duration * 0.75 THEN 1 ELSE 0 END) as completed_streams,
    ROUND(SUM(CASE WHEN stream_duration_seconds >= track_duration * 0.75 THEN 1 ELSE 0 END) * 100.0 / 
          SUM(stream_count), 2) as completion_rate,
    SUM(revenue) as total_revenue
FROM streaming_data
WHERE stream_date >= SYSDATE - 30
GROUP BY artist_id
HAVING SUM(stream_count) >= 1000
ORDER BY total_streams DESC;

%python
result = (df.filter(df.stream_date >= F.current_date() - 30)
          .groupBy("artist_id")
          .agg(F.countDistinct("track_id").alias("total_tracks"),
               F.sum("stream_count").alias("total_streams"),
               F.avg("stream_duration_seconds").alias("avg_stream_duration"),
               F.countDistinct("user_id").alias("unique_listeners"),
               F.sum(F.when(df.stream_duration_seconds >= df.track_duration * 0.75, 1).otherwise(0)).alias("completed_streams"),
               F.sum("revenue").alias("total_revenue"))
          .withColumn("completion_rate", 
                     (F.col("completed_streams") * 100.0 / F.col("total_streams")))
          .filter(F.col("total_streams") >= 1000)
          .orderBy(F.desc("total_streams")))

# Accenture (Common Pattern: Consulting project metrics)

%sql
SELECT 
    project_id,
    client_id,
    SUM(billable_hours) as total_billable_hours,
    SUM(billed_amount) as total_revenue,
    AVG(employee_hourly_rate) as avg_hourly_rate,
    COUNT(DISTINCT consultant_id) as unique_consultants,
    AVG(client_satisfaction_score) as avg_satisfaction,
    SUM(CASE WHEN milestone_status = 'completed_early' THEN 1 ELSE 0 END) as early_completions,
    ROUND(SUM(CASE WHEN milestone_status = 'completed_early' THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(*), 2) as early_completion_rate
FROM project_milestones
WHERE milestone_date >= ADD_MONTHS(SYSDATE, -6)
GROUP BY project_id, client_id
HAVING COUNT(*) >= 3
ORDER BY total_revenue DESC;

%python
result = (df.filter(df.milestone_date >= F.add_months(F.current_date(), -6))
          .groupBy("project_id", "client_id")
          .agg(F.sum("billable_hours").alias("total_billable_hours"),
               F.sum("billed_amount").alias("total_revenue"),
               F.avg("employee_hourly_rate").alias("avg_hourly_rate"),
               F.countDistinct("consultant_id").alias("unique_consultants"),
               F.avg("client_satisfaction_score").alias("avg_satisfaction"),
               F.sum(F.when(df.milestone_status == "completed_early", 1).otherwise(0)).alias("early_completions"),
               F.count("*").alias("total_milestones"))
          .withColumn("early_completion_rate", 
                     (F.col("early_completions") * 100.0 / F.col("total_milestones")))
          .filter(F.col("total_milestones") >= 3)
          .orderBy(F.desc("total_revenue")))

# Google (Common Pattern: Ad performance analytics)

%sql
SELECT 
    ad_campaign_id,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) as impressions,
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
    SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) as conversions,
    ROUND(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END), 0), 2) as ctr,
    ROUND(SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END), 0), 2) as conversion_rate,
    SUM(revenue) as total_revenue
FROM ad_events
WHERE event_date >= SYSDATE - 30
GROUP BY ad_campaign_id
HAVING SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) >= 1000
ORDER BY conversion_rate DESC;

%python
result = (df.filter(df.event_date >= F.current_date() - 30)
          .groupBy("ad_campaign_id")
          .agg(F.countDistinct("user_id").alias("unique_users"),
               F.sum(F.when(df.event_type == "impression", 1).otherwise(0)).alias("impressions"),
               F.sum(F.when(df.event_type == "click", 1).otherwise(0)).alias("clicks"),
               F.sum(F.when(df.event_type == "conversion", 1).otherwise(0)).alias("conversions"),
               F.sum("revenue").alias("total_revenue"))
          .withColumn("ctr", (F.col("clicks") * 100.0 / F.nullif(F.col("impressions"), 0)))
          .withColumn("conversion_rate", (F.col("conversions") * 100.0 / F.nullif(F.col("clicks"), 0)))
          .filter(F.col("impressions") >= 1000)
          .orderBy(F.desc("conversion_rate")))

# Google, Airbnb (Common Pattern: Search ranking analytics)

%sql
SELECT 
    search_query,
    AVG(CASE WHEN click_position <= 3 THEN 1 ELSE 0 END) as top3_ctr,
    AVG(CASE WHEN click_position <= 10 THEN 1 ELSE 0 END) as top10_ctr,
    COUNT(DISTINCT session_id) as total_searches,
    AVG(result_count) as avg_results,
    SUM(CASE WHEN click_position IS NULL THEN 1 ELSE 0 END) as no_click_searches,
    ROUND(SUM(CASE WHEN click_position IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(DISTINCT session_id), 2) as overall_ctr
FROM search_logs
WHERE search_timestamp >= SYSDATE - 7
GROUP BY search_query
HAVING COUNT(DISTINCT session_id) >= 100
ORDER BY top3_ctr DESC;

%python
result = (df.filter(df.search_timestamp >= F.current_date() - 7)
          .groupBy("search_query")
          .agg(F.avg(F.when(df.click_position <= 3, 1).otherwise(0)).alias("top3_ctr"),
               F.avg(F.when(df.click_position <= 10, 1).otherwise(0)).alias("top10_ctr"),
               F.countDistinct("session_id").alias("total_searches"),
               F.avg("result_count").alias("avg_results"),
               F.sum(F.when(df.click_position.isNull(), 1).otherwise(0)).alias("no_click_searches"),
               F.sum(F.when(df.click_position.isNotNull(), 1).otherwise(0)).alias("click_searches"))
          .withColumn("overall_ctr", (F.col("click_searches") * 100.0 / F.col("total_searches")))
          .filter(F.col("total_searches") >= 100)
          .orderBy(F.desc("top3_ctr")))

# X (Common Pattern: Social media metrics)

%sql
SELECT 
    user_id,
    COUNT(*) as total_tweets,
    SUM(like_count) as total_likes,
    SUM(retweet_count) as total_retweets,
    SUM(reply_count) as total_replies,
    AVG(like_count) as avg_likes_per_tweet,
    COUNT(DISTINCT hashtag) as unique_hashtags,
    ROUND(SUM(retweet_count) * 100.0 / NULLIF(SUM(like_count), 0), 2) as engagement_rate
FROM tweets
WHERE tweet_date >= SYSDATE - 30
GROUP BY user_id
HAVING COUNT(*) >= 5
ORDER BY engagement_rate DESC;

%python
result = (df.filter(df.tweet_date >= F.current_date() - 30)
          .groupBy("user_id")
          .agg(F.count("*").alias("total_tweets"),
               F.sum("like_count").alias("total_likes"),
               F.sum("retweet_count").alias("total_retweets"),
               F.sum("reply_count").alias("total_replies"),
               F.avg("like_count").alias("avg_likes_per_tweet"),
               F.countDistinct("hashtag").alias("unique_hashtags"))
          .withColumn("engagement_rate", 
                     (F.col("total_retweets") * 100.0 / F.nullif(F.col("total_likes"), 0)))
          .filter(F.col("total_tweets") >= 5)
          .orderBy(F.desc("engagement_rate")))

# Uber (Common Pattern: Driver performance analytics)

%sql
SELECT 
    driver_id,
    COUNT(*) as total_rides,
    AVG(rating) as avg_rating,
    SUM(fare_amount) as total_earnings,
    AVG(fare_amount) as avg_fare,
    SUM(CASE WHEN surge_multiplier > 1.0 THEN 1 ELSE 0 END) as surge_rides,
    AVG(DATEDIFF(dropoff_time, pickup_time)) as avg_trip_duration,
    ROUND(SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as five_star_rate
FROM rides
WHERE pickup_time >= TRUNC(SYSDATE) - 7
GROUP BY driver_id
HAVING COUNT(*) >= 10
ORDER BY total_earnings DESC;

%python
result = (df.filter(df.pickup_time >= F.current_date() - 7)
          .groupBy("driver_id")
          .agg(F.count("*").alias("total_rides"),
               F.avg("rating").alias("avg_rating"),
               F.sum("fare_amount").alias("total_earnings"),
               F.avg("fare_amount").alias("avg_fare"),
               F.sum(F.when(df.surge_multiplier > 1.0, 1).otherwise(0)).alias("surge_rides"),
               F.avg(F.unix_timestamp("dropoff_time") - F.unix_timestamp("pickup_time")).alias("avg_trip_duration_seconds"),
               F.sum(F.when(df.rating == 5, 1).otherwise(0)).alias("five_star_rides"))
          .withColumn("five_star_rate", (F.col("five_star_rides") * 100.0 / F.col("total_rides")))
          .filter(F.col("total_rides") >= 10)
          .orderBy(F.desc("total_earnings")))

# Netflix (Common Pattern: Content engagement analytics)

%sql
SELECT 
    content_id,
    content_type,
    COUNT(DISTINCT user_id) as unique_viewers,
    SUM(watch_duration_minutes) as total_watch_time,
    AVG(watch_duration_minutes) as avg_watch_time,
    COUNT(*) as total_views,
    SUM(CASE WHEN watch_duration_minutes >= content_duration * 0.8 THEN 1 ELSE 0 END) as completion_views,
    ROUND(SUM(CASE WHEN watch_duration_minutes >= content_duration * 0.8 THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(*), 2) as completion_rate
FROM viewing_sessions
WHERE watch_date >= SYSDATE - 30
GROUP BY content_id, content_type
HAVING COUNT(*) >= 100
ORDER BY total_watch_time DESC;

%python
result = (df.filter(df.watch_date >= F.current_date() - 30)
          .groupBy("content_id", "content_type")
          .agg(F.countDistinct("user_id").alias("unique_viewers"),
               F.sum("watch_duration_minutes").alias("total_watch_time"),
               F.avg("watch_duration_minutes").alias("avg_watch_time"),
               F.count("*").alias("total_views"),
               F.sum(F.when(df.watch_duration_minutes >= df.content_duration * 0.8, 1).otherwise(0)).alias("completion_views"))
          .withColumn("completion_rate", (F.col("completion_views") * 100.0 / F.col("total_views")))
          .filter(F.col("total_views") >= 100)
          .orderBy(F.desc("total_watch_time")))

# Airbnb (Common Pattern: Host performance metrics)

%sql
SELECT 
    host_id,
    COUNT(DISTINCT listing_id) as total_listings,
    AVG(price_per_night) as avg_price,
    AVG(review_score) as avg_rating,
    COUNT(*) as total_bookings,
    SUM(booking_amount) as total_revenue,
    AVG(DATEDIFF(check_out_date, check_in_date)) as avg_stay_length,
    ROUND(SUM(CASE WHEN cancellation_date IS NULL THEN booking_amount ELSE 0 END) * 100.0 / 
          SUM(booking_amount), 2) as completion_rate
FROM bookings
WHERE check_in_date >= ADD_MONTHS(SYSDATE, -12)
GROUP BY host_id
HAVING COUNT(*) >= 5
ORDER BY total_revenue DESC;

%python
result = (df.filter(df.check_in_date >= F.add_months(F.current_date(), -12))
          .groupBy("host_id")
          .agg(F.countDistinct("listing_id").alias("total_listings"),
               F.avg("price_per_night").alias("avg_price"),
               F.avg("review_score").alias("avg_rating"),
               F.count("*").alias("total_bookings"),
               F.sum("booking_amount").alias("total_revenue"),
               F.avg(F.datediff("check_out_date", "check_in_date")).alias("avg_stay_length"),
               (F.sum(F.when(df.cancellation_date.isNull(), df.booking_amount).otherwise(0)) * 100.0 /
                F.sum("booking_amount")).alias("completion_rate"))
          .filter(F.col("total_bookings") >= 5)
          .orderBy(F.desc("total_revenue")))

# Amazon (Common Pattern: Customer behavior analysis)

%sql
SELECT 
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(order_amount) as lifetime_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    AVG(DATEDIFF(order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date))) as avg_days_between_orders,
    COUNT(DISTINCT product_category) as unique_categories,
    CASE 
        WHEN COUNT(DISTINCT order_id) >= 5 AND SUM(order_amount) >= 500 THEN 'VIP'
        WHEN COUNT(DISTINCT order_id) >= 3 THEN 'Regular'
        ELSE 'New'
    END as customer_segment
FROM orders
GROUP BY customer_id
HAVING COUNT(DISTINCT order_id) >= 1;

%python
window_spec = Window.partitionBy("customer_id").orderBy("order_date")

result = (df.withColumn("prev_order_date", F.lag("order_date").over(window_spec))
          .withColumn("days_between_orders", F.datediff("order_date", "prev_order_date"))
          .groupBy("customer_id")
          .agg(F.countDistinct("order_id").alias("total_orders"),
               F.sum("order_amount").alias("lifetime_value"),
               F.min("order_date").alias("first_order_date"),
               F.max("order_date").alias("last_order_date"),
               F.avg("days_between_orders").alias("avg_days_between_orders"),
               F.countDistinct("product_category").alias("unique_categories"))
          .withColumn("customer_segment", 
                     F.when((F.col("total_orders") >= 5) & (F.col("lifetime_value") >= 500), "VIP")
                      .when(F.col("total_orders") >= 3, "Regular")
                      .otherwise("New")))

# Walmart (Common Pattern: Retail inventory optimization)

%sql
SELECT 
    store_id,
    product_category,
    AVG(daily_sales) as avg_daily_sales,
    AVG(current_inventory) as avg_inventory,
    AVG(current_inventory) / NULLIF(AVG(daily_sales), 0) as days_of_supply,
    SUM(CASE WHEN current_inventory = 0 THEN 1 ELSE 0 END) as out_of_stock_days,
    ROUND(SUM(CASE WHEN current_inventory = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as out_of_stock_rate
FROM store_inventory
WHERE inventory_date >= TRUNC(SYSDATE) - 90
GROUP BY store_id, product_category
HAVING COUNT(*) >= 60
ORDER BY out_of_stock_rate DESC;

%python
result = (df.filter(df.inventory_date >= F.current_date() - 90)
          .groupBy("store_id", "product_category")
          .agg(F.avg("daily_sales").alias("avg_daily_sales"),
               F.avg("current_inventory").alias("avg_inventory"),
               F.sum(F.when(df.current_inventory == 0, 1).otherwise(0)).alias("out_of_stock_days"),
               F.count("*").alias("total_days"))
          .withColumn("days_of_supply", F.col("avg_inventory") / F.nullif(F.col("avg_daily_sales"), 0))
          .withColumn("out_of_stock_rate", (F.col("out_of_stock_days") * 100.0 / F.col("total_days")))
          .filter(F.col("total_days") >= 60)
          .orderBy(F.desc("out_of_stock_rate")))

# Meta (Common Pattern: Social network analysis)

%sql
SELECT 
    user_id,
    COUNT(DISTINCT friend_id) as friend_count,
    COUNT(DISTINCT post_id) as post_count,
    AVG(like_count) as avg_likes_per_post,
    SUM(comment_count) as total_comments,
    COUNT(DISTINCT group_id) as groups_joined,
    RANK() OVER (ORDER BY COUNT(DISTINCT friend_id) DESC) as social_rank
FROM user_activities
WHERE activity_date >= SYSDATE - 90
GROUP BY user_id
HAVING COUNT(DISTINCT friend_id) >= 10
   AND COUNT(DISTINCT post_id) >= 5;

%python
window_spec = Window.orderBy(F.desc("friend_count"))

result = (df.filter(df.activity_date >= F.current_date() - 90)
          .groupBy("user_id")
          .agg(F.countDistinct("friend_id").alias("friend_count"),
               F.countDistinct("post_id").alias("post_count"),
               F.avg("like_count").alias("avg_likes_per_post"),
               F.sum("comment_count").alias("total_comments"),
               F.countDistinct("group_id").alias("groups_joined"))
          .withColumn("social_rank", F.rank().over(window_spec))
          .filter((F.col("friend_count") >= 10) & (F.col("post_count") >= 5)))

# Microsoft (Common Pattern: Software license usage)

%sql
SELECT 
    product_name,
    license_type,
    COUNT(DISTINCT user_id) as active_users,
    COUNT(DISTINCT device_id) as active_devices,
    AVG(session_duration) as avg_session_duration,
    SUM(CASE WHEN usage_hours > 8 THEN 1 ELSE 0 END) as heavy_users,
    MAX(last_used_date) as last_usage_date
FROM software_usage
WHERE usage_date >= ADD_MONTHS(SYSDATE, -3)
GROUP BY product_name, license_type
HAVING COUNT(DISTINCT user_id) >= 5
ORDER BY active_users DESC;

%python
result = (df.filter(df.usage_date >= F.add_months(F.current_date(), -3))
          .groupBy("product_name", "license_type")
          .agg(F.countDistinct("user_id").alias("active_users"),
               F.countDistinct("device_id").alias("active_devices"),
               F.avg("session_duration").alias("avg_session_duration"),
               F.sum(F.when(df.usage_hours > 8, 1).otherwise(0)).alias("heavy_users"),
               F.max("last_used_date").alias("last_usage_date"))
          .filter(F.col("active_users") >= 5)
          .orderBy(F.desc("active_users")))

# Apple (Common Pattern: App store analytics)

%sql
SELECT 
    app_category,
    AVG(rating) as avg_rating,
    COUNT(*) as total_apps,
    SUM(downloads) as total_downloads,
    AVG(price) as avg_price,
    SUM(CASE WHEN rating >= 4.5 THEN 1 ELSE 0 END) as highly_rated_apps,
    ROUND(SUM(CASE WHEN rating >= 4.5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as highly_rated_percentage
FROM app_store
WHERE release_date >= ADD_MONTHS(SYSDATE, -24)
GROUP BY app_category
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC;

%python
result = (df.filter(df.release_date >= F.add_months(F.current_date(), -24))
          .groupBy("app_category")
          .agg(F.avg("rating").alias("avg_rating"),
               F.count("*").alias("total_apps"),
               F.sum("downloads").alias("total_downloads"),
               F.avg("price").alias("avg_price"),
               F.sum(F.when(df.rating >= 4.5, 1).otherwise(0)).alias("highly_rated_apps"))
          .withColumn("highly_rated_percentage", 
                     (F.col("highly_rated_apps") * 100.0 / F.col("total_apps")))
          .filter(F.col("total_apps") >= 10)
          .orderBy(F.desc("avg_rating")))

# Amazon (Common Pattern: Inventory turnover)

%sql
SELECT 
    product_id,
    warehouse_id,
    SUM(quantity_sold) as total_sold,
    AVG(inventory_level) as avg_inventory,
    SUM(quantity_sold) / NULLIF(AVG(inventory_level), 0) as inventory_turnover,
    COUNT(DISTINCT CASE WHEN quantity_sold > 0 THEN sales_date END) as selling_days
FROM inventory_sales
WHERE sales_date >= ADD_MONTHS(SYSDATE, -3)
GROUP BY product_id, warehouse_id
HAVING SUM(quantity_sold) > 0
ORDER BY inventory_turnover DESC;

%python
result = (df.filter(df.sales_date >= F.add_months(F.current_date(), -3))
          .groupBy("product_id", "warehouse_id")
          .agg(F.sum("quantity_sold").alias("total_sold"),
               F.avg("inventory_level").alias("avg_inventory"),
               F.countDistinct(F.when(df.quantity_sold > 0, df.sales_date)).alias("selling_days"))
          .withColumn("inventory_turnover", 
                     F.col("total_sold") / F.nullif(F.col("avg_inventory"), 0))
          .filter(F.col("total_sold") > 0)
          .orderBy(F.desc("inventory_turnover")))

# Visa (Common Pattern: Payment transaction analysis)

%sql
SELECT 
    merchant_category,
    COUNT(*) as transaction_count,
    SUM(transaction_amount) as total_volume,
    AVG(transaction_amount) as avg_transaction_size,
    COUNT(DISTINCT card_id) as unique_cards,
    SUM(CASE WHEN transaction_status = 'declined' THEN 1 ELSE 0 END) as declined_transactions,
    ROUND(SUM(CASE WHEN transaction_status = 'declined' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as decline_rate
FROM transactions
WHERE transaction_date >= TRUNC(SYSDATE) - 30
GROUP BY merchant_category
HAVING COUNT(*) >= 1000
ORDER BY total_volume DESC;

%python
result = (df.filter(df.transaction_date >= F.current_date() - 30)
          .groupBy("merchant_category")
          .agg(F.count("*").alias("transaction_count"),
               F.sum("transaction_amount").alias("total_volume"),
               F.avg("transaction_amount").alias("avg_transaction_size"),
               F.countDistinct("card_id").alias("unique_cards"),
               F.sum(F.when(df.transaction_status == "declined", 1).otherwise(0)).alias("declined_transactions"))
          .withColumn("decline_rate", 
                     (F.col("declined_transactions") * 100.0 / F.col("transaction_count")))
          .filter(F.col("transaction_count") >= 1000)
          .orderBy(F.desc("total_volume")))

# Deloitte, EY, TCS (Common Pattern: Consulting project analytics)

%sql
SELECT 
    project_id,
    client_id,
    SUM(billable_hours) as total_billable_hours,
    SUM(billed_amount) as total_revenue,
    AVG(employee_rate) as avg_hourly_rate,
    SUM(CASE WHEN milestone_status = 'completed' THEN 1 ELSE 0 END) as completed_milestones,
    COUNT(*) as total_milestones,
    ROUND(SUM(CASE WHEN milestone_status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as completion_percentage
FROM project_milestones
WHERE milestone_date >= ADD_MONTHS(SYSDATE, -6)
GROUP BY project_id, client_id
HAVING COUNT(*) >= 3;

%python
result = (df.filter(df.milestone_date >= F.add_months(F.current_date(), -6))
          .groupBy("project_id", "client_id")
          .agg(F.sum("billable_hours").alias("total_billable_hours"),
               F.sum("billed_amount").alias("total_revenue"),
               F.avg("employee_rate").alias("avg_hourly_rate"),
               F.sum(F.when(df.milestone_status == "completed", 1).otherwise(0)).alias("completed_milestones"),
               F.count("*").alias("total_milestones"))
          .withColumn("completion_percentage", 
                     (F.col("completed_milestones") * 100.0 / F.col("total_milestones")))
          .filter(F.col("total_milestones") >= 3))

# Expedia, Airbnb, Tripadvisor (Common Pattern: Booking patterns)

%sql
SELECT 
    hotel_id,
    EXTRACT(MONTH FROM check_in_date) as month,
    COUNT(*) as total_bookings,
    AVG(booking_amount) as avg_booking_value,
    AVG(DATEDIFF(check_out_date, check_in_date)) as avg_stay_duration,
    SUM(CASE WHEN cancellation_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as completion_rate
FROM bookings
WHERE check_in_date >= ADD_MONTHS(SYSDATE, -12)
GROUP BY hotel_id, EXTRACT(MONTH FROM check_in_date)
HAVING COUNT(*) >= 10
ORDER BY hotel_id, month;

%python
result = (df.filter(df.check_in_date >= F.add_months(F.current_date(), -12))
          .withColumn("month", F.month("check_in_date"))
          .groupBy("hotel_id", "month")
          .agg(F.count("*").alias("total_bookings"),
               F.avg("booking_amount").alias("avg_booking_value"),
               F.avg(F.datediff("check_out_date", "check_in_date")).alias("avg_stay_duration"),
               (F.sum(F.when(df.cancellation_date.isNull(), 1).otherwise(0)) * 100.0 / 
                F.count("*")).alias("completion_rate"))
          .filter(F.col("total_bookings") >= 10)
          .orderBy("hotel_id", "month"))

# Amazon, Doordash, Bosch (Common Pattern: Time series gap analysis)

%sql
SELECT 
    machine_id,
    status,
    start_time,
    end_time,
    LEAD(start_time) OVER (PARTITION BY machine_id ORDER BY start_time) as next_start_time,
    EXTRACT(MINUTE FROM (LEAD(start_time) OVER (PARTITION BY machine_id ORDER BY start_time) - end_time)) as gap_minutes
FROM machine_status
WHERE status = 'idle'
QUALIFY gap_minutes > 60;

%python
window_spec = Window.partitionBy("machine_id").orderBy("start_time")

result = (df.filter(df.status == "idle")
          .withColumn("next_start_time", F.lead("start_time").over(window_spec))
          .withColumn("gap_minutes", 
                     (F.unix_timestamp("next_start_time") - F.unix_timestamp("end_time")) / 60)
          .filter(F.col("gap_minutes") > 60))

# Goldman Sachs (Common Pattern: Fraud detection)

%sql
SELECT 
    transaction_id,
    account_id,
    transaction_amount,
    transaction_date,
    AVG(transaction_amount) OVER (PARTITION BY account_id ORDER BY transaction_date 
                                 RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) as avg_7d_amount,
    CASE WHEN transaction_amount > 3 * AVG(transaction_amount) OVER (PARTITION BY account_id ORDER BY transaction_date 
                                  RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END as is_suspicious
FROM transactions
WHERE transaction_date >= SYSDATE - 90
QUALIFY is_suspicious = 1;

%python
window_7d = Window.partitionBy("account_id").orderBy("transaction_date").rangeBetween(-7*86400, 0)
window_30d = Window.partitionBy("account_id").orderBy("transaction_date").rangeBetween(-30*86400, 0)

result = (df.filter(df.transaction_date >= F.current_date() - 90)
          .withColumn("avg_7d_amount", F.avg("transaction_amount").over(window_7d))
          .withColumn("avg_30d_amount", F.avg("transaction_amount").over(window_30d))
          .withColumn("is_suspicious", F.when(df.transaction_amount > 3 * F.col("avg_30d_amount"), 1).otherwise(0))
          .filter(F.col("is_suspicious") == 1))

# Meta (Common Pattern: User engagement funnel)

%sql
SELECT 
    campaign_id,
    COUNT(DISTINCT user_id) as total_users,
    COUNT(DISTINCT CASE WHEN event_type = 'impression' THEN user_id END) as impressions,
    COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END) as clicks,
    COUNT(DISTINCT CASE WHEN event_type = 'conversion' THEN user_id END) as conversions,
    ROUND(COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'impression' THEN user_id END), 0), 2) as ctr,
    ROUND(COUNT(DISTINCT CASE WHEN event_type = 'conversion' THEN user_id END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END), 0), 2) as conversion_rate
FROM campaign_events
WHERE event_date >= SYSDATE - 30
GROUP BY campaign_id
ORDER BY conversion_rate DESC;

%python
result = (df.filter(df.event_date >= F.current_date() - 30)
          .groupBy("campaign_id")
          .agg(F.countDistinct("user_id").alias("total_users"),
               F.countDistinct(F.when(df.event_type == "impression", df.user_id)).alias("impressions"),
               F.countDistinct(F.when(df.event_type == "click", df.user_id)).alias("clicks"),
               F.countDistinct(F.when(df.event_type == "conversion", df.user_id)).alias("conversions"))
          .withColumn("ctr", (F.col("clicks") * 100.0 / F.nullif(F.col("impressions"), 0)))
          .withColumn("conversion_rate", (F.col("conversions") * 100.0 / F.nullif(F.col("clicks"), 0)))
          .orderBy(F.desc("conversion_rate")))

# Amazon (Common Pattern: Customer lifetime value)

%sql
SELECT 
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(order_amount) as total_spent,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    AVG(order_amount) as avg_order_value,
    ROUND((MAX(order_date) - MIN(order_date)) / COUNT(DISTINCT order_id), 2) as avg_days_between_orders
FROM orders
GROUP BY customer_id
HAVING COUNT(DISTINCT order_id) >= 3
   AND SUM(order_amount) >= 500
ORDER BY total_spent DESC;

%python
result = (df.groupBy("customer_id")
          .agg(F.countDistinct("order_id").alias("total_orders"),
               F.sum("order_amount").alias("total_spent"),
               F.min("order_date").alias("first_order_date"),
               F.max("order_date").alias("last_order_date"),
               F.avg("order_amount").alias("avg_order_value"))
          .withColumn("avg_days_between_orders", 
                     (F.datediff("last_order_date", "first_order_date") / F.col("total_orders")))
          .filter((F.col("total_orders") >= 3) & (F.col("total_spent") >= 500))
          .orderBy(F.desc("total_spent")))

# Tesla (Common Pattern: Vehicle performance metrics)

%sql
SELECT 
    vehicle_id,
    AVG(battery_health) as avg_battery_health,
    AVG(energy_consumption) as avg_energy_consumption,
    MAX(odometer) as total_mileage,
    COUNT(DISTINCT charging_session_id) as charging_sessions,
    AVG(charging_duration) as avg_charging_time
FROM vehicle_metrics
WHERE metric_date >= ADD_MONTHS(SYSDATE, -3)
GROUP BY vehicle_id
HAVING MAX(odometer) >= 1000;

%python
result = (df.filter(df.metric_date >= F.add_months(F.current_date(), -3))
          .groupBy("vehicle_id")
          .agg(F.avg("battery_health").alias("avg_battery_health"),
               F.avg("energy_consumption").alias("avg_energy_consumption"),
               F.max("odometer").alias("total_mileage"),
               F.countDistinct("charging_session_id").alias("charging_sessions"),
               F.avg("charging_duration").alias("avg_charging_time"))
          .filter(F.col("total_mileage") >= 1000))

# Doordash (Common Pattern: Delivery performance)

%sql
SELECT 
    dasher_id,
    AVG(delivery_time - order_time) as avg_delivery_time,
    AVG(CASE WHEN delivery_time - order_time <= 30 THEN 1 ELSE 0 END) as on_time_rate,
    COUNT(*) as total_deliveries,
    SUM(tip_amount) as total_tips,
    AVG(tip_amount) as avg_tip
FROM deliveries
WHERE order_date >= TRUNC(SYSDATE) - 7
GROUP BY dasher_id
HAVING COUNT(*) >= 5
ORDER BY on_time_rate DESC;

%python
result = (df.filter(df.order_date >= F.current_date() - 7)
          .withColumn("delivery_duration", 
                     F.unix_timestamp("delivery_time") - F.unix_timestamp("order_time"))
          .groupBy("dasher_id")
          .agg(F.avg("delivery_duration").alias("avg_delivery_time"),
               F.avg(F.when(F.col("delivery_duration") <= 1800, 1).otherwise(0)).alias("on_time_rate"),
               F.count("*").alias("total_deliveries"),
               F.sum("tip_amount").alias("total_tips"),
               F.avg("tip_amount").alias("avg_tip"))
          .filter(F.col("total_deliveries") >= 5)
          .orderBy(F.desc("on_time_rate")))

# Meta (Common Pattern: Social media engagement)

%sql
SELECT 
    post_id,
    user_id,
    COUNT(*) as total_reactions,
    COUNT(DISTINCT reaction_type) as unique_reaction_types,
    SUM(CASE WHEN reaction_type = 'like' THEN 1 ELSE 0 END) as likes,
    SUM(CASE WHEN reaction_type = 'share' THEN 1 ELSE 0 END) as shares,
    SUM(CASE WHEN reaction_type = 'comment' THEN 1 ELSE 0 END) as comments,
    ROUND(SUM(CASE WHEN reaction_type = 'share' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as share_rate
FROM reactions
WHERE reaction_date >= SYSDATE - 7
GROUP BY post_id, user_id
HAVING COUNT(*) >= 10;

%python
result = (df.filter(df.reaction_date >= F.current_date() - 7)
          .groupBy("post_id", "user_id")
          .agg(F.count("*").alias("total_reactions"),
               F.countDistinct("reaction_type").alias("unique_reaction_types"),
               F.sum(F.when(df.reaction_type == "like", 1).otherwise(0)).alias("likes"),
               F.sum(F.when(df.reaction_type == "share", 1).otherwise(0)).alias("shares"),
               F.sum(F.when(df.reaction_type == "comment", 1).otherwise(0)).alias("comments"))
          .withColumn("share_rate", (F.col("shares") * 100.0 / F.col("total_reactions")))
          .filter(F.col("total_reactions") >= 10))

# Amazon (Common Pattern: Order fulfillment)

%sql
SELECT 
    warehouse_id,
    product_category,
    AVG(processing_time) as avg_processing_time,
    SUM(CASE WHEN processing_time > 48 THEN 1 ELSE 0 END) as delayed_orders,
    COUNT(*) as total_orders,
    ROUND(SUM(CASE WHEN processing_time > 48 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delay_percentage
FROM orders
WHERE order_date >= TRUNC(SYSDATE) - 30
GROUP BY warehouse_id, product_category
HAVING COUNT(*) >= 50
ORDER BY delay_percentage DESC;

%python
result = (df.filter(df.order_date >= F.current_date() - 30)
          .groupBy("warehouse_id", "product_category")
          .agg(F.avg("processing_time").alias("avg_processing_time"),
               F.sum(F.when(df.processing_time > 48, 1).otherwise(0)).alias("delayed_orders"),
               F.count("*").alias("total_orders"))
          .withColumn("delay_percentage", 
                     (F.col("delayed_orders") * 100.0 / F.col("total_orders")))
          .filter(F.col("total_orders") >= 50)
          .orderBy(F.desc("delay_percentage")))

# ESPN (Common Pattern: Sports statistics)

%sql
SELECT 
    player_id,
    team_id,
    AVG(points) as avg_points,
    AVG(rebounds) as avg_rebounds,
    AVG(assists) as avg_assists,
    SUM(points) as total_points,
    RANK() OVER (PARTITION BY team_id ORDER BY SUM(points) DESC) as team_rank
FROM player_stats
WHERE game_date >= TRUNC(SYSDATE) - 365
GROUP BY player_id, team_id
HAVING COUNT(*) >= 10;

%python
window_spec = Window.partitionBy("team_id").orderBy(F.desc("total_points"))

result = (df.filter(df.game_date >= F.current_date() - 365)
          .groupBy("player_id", "team_id")
          .agg(F.avg("points").alias("avg_points"),
               F.avg("rebounds").alias("avg_rebounds"),
               F.avg("assists").alias("avg_assists"),
               F.sum("points").alias("total_points"),
               F.count("*").alias("games_played"))
          .filter(F.col("games_played") >= 10)
          .withColumn("team_rank", F.rank().over(window_spec)))

# Google (Common Pattern: Search query analysis)

%sql
SELECT 
    search_query,
    COUNT(*) as total_searches,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(CASE WHEN click_position IS NOT NULL THEN 1 ELSE 0 END) as click_through_rate,
    AVG(result_count) as avg_results
FROM search_logs
WHERE search_timestamp >= SYSDATE - 7
GROUP BY search_query
HAVING COUNT(*) >= 100
ORDER BY total_searches DESC;

%python
result = (df.filter(df.search_timestamp >= F.current_date() - 7)
          .groupBy("search_query")
          .agg(F.count("*").alias("total_searches"),
               F.countDistinct("user_id").alias("unique_users"),
               F.avg(F.when(df.click_position.isNotNull(), 1).otherwise(0)).alias("click_through_rate"),
               F.avg("result_count").alias("avg_results"))
          .filter(F.col("total_searches") >= 100)
          .orderBy(F.desc("total_searches")))

# Amazon (Common Pattern: Product review sentiment)

%sql
SELECT 
    product_id,
    AVG(rating) as avg_rating,
    COUNT(*) as total_reviews,
    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) as positive_reviews,
    SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) as negative_reviews,
    ROUND(SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positive_percentage
FROM reviews
WHERE review_date >= ADD_MONTHS(SYSDATE, -6)
GROUP BY product_id
HAVING COUNT(*) >= 10
ORDER BY positive_percentage DESC;

%python
result = (df.filter(df.review_date >= F.add_months(F.current_date(), -6))
          .groupBy("product_id")
          .agg(F.avg("rating").alias("avg_rating"),
               F.count("*").alias("total_reviews"),
               F.sum(F.when(df.rating >= 4, 1).otherwise(0)).alias("positive_reviews"),
               F.sum(F.when(df.rating <= 2, 1).otherwise(0)).alias("negative_reviews"))
          .withColumn("positive_percentage", 
                     (F.col("positive_reviews") * 100.0 / F.col("total_reviews")))
          .filter(F.col("total_reviews") >= 10)
          .orderBy(F.desc("positive_percentage")))

# Microsoft (Common Pattern: Software usage analytics)

%sql
SELECT 
    user_id,
    feature_name,
    COUNT(*) as usage_count,
    AVG(usage_duration) as avg_duration,
    RANK() OVER (PARTITION BY feature_name ORDER BY COUNT(*) DESC) as usage_rank
FROM feature_usage
WHERE usage_date >= SYSDATE - 90
GROUP BY user_id, feature_name
QUALIFY RANK() OVER (PARTITION BY feature_name ORDER BY COUNT(*) DESC) <= 10;

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("feature_name").orderBy(F.desc("usage_count"))

result = (df.filter(df.usage_date >= F.current_date() - 90)
          .groupBy("user_id", "feature_name")
          .agg(F.count("*").alias("usage_count"),
               F.avg("usage_duration").alias("avg_duration"))
          .withColumn("usage_rank", F.rank().over(window_spec))
          .filter(F.col("usage_rank") <= 10))

# Uber (Common Pattern: Ride analysis and surge pricing)

%sql
SELECT 
    driver_id,
    COUNT(*) as total_rides,
    AVG(rating) as avg_rating,
    SUM(CASE WHEN surge_multiplier > 1.5 THEN 1 ELSE 0 END) as surge_rides,
    SUM(fare_amount * surge_multiplier) as total_earnings
FROM rides
WHERE ride_date >= TRUNC(SYSDATE) - 30
GROUP BY driver_id
HAVING COUNT(*) >= 20
ORDER BY total_earnings DESC;

%python
result = (df.filter(df.ride_date >= F.current_date() - 30)
          .groupBy("driver_id")
          .agg(F.count("*").alias("total_rides"),
               F.avg("rating").alias("avg_rating"),
               F.sum(F.when(df.surge_multiplier > 1.5, 1).otherwise(0)).alias("surge_rides"),
               F.sum(df.fare_amount * df.surge_multiplier).alias("total_earnings"))
          .filter(F.col("total_rides") >= 20)
          .orderBy(F.desc("total_earnings")))

# Goldman Sachs (Common Pattern: Financial transactions)

%sql
SELECT 
    account_id,
    transaction_date,
    amount,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY transaction_date 
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance,
    AVG(amount) OVER (PARTITION BY account_id ORDER BY transaction_date 
                     RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) as avg_7d_amount
FROM transactions
WHERE transaction_date >= SYSDATE - 90
ORDER BY account_id, transaction_date;

%python
window_spec_running = Window.partitionBy("account_id").orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
window_spec_7d = Window.partitionBy("account_id").orderBy("transaction_date").rangeBetween(-7*86400, 0)  # 7 days in seconds

result = (df.filter(df.transaction_date >= F.current_date() - 90)
          .withColumn("running_balance", F.sum("amount").over(window_spec_running))
          .withColumn("avg_7d_amount", F.avg("amount").over(window_spec_7d))
          .orderBy("account_id", "transaction_date"))

# Google (Common Pattern: Query performance)

%sql
SELECT 
    query_type,
    AVG(execution_time) as avg_execution_time,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time) as p95_execution_time,
    COUNT(*) as total_queries,
    SUM(CASE WHEN execution_time > 1000 THEN 1 ELSE 0 END) as slow_queries
FROM query_logs
WHERE log_timestamp >= SYSDATE - 7
GROUP BY query_type
HAVING COUNT(*) >= 100;

%python
result = (df.filter(df.log_timestamp >= F.current_date() - 7)
          .groupBy("query_type")
          .agg(F.avg("execution_time").alias("avg_execution_time"),
               F.expr("percentile_approx(execution_time, 0.95)").alias("p95_execution_time"),
               F.count("*").alias("total_queries"),
               F.sum(F.when(df.execution_time > 1000, 1).otherwise(0)).alias("slow_queries"))
          .filter(F.col("total_queries") >= 100))

# Amazon (Common Pattern: Customer retention)

%sql
SELECT 
    TO_CHAR(first_order_date, 'YYYY-MM') as cohort_month,
    COUNT(DISTINCT customer_id) as total_customers,
    COUNT(DISTINCT CASE WHEN order_date BETWEEN first_order_date AND first_order_date + 30 
                        THEN customer_id END) as retained_30d,
    ROUND(COUNT(DISTINCT CASE WHEN order_date BETWEEN first_order_date AND first_order_date + 30 
                             THEN customer_id END) * 100.0 / 
          COUNT(DISTINCT customer_id), 2) as retention_rate
FROM (
    SELECT 
        customer_id,
        order_date,
        MIN(order_date) OVER (PARTITION BY customer_id) as first_order_date
    FROM orders
)
GROUP BY TO_CHAR(first_order_date, 'YYYY-MM')
ORDER BY cohort_month;

%python
window_spec = Window.partitionBy("customer_id")
df_with_cohort = df.withColumn("first_order_date", F.min("order_date").over(window_spec))

result = (df_with_cohort
          .groupBy(F.date_format("first_order_date", "yyyy-MM").alias("cohort_month"))
          .agg(F.countDistinct("customer_id").alias("total_customers"),
               F.countDistinct(F.when(
                   (F.col("order_date") >= F.col("first_order_date")) &
                   (F.col("order_date") <= F.col("first_order_date") + 30),
                   F.col("customer_id")
               )).alias("retained_30d"))
          .withColumn("retention_rate", 
                     (F.col("retained_30d") * 100.0 / F.col("total_customers"))))

# Walmart (Common Pattern: Inventory management)

%sql
SELECT 
    product_id,
    product_name,
    current_stock,
    daily_sales_avg,
    ROUND(current_stock / NULLIF(daily_sales_avg, 0)) as days_of_supply
FROM (
    SELECT 
        p.product_id,
        p.product_name,
        p.current_stock,
        AVG(s.quantity) as daily_sales_avg
    FROM products p
    LEFT JOIN sales s ON p.product_id = s.product_id 
                     AND s.sale_date >= SYSDATE - 30
    GROUP BY p.product_id, p.product_name, p.current_stock
)
WHERE current_stock / NULLIF(daily_sales_avg, 0) < 7;

%python
products = spark.table("products")
sales = spark.table("sales").filter(F.col("sale_date") >= F.current_date() - 30)

result = (products.join(sales, "product_id", "left")
          .groupBy("product_id", "product_name", "current_stock")
          .agg(F.avg("quantity").alias("daily_sales_avg"))
          .withColumn("days_of_supply", 
                     F.col("current_stock") / F.nullif(F.col("daily_sales_avg"), 0))
          .filter(F.col("days_of_supply") < 7))

# City of San Francisco (Common Pattern: Salary analysis)

%sql
SELECT 
    department,
    job_title,
    AVG(salary) as avg_salary,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) as median_salary,
    MAX(salary) - MIN(salary) as salary_range
FROM employees
WHERE employment_status = 'Active'
GROUP BY department, job_title
HAVING COUNT(*) >= 5;

%python
result = (df.filter(df.employment_status == "Active")
          .groupBy("department", "job_title")
          .agg(F.avg("salary").alias("avg_salary"),
               F.expr("percentile_approx(salary, 0.5)").alias("median_salary"),
               (F.max("salary") - F.min("salary")).alias("salary_range"),
               F.count("*").alias("employee_count"))
          .filter(F.col("employee_count") >= 5))

# IBM (Common Pattern: Employee department transfer)

%sql
SELECT 
    employee_id,
    department_id,
    start_date,
    end_date,
    LEAD(start_date) OVER (PARTITION BY employee_id ORDER BY start_date) as next_start_date
FROM department_history
ORDER BY employee_id, start_date;

%python
window_spec = Window.partitionBy("employee_id").orderBy("start_date")
result = df.withColumn("next_start_date", F.lead("start_date").over(window_spec))

# Amazon (Common Pattern: Monthly growth rate)

%sql
SELECT 
    TO_CHAR(order_date, 'YYYY-MM') as month,
    COUNT(*) as order_count,
    LAG(COUNT(*)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM')) as prev_month_count,
    ROUND((COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM'))) * 100.0 /
          NULLIF(LAG(COUNT(*)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM')), 0), 2) as growth_rate
FROM orders
GROUP BY TO_CHAR(order_date, 'YYYY-MM')
ORDER BY month;

%python
from pyspark.sql.window import Window

monthly_orders = (df.groupBy(F.date_format("order_date", "yyyy-MM").alias("month"))
                  .agg(F.count("*").alias("order_count")))

window_spec = Window.orderBy("month")
result = (monthly_orders
          .withColumn("prev_month_count", F.lag("order_count").over(window_spec))
          .withColumn("growth_rate", 
                     ((F.col("order_count") - F.col("prev_month_count")) * 100.0 /
                      F.nullif(F.col("prev_month_count"), 0)))
          .orderBy("month"))

# Google (Common Pattern: Click-through rate)

%sql
SELECT 
    ad_id,
    ROUND(
        SUM(CASE WHEN action = 'click' THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(SUM(CASE WHEN action IN ('click', 'view') THEN 1 ELSE 0 END), 0),
    2) as ctr
FROM ad_events
GROUP BY ad_id
ORDER BY ctr DESC, ad_id ASC;

%python
result = (df.filter(df.action.isin(["click", "view"]))
          .groupBy("ad_id")
          .agg(
              (F.sum(F.when(df.action == "click", 1).otherwise(0)) * 100.0 /
               F.nullif(F.sum(F.when(df.action.isin(["click", "view"]), 1).otherwise(0)), 0)
              ).alias("ctr"))
          .orderBy(F.desc("ctr"), F.asc("ad_id")))

# Meta (Common Pattern: Active users/engagement)

%sql
SELECT 
    user_id,
    COUNT(DISTINCT session_date) as active_days,
    AVG(session_duration) as avg_session_duration
FROM user_sessions
WHERE session_date BETWEEN SYSDATE - 30 AND SYSDATE
GROUP BY user_id
HAVING COUNT(DISTINCT session_date) >= 15;

%python
result = (df.filter((df.session_date >= F.current_date() - 30) & 
                   (df.session_date <= F.current_date()))
          .groupBy("user_id")
          .agg(F.countDistinct("session_date").alias("active_days"),
               F.avg("session_duration").alias("avg_session_duration"))
          .filter(F.col("active_days") >= 15))

# KPMG (Common Pattern: Customer segmentation)

%sql
 SELECT 
    customer_id,
    CASE 
        WHEN total_spent >= 10000 THEN 'Platinum'
        WHEN total_spent >= 5000 THEN 'Gold'
        WHEN total_spent >= 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_tier,
    total_spent
FROM (
    SELECT customer_id, SUM(amount) as total_spent
    FROM transactions
    WHERE transaction_date >= ADD_MONTHS(SYSDATE, -12)
    GROUP BY customer_id
);

%python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

result = (df.filter(df.transaction_date >= F.current_date() - 365)
          .groupBy("customer_id")
          .agg(F.sum("amount").alias("total_spent"))
          .withColumn("customer_tier", 
                     F.when(F.col("total_spent") >= 10000, "Platinum")
                      .when(F.col("total_spent") >= 5000, "Gold")
                      .when(F.col("total_spent") >= 1000, "Silver")
                      .otherwise("Bronze")))

# Microsoft (Common Pattern: Employee hierarchy)

%sql
SELECT e.employee_name, m.employee_name as manager_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id;

%python
employees = spark.table("employees").alias("e")
managers = spark.table("employees").alias("m")

result = (employees.join(managers, 
                       employees.manager_id == managers.employee_id, 
                       "left")
          .select("e.employee_name", "m.employee_name".alias("manager_name")))

# Amazon (Common Pattern: Product recommendation)

%sql
SELECT p1.product_id, p2.product_id, COUNT(*) as frequency
FROM orders o1
JOIN orders o2 ON o1.customer_id = o2.customer_id 
               AND o1.order_id = o2.order_id
               AND o1.product_id < o2.product_id
GROUP BY p1.product_id, p2.product_id
ORDER BY frequency DESC
FETCH FIRST 10 ROWS ONLY;

%python
# Self-join to find products bought together
orders1 = df.alias("o1")
orders2 = df.alias("o2")

result = (orders1.join(orders2, 
                     (F.col("o1.customer_id") == F.col("o2.customer_id")) &
                     (F.col("o1.order_id") == F.col("o2.order_id")) &
                     (F.col("o1.product_id") < F.col("o2.product_id")))
          .groupBy("o1.product_id", "o2.product_id")
          .agg(F.count("*").alias("frequency"))
          .orderBy(F.desc("frequency"))
          .limit(10))

# Doordash (Common Pattern: Delivery time analysis)

%sql
SELECT driver_id, 
       AVG(delivery_time - order_time) as avg_delivery_time,
       COUNT(*) as total_deliveries
FROM deliveries
WHERE order_time >= SYSDATE - 7
GROUP BY driver_id
HAVING COUNT(*) >= 5;

%python
result = (df.filter(df.order_time >= F.current_date() - 7)
          .withColumn("delivery_duration", 
                     F.unix_timestamp("delivery_time") - F.unix_timestamp("order_time"))
          .groupBy("driver_id")
          .agg(F.avg("delivery_duration").alias("avg_delivery_time"),
               F.count("*").alias("total_deliveries"))
          .filter(F.col("total_deliveries") >= 5))

# Amazon (Common Pattern: Customer spending analysis)

%sql
SELECT customer_id, SUM(amount) as total_spent
FROM orders
WHERE order_date >= ADD_MONTHS(SYSDATE, -3)
GROUP BY customer_id
HAVING SUM(amount) > 1000;

%python
from pyspark.sql import functions as F

three_months_ago = F.current_date() - F.expr("INTERVAL 3 MONTHS")
result = (df.filter(df.order_date >= three_months_ago)
          .groupBy("customer_id")
          .agg(F.sum("amount").alias("total_spent"))
          .filter(F.col("total_spent") > 1000))

# Apple, Microsoft (Common Pattern: Consecutive login days)

%sql
SELECT user_id
FROM (
    SELECT user_id, login_date,
           login_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) as grp
    FROM logins
)
GROUP BY user_id, grp
HAVING COUNT(*) >= 3;

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("user_id").orderBy("login_date")
df = df.withColumn("rn", F.row_number().over(window_spec))
df = df.withColumn("date_diff", F.datediff("login_date", F.to_date(F.lit("1970-01-01"))))
df = df.withColumn("grp", df.date_diff - df.rn)

result = (df.groupBy("user_id", "grp")
          .agg(F.count("*").alias("consecutive_days"))
          .filter(F.col("consecutive_days") >= 3)
          .select("user_id"))

# Twitch (Common Pattern: Active users/streamers)

%sql
SELECT user_id, COUNT(DISTINCT session_date) as active_days
FROM streaming_sessions
WHERE session_date >= SYSDATE - 30
GROUP BY user_id
HAVING COUNT(DISTINCT session_date) >= 5;

%python
from pyspark.sql import functions as F

df = spark.table("streaming_sessions")
thirty_days_ago = F.current_date() - F.expr("INTERVAL 30 DAYS")

result = (df.filter(df.session_date >= thirty_days_ago)
          .groupBy("user_id")
          .agg(F.countDistinct("session_date").alias("active_days"))
          .filter(F.col("active_days") >= 5))

# Amazon (Common Pattern: Department highest salary)

%sql
SELECT department, employee_name, salary
FROM (
    SELECT department, employee_name, salary,
           DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rnk
    FROM employees
) 
WHERE rnk = 1;

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("department").orderBy(F.desc("salary"))
df = df.withColumn("rank", F.dense_rank().over(window_spec))
result = df.filter(df.rank == 1).select("department", "employee_name", "salary")

# Amazon (Common Pattern: Second highest salary)

%sql
SELECT MAX(salary) as second_highest_salary
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);

%python
from pyspark.sql.window import Window
from pyspark.sql import functions as F

window_spec = Window.orderBy(F.desc("salary"))
df = df.withColumn("rank", F.dense_rank().over(window_spec))
result = df.filter(df.rank == 2).select("salary")

# Google (Common Pattern: Finding duplicates or unique values)

%sql
SELECT email, COUNT(*)
FROM users
GROUP BY email
HAVING COUNT(*) > 1;

%python
from pyspark.sql import functions as F

df = spark.table("users")
result = df.groupBy("email").agg(F.count("*").alias("count"))
result.filter(result.count > 1).show()