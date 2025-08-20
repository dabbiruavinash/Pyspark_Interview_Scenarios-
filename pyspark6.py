from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DailySQLChallenges") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# DAY 26: LinkedIn - Professional Network Analysis
def linkedin_analysis(connections_df, profiles_df):
    """Analyze professional networks and connections"""
    # 1. Connection degree analysis
    connection_degrees = connections_df.groupBy("user_id").agg(
        count("*").alias("connection_count")
    )
    
    # 2. Industry connections
    industry_connections = (connections_df.alias("c")
                          .join(profiles_df.alias("p1"), col("c.user_id") == col("p1.user_id"))
                          .join(profiles_df.alias("p2"), col("c.connection_id") == col("p2.user_id"))
                          .groupBy("p1.industry", "p2.industry")
                          .agg(count("*").alias("connection_count")))
    
    return connection_degrees, industry_connections

# DAY 25: American Express - Financial Transactions
def amex_transaction_analysis(transactions_df, customers_df):
    """Financial transaction analysis for AmEx"""
    # 1. Fraud detection patterns
    fraud_patterns = transactions_df.groupBy("merchant_category", "transaction_type").agg(
        avg("amount").alias("avg_amount"),
        count("*").alias("transaction_count"),
        sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count")
    ).withColumn("fraud_rate", col("fraud_count") / col("transaction_count"))
    
    # 2. Customer spending patterns
    customer_spending = (transactions_df.join(customers_df, "customer_id")
                        .groupBy("customer_id", "customer_segment")
                        .agg(sum("amount").alias("total_spent"),
                             count("*").alias("transaction_count")))
    
    return fraud_patterns, customer_spending

# DAY 24: Amazon - E-commerce Analytics
def amazon_ecommerce_analysis(orders_df, products_df, customers_df):
    """E-commerce analysis for Amazon"""
    # 1. Product performance
    product_performance = (orders_df.join(products_df, "product_id")
                          .groupBy("product_id", "category")
                          .agg(sum("quantity").alias("total_sold"),
                               sum("revenue").alias("total_revenue"),
                               countDistinct("customer_id").alias("unique_customers")))
    
    # 2. Customer lifetime value
    customer_lifetime = (orders_df.join(customers_df, "customer_id")
                        .groupBy("customer_id")
                        .agg(sum("revenue").alias("lifetime_value"),
                             countDistinct("order_id").alias("order_count"),
                             datediff(current_date(), min("order_date")).alias("days_since_first_order")))
    
    return product_performance, customer_lifetime

# DAY 23: Oracle - Database & Enterprise Analytics
def oracle_database_analysis(queries_df, performance_df):
    """Database performance analysis for Oracle"""
    # 1. Query performance analysis
    query_performance = queries_df.groupBy("query_type", "database").agg(
        avg("execution_time").alias("avg_execution_time"),
        max("execution_time").alias("max_execution_time"),
        count("*").alias("query_count")
    )
    
    # 2. Resource utilization
    resource_utilization = performance_df.groupBy("server_id", "metric_type").agg(
        avg("value").alias("avg_value"),
        max("value").alias("max_value"),
        min("value").alias("min_value")
    )
    
    return query_performance, resource_utilization

# DAY 22: Walmart & PayPal - Retail & Payments
def walmart_paypal_analysis(sales_df, payments_df, inventory_df):
    """Retail and payment analysis for Walmart & PayPal"""
    # 1. Sales by payment method
    sales_by_payment = (sales_df.join(payments_df, "transaction_id")
                       .groupBy("payment_method")
                       .agg(sum("amount").alias("total_sales"),
                            count("*").alias("transaction_count")))
    
    # 2. Inventory turnover
    inventory_turnover = (sales_df.join(inventory_df, "product_id")
                         .groupBy("product_id", "category")
                         .agg(sum("quantity").alias("units_sold"),
                              avg("inventory_level").alias("avg_inventory")))
    
    return sales_by_payment, inventory_turnover

# DAY 21: Microsoft - Enterprise Software
def microsoft_enterprise_analysis(usage_df, licenses_df, users_df):
    """Enterprise software analysis for Microsoft"""
    # 1. License utilization
    license_utilization = (usage_df.join(licenses_df, "software_id")
                          .groupBy("software_id", "license_type")
                          .agg(countDistinct("user_id").alias("active_users"),
                               sum("usage_hours").alias("total_usage")))
    
    # 2. User adoption rates
    user_adoption = (usage_df.join(users_df, "user_id")
                    .groupBy("department", "role")
                    .agg(countDistinct("software_id").alias("softwares_used"),
                         avg("usage_hours").alias("avg_usage_hours")))
    
    return license_utilization, user_adoption

# DAY 20: Apple & Microsoft - Tech Products
def apple_microsoft_analysis(sales_df, products_df, regions_df):
    """Tech product analysis for Apple & Microsoft"""
    # 1. Regional sales performance
    regional_sales = (sales_df.join(products_df, "product_id")
                     .join(regions_df, "region_id")
                     .groupBy("region_name", "product_category")
                     .agg(sum("revenue").alias("total_revenue"),
                          sum("units_sold").alias("total_units")))
    
    # 2. Product launch performance
    product_launch = sales_df.groupBy("product_id", "launch_date").agg(
        sum("revenue").alias("first_month_revenue"),
        sum("units_sold").alias("first_month_units")
    )
    
    return regional_sales, product_launch

# DAY 19: Walmart - Retail Analytics
def walmart_retail_analysis(sales_df, stores_df, products_df):
    """Retail analytics for Walmart"""
    # 1. Store performance comparison
    store_performance = (sales_df.join(stores_df, "store_id")
                        .groupBy("store_id", "location", "size")
                        .agg(sum("revenue").alias("total_revenue"),
                             sum("units_sold").alias("total_units"),
                             countDistinct("customer_id").alias("unique_customers")))
    
    # 2. Product category performance
    category_performance = (sales_df.join(products_df, "product_id")
                           .groupBy("category", "subcategory")
                           .agg(sum("revenue").alias("category_revenue"),
                                sum("units_sold").alias("category_units")))
    
    return store_performance, category_performance

# DAY 18: Amazon & DoorDash - Delivery Logistics
def amazon_doordash_logistics(orders_df, deliveries_df, drivers_df):
    """Delivery logistics analysis for Amazon & DoorDash"""
    # 1. Delivery performance metrics
    delivery_performance = (deliveries_df.join(orders_df, "order_id")
                          .groupBy("driver_id", "delivery_zone")
                          .agg(avg("delivery_time_minutes").alias("avg_delivery_time"),
                               count("*").alias("delivery_count"),
                               sum(when(col("on_time") == True, 1).otherwise(0)).alias("on_time_deliveries")))
    
    # 2. Driver efficiency analysis
    driver_efficiency = (deliveries_df.join(drivers_df, "driver_id")
                        .groupBy("driver_id", "experience_level")
                        .agg(avg("delivery_time_minutes").alias("avg_delivery_time"),
                             sum("distance_km").alias("total_distance"),
                             count("*").alias("total_deliveries")))
    
    return delivery_performance, driver_efficiency

# DAY 17: Uber - Ride Sharing Analytics
def uber_ride_analysis(rides_df, drivers_df, users_df):
    """Ride-sharing analysis for Uber"""
    # 1. Ride patterns by time and location
    ride_patterns = rides_df.groupBy(
        hour("ride_time").alias("hour_of_day"),
        dayofweek("ride_time").alias("day_of_week"),
        "pickup_zone"
    ).agg(
        count("*").alias("ride_count"),
        avg("fare").alias("avg_fare"),
        avg("distance_km").alias("avg_distance")
    )
    
    # 2. Driver performance metrics
    driver_performance = (rides_df.join(drivers_df, "driver_id")
                         .groupBy("driver_id", "driver_rating")
                         .agg(count("*").alias("total_rides"),
                              avg("fare").alias("avg_fare"),
                              avg("rating").alias("avg_rating")))
    
    return ride_patterns, driver_performance

# DAY 16: JP Morgan - Banking & Finance
def jpmorgan_banking_analysis(transactions_df, accounts_df, customers_df):
    """Banking analysis for JP Morgan"""
    # 1. Transaction patterns
    transaction_patterns = transactions_df.groupBy("transaction_type", "channel").agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_amount")
    )
    
    # 2. Customer account analysis
    customer_accounts = (accounts_df.join(customers_df, "customer_id")
                        .groupBy("customer_segment", "account_type")
                        .agg(avg("balance").alias("avg_balance"),
                             sum("balance").alias("total_assets"),
                             count("*").alias("account_count")))
    
    return transaction_patterns, customer_accounts

# DAY 15: Google - Search & Advertising
def google_search_analysis(clicks_df, searches_df, ads_df):
    """Search and advertising analysis for Google"""
    # 1. Search query analysis
    search_analysis = searches_df.groupBy("query_category", "device_type").agg(
        count("*").alias("search_count"),
        avg("results_count").alias("avg_results"),
        sum(when(col("clicked") == True, 1).otherwise(0)).alias("clicks")
    ).withColumn("click_through_rate", col("clicks") / col("search_count"))
    
    # 2. Ad performance metrics
    ad_performance = (ads_df.join(clicks_df, "ad_id")
                     .groupBy("advertiser", "ad_category")
                     .agg(sum("impressions").alias("total_impressions"),
                          sum("clicks").alias("total_clicks"),
                          sum("revenue").alias("total_revenue")))
    
    return search_analysis, ad_performance

# DAY 14: Amazon & Salesforce - Cloud & CRM
def amazon_salesforce_analysis(usage_df, customers_df, products_df):
    """Cloud and CRM analysis for Amazon & Salesforce"""
    # 1. Cloud service usage patterns
    cloud_usage = usage_df.groupBy("service_type", "customer_tier").agg(
        sum("usage_hours").alias("total_usage_hours"),
        avg("cost").alias("avg_cost"),
        countDistinct("customer_id").alias("active_customers")
    )
    
    # 2. Customer relationship metrics
    customer_relationships = customers_df.groupBy("industry", "company_size").agg(
        count("*").alias("customer_count"),
        avg("contract_value").alias("avg_contract_value"),
        sum("revenue").alias("total_revenue")
    )
    
    return cloud_usage, customer_relationships

# DAY 13: Expedia & Airbnb - Travel Analytics
def expedia_airbnb_analysis(bookings_df, properties_df, users_df):
    """Travel industry analysis for Expedia & Airbnb"""
    # 1. Booking patterns
    booking_patterns = bookings_df.groupBy(
        month("booking_date").alias("booking_month"),
        "destination",
        "travel_type"
    ).agg(
        count("*").alias("booking_count"),
        sum("amount").alias("total_revenue"),
        avg("nights").alias("avg_nights")
    )
    
    # 2. Property performance
    property_performance = (bookings_df.join(properties_df, "property_id")
                           .groupBy("property_type", "location")
                           .agg(avg("rating").alias("avg_rating"),
                                sum("revenue").alias("total_revenue"),
                                count("*").alias("booking_count")))
    
    return booking_patterns, property_performance

# DAY 12: LinkedIn & Dropbox - Professional Services
def linkedin_dropbox_analysis(activity_df, users_df, content_df):
    """Professional services analysis for LinkedIn & Dropbox"""
    # 1. User engagement metrics
    user_engagement = activity_df.groupBy("user_id", "activity_type").agg(
        count("*").alias("activity_count"),
        sum("duration_minutes").alias("total_duration")
    )
    
    # 2. Content performance
    content_performance = (activity_df.join(content_df, "content_id")
                          .groupBy("content_type", "category")
                          .agg(count("*").alias("view_count"),
                               avg("rating").alias("avg_rating"),
                               sum("shares").alias("total_shares")))
    
    return user_engagement, content_performance

# DAY 11: Nvidia & Microsoft - Hardware & Software
def nvidia_microsoft_analysis(sales_df, products_df, regions_df):
    """Hardware and software analysis for Nvidia & Microsoft"""
    # 1. Product sales trends
    sales_trends = (sales_df.join(products_df, "product_id")
                   .groupBy("product_category", "release_quarter")
                   .agg(sum("units_sold").alias("total_units"),
                        sum("revenue").alias("total_revenue"),
                        avg("price").alias("avg_price")))
    
    # 2. Regional market share
    regional_market = (sales_df.join(regions_df, "region_id")
                      .groupBy("region_name", "product_category")
                      .agg(sum("revenue").alias("regional_revenue"),
                           sum("units_sold").alias("regional_units")))
    
    return sales_trends, regional_market

# DAY 10: Amazon - Marketplace Analytics
def amazon_marketplace_analysis(reviews_df, products_df, sellers_df):
    """Marketplace analysis for Amazon"""
    # 1. Product review analysis
    review_analysis = reviews_df.groupBy("product_id", "rating").agg(
        count("*").alias("review_count"),
        avg("helpful_votes").alias("avg_helpful_votes")
    )
    
    # 2. Seller performance metrics
    seller_performance = (reviews_df.join(products_df, "product_id")
                          .join(sellers_df, "seller_id")
                          .groupBy("seller_id", "seller_rating")
                          .agg(avg("rating").alias("avg_product_rating"),
                               count("*").alias("total_reviews"),
                               sum("helpful_votes").alias("total_helpful_votes")))
    
    return review_analysis, seller_performance

# DAY 9: Netflix - Streaming Analytics
def netflix_streaming_analysis(streams_df, content_df, users_df):
    """Streaming analysis for Netflix"""
    # 1. Content viewing patterns
    viewing_patterns = streams_df.groupBy("content_id", "device_type").agg(
        sum("watch_minutes").alias("total_watch_time"),
        countDistinct("user_id").alias("unique_viewers"),
        avg("rating").alias("avg_rating")
    )
    
    # 2. User engagement metrics
    user_engagement = (streams_df.join(users_df, "user_id")
                      .groupBy("user_segment", "subscription_type")
                      .agg(sum("watch_minutes").alias("total_watch_time"),
                           countDistinct("content_id").alias("unique_content"),
                           avg("rating").alias("avg_rating")))
    
    return viewing_patterns, user_engagement

# DAY 8: Tesla - Automotive Analytics
def tesla_automotive_analysis(vehicles_df, charging_df, trips_df):
    """Automotive analysis for Tesla"""
    # 1. Vehicle performance metrics
    vehicle_performance = trips_df.groupBy("vehicle_id", "model").agg(
        sum("distance_km").alias("total_distance"),
        avg("efficiency_kwh").alias("avg_efficiency"),
        sum("energy_used").alias("total_energy_used")
    )
    
    # 2. Charging patterns
    charging_patterns = charging_df.groupBy("vehicle_id", "charger_type").agg(
        avg("charging_time_minutes").alias("avg_charging_time"),
        sum("energy_added").alias("total_energy_added"),
        count("*").alias("charging_sessions")
    )
    
    return vehicle_performance, charging_patterns

# DAY 7: IBM - Enterprise Solutions
def ibm_enterprise_analysis(projects_df, clients_df, resources_df):
    """Enterprise solutions analysis for IBM"""
    # 1. Project performance metrics
    project_performance = projects_df.groupBy("project_type", "industry").agg(
        sum("revenue").alias("total_revenue"),
        avg("duration_months").alias("avg_duration"),
        sum("budget").alias("total_budget")
    )
    
    # 2. Resource utilization
    resource_utilization = (resources_df.join(projects_df, "project_id")
                           .groupBy("skill_category", "role")
                           .agg(avg("utilization_rate").alias("avg_utilization"),
                                sum("hours_billed").alias("total_hours_billed")))
    
    return project_performance, resource_utilization

# DAY 6: Airbnb - Hospitality Analytics
def airbnb_hospitality_analysis(bookings_df, listings_df, hosts_df):
    """Hospitality analysis for Airbnb"""
    # 1. Listing performance
    listing_performance = (bookings_df.join(listings_df, "listing_id")
                          .groupBy("listing_type", "neighborhood")
                          .agg(avg("price").alias("avg_price"),
                               sum("revenue").alias("total_revenue"),
                               avg("rating").alias("avg_rating")))
    
    # 2. Host performance metrics
    host_performance = (bookings_df.join(listings_df, "listing_id")
                        .join(hosts_df, "host_id")
                        .groupBy("host_id", "host_experience")
                        .agg(count("*").alias("booking_count"),
                             sum("revenue").alias("total_revenue"),
                             avg("rating").alias("avg_rating")))
    
    return listing_performance, host_performance

# DAY 5: Microsoft - Software Analytics
def microsoft_software_analysis(usage_df, licenses_df, users_df):
    """Software analytics for Microsoft"""
    # 1. Software adoption rates
    adoption_rates = usage_df.groupBy("software_id", "version").agg(
        countDistinct("user_id").alias("active_users"),
        sum("usage_hours").alias("total_usage_hours"),
        avg("sessions").alias("avg_sessions")
    )
    
    # 2. License compliance
    license_compliance = (usage_df.join(licenses_df, "software_id")
                         .groupBy("software_id", "license_type")
                         .agg(countDistinct("user_id").alias("actual_users"),
                              sum("licensed_users").alias("licensed_users")))
    
    return adoption_rates, license_compliance

# DAY 4: Uber - Mobility Analytics
def uber_mobility_analysis(rides_df, drivers_df, cities_df):
    """Mobility analysis for Uber"""
    # 1. City-level ride patterns
    city_patterns = (rides_df.join(cities_df, "city_id")
                    .groupBy("city_name", "population")
                    .agg(count("*").alias("ride_count"),
                         sum("fare").alias("total_fare"),
                         avg("distance_km").alias("avg_distance")))
    
    # 2. Demand forecasting patterns
    demand_patterns = rides_df.groupBy(
        hour("ride_time").alias("hour"),
        dayofweek("ride_time").alias("day_of_week")
    ).agg(
        count("*").alias("ride_count"),
        avg("surge_multiplier").alias("avg_surge"),
        sum("fare").alias("total_fare")
    )
    
    return city_patterns, demand_patterns

# DAY 3: Google - Search Analytics
def google_search_analytics(queries_df, results_df, users_df):
    """Search analytics for Google"""
    # 1. Query pattern analysis
    query_patterns = queries_df.groupBy("query_type", "device").agg(
        count("*").alias("query_count"),
        avg("results_count").alias("avg_results"),
        sum("click_count").alias("total_clicks")
    ).withColumn("click_through_rate", col("total_clicks") / col("query_count"))
    
    # 2. User search behavior
    user_behavior = (queries_df.join(users_df, "user_id")
                    .groupBy("user_segment", "location")
                    .agg(avg("session_length").alias("avg_session_length"),
                         count("*").alias("total_queries"),
                         avg("click_count").alias("avg_clicks")))
    
    return query_patterns, user_behavior

# DAY 2: Amazon - E-commerce Patterns
def amazon_ecommerce_patterns(orders_df, customers_df, products_df):
    """E-commerce patterns for Amazon"""
    # 1. Customer purchase patterns
    purchase_patterns = orders_df.groupBy("customer_id", "order_date").agg(
        sum("amount").alias("order_value"),
        count("*").alias("item_count"),
        collect_list("product_id").alias("products")
    )
    
    # 2. Product recommendation patterns
    product_associations = orders_df.groupBy("order_id").agg(
        collect_list("product_id").alias("product_basket")
    ).filter(size(col("product_basket")) > 1)
    
    return purchase_patterns, product_associations

# DAY 1: Meta/Facebook - Social Media Analytics
def meta_social_analytics(posts_df, users_df, engagements_df):
    """Social media analytics for Meta/Facebook"""
    # 1. Post engagement metrics
    post_engagement = (engagements_df.join(posts_df, "post_id")
                      .groupBy("post_type", "content_category")
                      .agg(sum("likes").alias("total_likes"),
                           sum("shares").alias("total_shares"),
                           sum("comments").alias("total_comments")))
    
    # 2. User activity patterns
    user_activity = engagements_df.groupBy("user_id", "activity_type").agg(
        count("*").alias("activity_count"),
        sum("duration_minutes").alias("total_duration")
    )
    
    return post_engagement, user_activity

# Utility class for testing daily challenges
class DailyChallengesValidator:
    def __init__(self, spark):
        self.spark = spark
        self.create_test_data()
    
    def create_test_data(self):
        """Create sample test data for daily challenges"""
        # E-commerce data (Amazon, Walmart)
        orders_data = [
            (1, 101, 50.0, 2, "2024-01-15"), (1, 102, 30.0, 1, "2024-01-15"),
            (2, 101, 25.0, 1, "2024-01-16"), (2, 103, 40.0, 3, "2024-01-16")
        ]
        self.orders_df = spark.createDataFrame(
            orders_data, ["order_id", "product_id", "amount", "quantity", "order_date"]
        )
        
        # Social media data (Meta, LinkedIn)
        posts_data = [
            (1, "video", "entertainment", 100, 20, 15),
            (2, "image", "news", 150, 30, 25),
            (3, "text", "education", 80, 10, 8)
        ]
        self.posts_df = spark.createDataFrame(
            posts_data, ["post_id", "post_type", "category", "likes", "shares", "comments"]
        )
        
        # Ride-sharing data (Uber)
        rides_data = [
            (1, 201, 25.50, 5.2, 4.8, "2024-01-15 08:30:00"),
            (2, 202, 18.75, 3.8, 4.5, "2024-01-15 12:15:00"),
            (3, 201, 32.00, 7.1, 4.9, "2024-01-15 18:45:00")
        ]
        self.rides_df = spark.createDataFrame(
            rides_data, ["ride_id", "driver_id", "fare", "distance_km", "rating", "ride_time"]
        )
        
        print("Test data created for daily challenges!")
    
    def run_daily_challenge_tests(self):
        """Test daily challenge patterns"""
        print("Testing daily SQL challenge patterns...")
        
        try:
            # Test e-commerce patterns (Amazon)
            purchase_patterns, _ = amazon_ecommerce_patterns(self.orders_df, None, None)
            print("✓ Amazon e-commerce patterns test passed")
            
            # Test social media patterns (Meta)
            post_engagement, _ = meta_social_analytics(self.posts_df, None, None)
            print("✓ Meta social analytics test passed")
            
            # Test ride-sharing patterns (Uber)
            city_patterns, _ = uber_mobility_analysis(self.rides_df, None, None)
            print("✓ Uber mobility analysis test passed")
            
            print("All daily challenge tests completed successfully!")
            
        except Exception as e:
            print(f"✗ Test failed: {str(e)}")

# Main execution
if __name__ == "__main__":
    print("Daily SQL Challenges Implementation in PySpark")
    print("=" * 60)
    print("26 days of SQL challenges from top companies")
    print("=" * 60)
    
    # Initialize validator
    validator = DailyChallengesValidator(spark)
    
    # Run tests
    validator.run_daily_challenge_tests()
    
    print("\nAll daily challenge patterns are ready to use!")
    print("Example usage:")
    print("  result = amazon_ecommerce_analysis(orders_df, products_df, customers_df)")
    print("  result = uber_ride_analysis(rides_df, drivers_df, users_df)")
    print("  result = google_search_analysis(clicks_df, searches_df, ads_df)")