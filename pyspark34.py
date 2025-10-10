# Challenge 1: Remove Duplicate Rows from DataFrame

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def remove_duplicates(df, columns):
    """
    Remove duplicate rows from DataFrame based on specific columns
    
    Args:
        df: Input DataFrame
        columns: List of column names to consider for duplicates
    
    Returns:
        DataFrame with duplicates removed
    """
    return df.dropDuplicates(columns)

# Alternative implementation with row_number() for more control
def remove_duplicates_advanced(df, columns, order_by=None):
    """
    Remove duplicates with optional ordering for which record to keep
    
    Args:
        df: Input DataFrame
        columns: List of column names for duplicate check
        order_by: Column to determine which duplicate to keep (keep first/last)
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    if order_by:
        window_spec = Window.partitionBy(columns).orderBy(col(order_by).desc())
    else:
        window_spec = Window.partitionBy(columns)
    
    df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
    return df_with_row_num.filter(col("row_num") == 1).drop("row_num")

# Example usage
def example_challenge_1():
    spark = SparkSession.builder.appName("Challenge1").getOrCreate()
    
    # Sample data
    data = [
        (1, "John", "IT", 5000),
        (2, "Jane", "HR", 6000),
        (1, "John", "IT", 5000),  # Duplicate
        (3, "Bob", "IT", 5500),
        (2, "Jane", "HR", 6000),  # Duplicate
        (4, "Alice", "Finance", 7000)
    ]
    
    columns = ["id", "name", "department", "salary"]
    df = spark.createDataFrame(data, columns)
    
    print("Original DataFrame:")
    df.show()
    
    # Remove duplicates based on all columns
    df_no_duplicates = remove_duplicates(df, ["id", "name", "department", "salary"])
    print("After removing duplicates (all columns):")
    df_no_duplicates.show()
    
    # Remove duplicates based on specific columns only
    df_no_id_duplicates = remove_duplicates(df, ["id"])
    print("After removing duplicates based on 'id' column:")
    df_no_id_duplicates.show()
    
    spark.stop()

# Challenge 2: CSV to Parquet Pipeline with Null Filtering

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def csv_to_parquet_pipeline(input_path, output_path, filter_columns=None):
    """
    Read CSV, filter null values, and write to Parquet
    
    Args:
        input_path: Path to input CSV file
        output_path: Path for output Parquet file
        filter_columns: Specific columns to check for nulls (if None, check all columns)
    """
    spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()
    
    try:
        # Read CSV file
        print(f"Reading CSV from: {input_path}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        
        print("Original data count:", df.count())
        df.show(5)
        
        # Filter out null values
        if filter_columns:
            # Filter specific columns
            for column in filter_columns:
                df = df.filter(col(column).isNotNull())
        else:
            # Filter all columns
            for column in df.columns:
                df = df.filter(col(column).isNotNull())
        
        print("After filtering nulls count:", df.count())
        df.show(5)
        
        # Write to Parquet
        print(f"Writing Parquet to: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        
        # Verify the output
        parquet_df = spark.read.parquet(output_path)
        print("Parquet file count:", parquet_df.count())
        
        print("Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Error in pipeline: {str(e)}")
        raise e
    finally:
        spark.stop()

# Example usage with sample data creation
def create_sample_csv():
    """Create a sample CSV file for testing"""
    import pandas as pd
    
    data = {
        'id': [1, 2, 3, 4, 5, None],
        'name': ['John', None, 'Bob', 'Alice', 'Eve', 'Frank'],
        'age': [25, 30, None, 35, 40, 45],
        'salary': [50000, 60000, 55000, None, 70000, 65000],
        'department': ['IT', 'HR', 'IT', 'Finance', None, 'HR']
    }
    
    pd_df = pd.DataFrame(data)
    pd_df.to_csv('sample_data.csv', index=False)
    print("Sample CSV created: sample_data.csv")

def example_challenge_2():
    # Create sample data
    create_sample_csv()
    
    # Run pipeline
    csv_to_parquet_pipeline(
        input_path="sample_data.csv",
        output_path="output_parquet",
        filter_columns=["name", "department"]  # Only filter these specific columns
    )

# Challenge 3: Window Function for Ranking Salespeople

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col, rank, dense_rank, row_number

def rank_salespeople(df):
    """
    Rank salespeople based on total sales by region using window functions
    
    Args:
        df: DataFrame with sales data containing columns: 
            salesperson_id, salesperson_name, region, sales_amount, date
    
    Returns:
        DataFrame with rankings
    """
    # Calculate total sales per salesperson per region
    sales_totals = df.groupBy("salesperson_id", "salesperson_name", "region") \
                    .agg(sum("sales_amount").alias("total_sales"))
    
    # Define window specification - partition by region, order by total sales descending
    window_spec = Window.partitionBy("region").orderBy(col("total_sales").desc())
    
    # Add different ranking types
    ranked_df = sales_totals \
        .withColumn("rank", rank().over(window_spec)) \
        .withColumn("dense_rank", dense_rank().over(window_spec)) \
        .withColumn("row_number", row_number().over(window_spec)) \
        .withColumn("sales_percentage", 
                   (col("total_sales") / sum("total_sales").over(Window.partitionBy("region"))) * 100)
    
    return ranked_df

# Alternative implementation with multiple ranking strategies
def comprehensive_sales_ranking(df, ranking_method="rank"):
    """
    Comprehensive sales ranking with different ranking methods
    
    Args:
        df: Input sales DataFrame
        ranking_method: "rank", "dense_rank", or "row_number"
    """
    # Calculate total sales
    sales_totals = df.groupBy("salesperson_id", "salesperson_name", "region") \
                    .agg(sum("sales_amount").alias("total_sales"))
    
    window_spec = Window.partitionBy("region").orderBy(col("total_sales").desc())
    
    if ranking_method == "rank":
        ranked_df = sales_totals.withColumn("ranking", rank().over(window_spec))
    elif ranking_method == "dense_rank":
        ranked_df = sales_totals.withColumn("ranking", dense_rank().over(window_spec))
    else:  # row_number
        ranked_df = sales_totals.withColumn("ranking", row_number().over(window_spec))
    
    return ranked_df.orderBy("region", "ranking")

def example_challenge_3():
    spark = SparkSession.builder.appName("SalesRanking").getOrCreate()
    
    # Sample sales data
    sales_data = [
        (1, "John Doe", "North", 1000.0, "2024-01-01"),
        (1, "John Doe", "North", 1500.0, "2024-01-02"),
        (2, "Jane Smith", "North", 2000.0, "2024-01-01"),
        (2, "Jane Smith", "North", 1200.0, "2024-01-02"),
        (3, "Bob Johnson", "South", 1800.0, "2024-01-01"),
        (3, "Bob Johnson", "South", 2200.0, "2024-01-02"),
        (4, "Alice Brown", "South", 1600.0, "2024-01-01"),
        (4, "Alice Brown", "South", 1400.0, "2024-01-02"),
        (5, "Charlie Wilson", "North", 900.0, "2024-01-01"),
        (6, "Diana Lee", "South", 3000.0, "2024-01-01")
    ]
    
    columns = ["salesperson_id", "salesperson_name", "region", "sales_amount", "date"]
    sales_df = spark.createDataFrame(sales_data, columns)
    
    print("Original Sales Data:")
    sales_df.show()
    
    # Apply ranking
    ranked_sales = rank_salespeople(sales_df)
    print("Salespeople Ranking by Region:")
    ranked_sales.show()
    
    # Show different ranking methods
    print("Using Rank (standard competition ranking):")
    rank_df = comprehensive_sales_ranking(sales_df, "rank")
    rank_df.show()
    
    print("Using Dense Rank:")
    dense_df = comprehensive_sales_ranking(sales_df, "dense_rank")
    dense_df.show()
    
    spark.stop()

# Challenge 4: SQL Query for Average Salary by Department

from pyspark.sql import SparkSession

def average_salary_by_department(spark, employees_df, experience_threshold=3):
    """
    Calculate average salary by department for employees with more than specified years of experience
    
    Args:
        spark: SparkSession
        employees_df: DataFrame with employee data
        experience_threshold: Minimum years of experience required
    
    Returns:
        DataFrame with average salary by department
    """
    # Create temporary view
    employees_df.createOrReplaceTempView("employees")
    
    # SQL query
    sql_query = f"""
    SELECT 
        department,
        COUNT(*) as employee_count,
        ROUND(AVG(salary), 2) as average_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary
    FROM employees
    WHERE years_experience > {experience_threshold}
    GROUP BY department
    ORDER BY average_salary DESC
    """
    
    return spark.sql(sql_query)

# Alternative DataFrame API implementation
def average_salary_dataframe_api(df, experience_threshold=3):
    """
    Same functionality using DataFrame API instead of SQL
    """
    from pyspark.sql.functions import avg, count, min, max, round
    
    return df.filter(df.years_experience > experience_threshold) \
            .groupBy("department") \
            .agg(
                count("*").alias("employee_count"),
                round(avg("salary"), 2).alias("average_salary"),
                min("salary").alias("min_salary"),
                max("salary").alias("max_salary")
            ) \
            .orderBy("average_salary", ascending=False)

def example_challenge_4():
    spark = SparkSession.builder.appName("SalaryAnalysis").getOrCreate()
    
    # Sample employee data
    employee_data = [
        (1, "John Doe", "Engineering", 5, 75000),
        (2, "Jane Smith", "Engineering", 2, 65000),  # Excluded (exp <= 3)
        (3, "Bob Johnson", "Marketing", 4, 60000),
        (4, "Alice Brown", "Marketing", 6, 62000),
        (5, "Charlie Wilson", "HR", 7, 55000),
        (6, "Diana Lee", "Engineering", 8, 85000),
        (7, "Eve Davis", "HR", 1, 45000),  # Excluded (exp <= 3)
        (8, "Frank Miller", "Finance", 5, 70000),
        (9, "Grace Wilson", "Finance", 4, 68000),
        (10, "Henry Moore", "Marketing", 2, 52000)  # Excluded (exp <= 3)
    ]
    
    columns = ["emp_id", "name", "department", "years_experience", "salary"]
    employees_df = spark.createDataFrame(employee_data, columns)
    
    print("Employee Data:")
    employees_df.show()
    
    # Using SQL approach
    print("Average Salary by Department (SQL approach - experience > 3 years):")
    result_sql = average_salary_by_department(spark, employees_df, 3)
    result_sql.show()
    
    # Using DataFrame API approach
    print("Average Salary by Department (DataFrame API - experience > 3 years):")
    result_df = average_salary_dataframe_api(employees_df, 3)
    result_df.show()
    
    # Different experience threshold
    print("Average Salary by Department (experience > 5 years):")
    result_high_exp = average_salary_by_department(spark, employees_df, 5)
    result_high_exp.show()
    
    spark.stop()

# Challenge 5: Split DataFrame by Column Value

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def split_dataframe_by_column(df, split_column, output_base_path=None):
    """
    Split a large DataFrame into smaller DataFrames based on a specific column value
    
    Args:
        df: Input DataFrame
        split_column: Column name to split by
        output_base_path: Base path to write split DataFrames (optional)
    
    Returns:
        Dictionary of {column_value: DataFrame} if output_path is None,
        otherwise writes to disk and returns file paths
    """
    # Get distinct values of the split column
    distinct_values = df.select(split_column).distinct().collect()
    distinct_values = [row[split_column] for row in distinct_values]
    
    print(f"Splitting DataFrame by '{split_column}' into {len(distinct_values)} parts")
    print(f"Distinct values: {distinct_values}")
    
    result_dataframes = {}
    
    for value in distinct_values:
        # Filter DataFrame for each distinct value
        filtered_df = df.filter(col(split_column) == value)
        result_dataframes[value] = filtered_df
        
        # Write to disk if output path provided
        if output_base_path:
            output_path = os.path.join(output_base_path, f"{split_column}={value}")
            filtered_df.write.mode("overwrite").parquet(output_path)
            print(f"Written: {output_path}")
    
    if output_base_path:
        return {"output_base_path": output_base_path, "split_count": len(distinct_values)}
    else:
        return result_dataframes

def split_dataframe_with_partitioning(df, split_column, max_partitions=None):
    """
    Split DataFrame with additional partitioning control
    """
    from pyspark.sql.functions import spark_partition_id
    
    # Repartition by the split column for better performance
    if max_partitions:
        df_repartitioned = df.repartition(max_partitions, split_column)
    else:
        df_repartitioned = df.repartition(split_column)
    
    # Get partition information
    partition_info = df_repartitioned \
        .withColumn("partition_id", spark_partition_id()) \
        .groupBy(split_column, "partition_id") \
        .count() \
        .orderBy(split_column, "partition_id")
    
    return df_repartitioned, partition_info

def example_challenge_5():
    spark = SparkSession.builder.appName("DataFrameSplitter").getOrCreate()
    
    # Create sample large DataFrame
    data = []
    departments = ["Engineering", "Marketing", "HR", "Finance", "Sales"]
    locations = ["NY", "CA", "TX", "IL", "FL"]
    
    for i in range(1000):
        dept = departments[i % len(departments)]
        loc = locations[i % len(locations)]
        salary = 50000 + (i % 10) * 5000
        data.append((i + 1, f"Employee_{i+1}", dept, loc, salary))
    
    columns = ["emp_id", "name", "department", "location", "salary"]
    large_df = spark.createDataFrame(data, columns)
    
    print("Original DataFrame:")
    print(f"Total rows: {large_df.count()}")
    print(f"Partitions: {large_df.rdd.getNumPartitions()}")
    large_df.show(10)
    
    # Split by department (in memory)
    print("\n1. Splitting by department (in-memory):")
    split_dfs = split_dataframe_by_column(large_df, "department")
    
    for dept, dept_df in split_dfs.items():
        print(f"Department {dept}: {dept_df.count()} employees")
        dept_df.show(5)
    
    # Split by location and write to disk
    print("\n2. Splitting by location (writing to disk):")
    output_info = split_dataframe_by_column(
        large_df, 
        "location", 
        output_base_path="split_output"
    )
    
    print(f"Split completed: {output_info}")
    
    # Verify the written data
    print("\n3. Verifying written data:")
    for loc in locations:
        path = f"split_output/location={loc}"
        if os.path.exists(path):
            loc_df = spark.read.parquet(path)
            print(f"Location {loc}: {loc_df.count()} records")
    
    # Advanced partitioning
    print("\n4. Advanced partitioning:")
    partitioned_df, part_info = split_dataframe_with_partitioning(large_df, "department", 10)
    print("Partition distribution:")
    part_info.show()
    
    print(f"Repartitioned DataFrame partitions: {partitioned_df.rdd.getNumPartitions()}")
    
    spark.stop()

# Utility function to clean up generated files
def cleanup_files():
    """Clean up generated files from examples"""
    import shutil
    folders_to_remove = ["split_output", "output_parquet"]
    files_to_remove = ["sample_data.csv"]
    
    for folder in folders_to_remove:
        if os.path.exists(folder):
            shutil.rmtree(folder)
            print(f"Removed: {folder}")
    
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
            print(f"Removed: {file}")

# Run all examples
if __name__ == "__main__":
    print("=== PySpark Challenges Solutions ===")
    
    # Run each challenge example
    example_challenge_1()
    example_challenge_2() 
    example_challenge_3()
    example_challenge_4()
    example_challenge_5()
    
    # Cleanup
    cleanup_files()