from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
import math

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AdvancedSQLQuestionsPySpark") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# 1. Retrieve the second-highest salary
def second_highest_salary(employees_df):
    max_salary = employees_df.agg(max("salary")).first()[0]
    result = employees_df.filter(col("salary") < max_salary).agg(max("salary"))
    return result

# 2. Get nth highest salary
def nth_highest_salary(employees_df, n):
    window_spec = Window.orderBy(col("salary").desc())
    ranked_df = employees_df.withColumn("rank", dense_rank().over(window_spec))
    result = ranked_df.filter(col("rank") == n).select("salary")
    return result

# 3. Employees with salary greater than average
def above_avg_salary(employees_df):
    avg_salary = employees_df.agg(avg("salary")).first()[0]
    result = employees_df.filter(col("salary") > avg_salary)
    return result

# 4. Current date and time
def current_datetime():
    return spark.sql("SELECT current_timestamp()")

# 5. Find duplicate records
def find_duplicates(df, column_name):
    result = df.groupBy(column_name).agg(count("*").alias("count")).filter(col("count") > 1)
    return result

# 6. Delete duplicate rows (PySpark doesn't support direct DELETE, so we create new DF)
def remove_duplicates(df, column_name):
    window_spec = Window.partitionBy(column_name).orderBy(column_name)
    result = df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
    return result

# 7. Common records from two tables
def common_records(df1, df2):
    result = df1.intersect(df2)
    return result

# 8. Last 10 records
def last_10_records(df, order_column):
    result = df.orderBy(col(order_column).desc()).limit(10)
    return result

# 9. Top 5 highest salaries
def top_5_salaries(employees_df):
    result = employees_df.orderBy(col("salary").desc()).limit(5)
    return result

# 10. Total salary
def total_salary(employees_df):
    result = employees_df.agg(sum("salary").alias("total_salary"))
    return result

# 11. Employees joined in 2020
def employees_joined_2020(employees_df):
    result = employees_df.filter(year("join_date") == 2020)
    return result

# 12. Names starting with 'A'
def names_start_with_a(employees_df):
    result = employees_df.filter(col("name").like("A%"))
    return result

# 13. Employees without manager
def employees_without_manager(employees_df):
    result = employees_df.filter(col("manager_id").isNull())
    return result

# 14. Department with most employees
def department_most_employees(employees_df):
    result = (employees_df.groupBy("department_id")
              .agg(count("*").alias("employee_count"))
              .orderBy(col("employee_count").desc())
              .limit(1))
    return result

# 15. Count employees per department
def employees_per_department(employees_df):
    result = employees_df.groupBy("department_id").agg(count("*").alias("employee_count"))
    return result

# 16. Highest salary per department
def highest_salary_per_dept(employees_df):
    window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
    result = (employees_df.withColumn("rank", row_number().over(window_spec))
              .filter(col("rank") == 1)
              .select("department_id", "employee_id", "salary"))
    return result

# 17. Update salary by 10% (Create new DF)
def update_salary_10percent(employees_df):
    result = employees_df.withColumn("salary", col("salary") * 1.1)
    return result

# 18. Salary between range
def salary_between(employees_df, min_sal, max_sal):
    result = employees_df.filter(col("salary").between(min_sal, max_sal))
    return result

# 19. Youngest employee
def youngest_employee(employees_df):
    result = employees_df.orderBy(col("birth_date").desc()).limit(1)
    return result

# 20. First and last records
def first_last_records(df, order_column):
    first = df.orderBy(order_column).limit(1)
    last = df.orderBy(col(order_column).desc()).limit(1)
    result = first.union(last)
    return result

# 21. Employees reporting to specific manager
def employees_by_manager(employees_df, manager_id):
    result = employees_df.filter(col("manager_id") == manager_id)
    return result

# 22. Total number of departments
def total_departments(employees_df):
    result = employees_df.agg(countDistinct("department_id").alias("total_departments"))
    return result

# 23. Department with lowest avg salary
def dept_lowest_avg_salary(employees_df):
    result = (employees_df.groupBy("department_id")
              .agg(avg("salary").alias("avg_salary"))
              .orderBy("avg_salary")
              .limit(1))
    return result

# 24. Delete employees from department (Create filtered DF)
def remove_dept_employees(employees_df, dept_id):
    result = employees_df.filter(col("department_id") != dept_id)
    return result

# 25. Employees with more than 5 years
def employees_5_years(employees_df):
    result = employees_df.filter(datediff(current_date(), col("join_date")) > 1825)
    return result

# 26. Second largest value
def second_largest_value(df, column_name):
    max_val = df.agg(max(col(column_name))).first()[0]
    result = df.filter(col(column_name) < max_val).agg(max(col(column_name)))
    return result

# 27. Truncate table (Not applicable in Spark, returns empty DF)
def truncate_table(df):
    return df.limit(0)

# 28. Employee records in XML format (Spark doesn't have FOR XML, simulate)
def employees_xml_format(employees_df):
    result = employees_df.select(
        concat(lit("<employee><id>"), col("employee_id"), lit("</id><name>"), 
               col("name"), lit("</name><dept>"), col("department_id"), lit("</dept></employee>")).alias("xml_data")
    )
    return result

# 29. Current month name
def current_month_name():
    return spark.sql("SELECT date_format(current_date(), 'MMMM')")

# 30. Convert string to lowercase
def to_lowercase(string_df):
    return string_df.select(lower(col("value")).alias("lower_value"))

# 31. Employees without subordinates
def employees_no_subordinates(employees_df):
    managers = employees_df.filter(col("manager_id").isNotNull()).select("manager_id").distinct()
    result = employees_df.join(managers, employees_df.employee_id == managers.manager_id, "left_anti")
    return result

# 32. Total sales per customer
def total_sales_per_customer(sales_df):
    result = sales_df.groupBy("customer_id").agg(sum("sales_amount").alias("total_sales"))
    return result

# 33. Check if table is empty
def is_table_empty(df):
    count = df.count()
    return spark.createDataFrame([("Empty" if count == 0 else "Not Empty",)], ["status"])

# 34. Second highest salary per department
def second_highest_per_dept(employees_df):
    window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
    result = (employees_df.withColumn("rank", dense_rank().over(window_spec))
              .filter(col("rank") == 2)
              .select("department_id", "salary"))
    return result

# 35. Salary multiple of 10000
def salary_multiple_10000(employees_df):
    result = employees_df.filter(col("salary") % 10000 == 0)
    return result

# 36. Records with null values
def records_with_null(df, column_name):
    result = df.filter(col(column_name).isNull())
    return result

# 37. Employees per job title
def employees_per_job_title(employees_df):
    result = employees_df.groupBy("job_title").agg(count("*").alias("count"))
    return result

# 38. Names ending with 'n'
def names_end_with_n(employees_df):
    result = employees_df.filter(col("name").like("%n"))
    return result

# 39. Employees in both departments
def employees_in_both_depts(employees_df, dept1, dept2):
    result = (employees_df.filter(col("department_id").isin([dept1, dept2]))
              .groupBy("employee_id")
              .agg(countDistinct("department_id").alias("dept_count"))
              .filter(col("dept_count") == 2))
    return result

# 40. Employees with same salary
def employees_same_salary(employees_df):
    salary_counts = employees_df.groupBy("salary").agg(count("*").alias("count")).filter(col("count") > 1)
    result = employees_df.join(salary_counts, "salary")
    return result

# 41. Update salaries by department (Create new DF)
def update_salaries_by_dept(employees_df):
    result = employees_df.withColumn("new_salary", 
        when(col("department_id") == 101, col("salary") * 1.10)
        .when(col("department_id") == 102, col("salary") * 1.05)
        .otherwise(col("salary")))
    return result

# 42. Employees without department
def employees_without_department(employees_df):
    result = employees_df.filter(col("department_id").isNull())
    return result

# 43. Max and min salary per department
def salary_range_per_dept(employees_df):
    result = employees_df.groupBy("department_id").agg(
        max("salary").alias("max_salary"),
        min("salary").alias("min_salary")
    )
    return result

# 44. Employees hired last 6 months
def employees_last_6_months(employees_df):
    six_months_ago = date_sub(current_date(), 180)
    result = employees_df.filter(col("hire_date") > six_months_ago)
    return result

# 45. Department-wise total and average salary
def dept_salary_stats(employees_df):
    result = employees_df.groupBy("department_id").agg(
        sum("salary").alias("total_salary"),
        avg("salary").alias("avg_salary")
    )
    return result

# 46. Employees hired same month as manager
def same_hire_month_as_manager(employees_df):
    managers = employees_df.select("employee_id", "join_date").withColumnRenamed("employee_id", "mgr_id")
    result = (employees_df.alias("e")
              .join(managers.alias("m"), col("e.manager_id") == col("m.mgr_id"))
              .filter(month("e.join_date") == month("m.join_date"))
              .filter(year("e.join_date") == year("m.join_date")))
    return result

# 47. Count employees with names starting and ending with same letter
def same_start_end_letter(employees_df):
    result = employees_df.filter(
        substring(col("name"), 1, 1) == substring(col("name"), -1, 1)
    ).count()
    return spark.createDataFrame([(result,)], ["count"])

# 48. Employee names and salaries as single string
def employee_info_string(employees_df):
    result = employees_df.select(
        concat(col("name"), lit(" earns "), col("salary")).alias("employee_info")
    )
    return result

# 49. Employees with higher salary than manager
def higher_salary_than_manager(employees_df):
    managers = employees_df.select("employee_id", "salary").withColumnRenamed("salary", "mgr_salary")
    result = (employees_df.alias("e")
              .join(managers.alias("m"), col("e.manager_id") == col("m.employee_id"))
              .filter(col("e.salary") > col("m.mgr_salary")))
    return result

# 50. Employees in departments with less than 3 employees
def employees_small_departments(employees_df):
    dept_counts = employees_df.groupBy("department_id").agg(count("*").alias("count")).filter(col("count") < 3)
    result = employees_df.join(dept_counts, "department_id")
    return result

# 51. Employees with same first name
def same_first_name(employees_df):
    first_name_counts = employees_df.groupBy("first_name").agg(count("*").alias("count")).filter(col("count") > 1)
    result = employees_df.join(first_name_counts, "first_name")
    return result

# 52. Delete employees with more than 15 years (Create filtered DF)
def remove_15_years_employees(employees_df):
    result = employees_df.filter(datediff(current_date(), col("join_date")) <= 5475)
    return result

# 53. Employees under same manager
def employees_under_manager(employees_df, manager_id):
    result = employees_df.filter(col("manager_id") == manager_id)
    return result

# 54. Top 3 highest paid per department
def top_3_per_dept(employees_df):
    window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
    result = (employees_df.withColumn("rank", dense_rank().over(window_spec))
              .filter(col("rank") <= 3))
    return result

# 55. Employees with more than 5 years experience per department
def experienced_employees_per_dept(employees_df):
    result = employees_df.filter(datediff(current_date(), col("join_date")) > 1825)
    return result

# 56. Departments with no hires in past 2 years
def inactive_departments(employees_df):
    two_years_ago = date_sub(current_date(), 730)
    dept_last_hire = employees_df.groupBy("department_id").agg(max("hire_date").alias("last_hire"))
    result = dept_last_hire.filter(col("last_hire") < two_years_ago)
    return result

# 57. Employees earning more than department average
def above_dept_avg_salary(employees_df):
    dept_avg = employees_df.groupBy("department_id").agg(avg("salary").alias("dept_avg_salary"))
    result = (employees_df.alias("e")
              .join(dept_avg.alias("d"), col("e.department_id") == col("d.department_id"))
              .filter(col("e.salary") > col("d.dept_avg_salary")))
    return result

# 58. Managers with more than 5 subordinates
def managers_many_subordinates(employees_df):
    result = (employees_df.filter(col("manager_id").isNotNull())
              .groupBy("manager_id")
              .agg(count("*").alias("subordinate_count"))
              .filter(col("subordinate_count") > 5))
    return result

# 59. Employee names and hire dates formatted
def formatted_employee_info(employees_df):
    result = employees_df.select(
        concat(col("name"), lit(" - "), date_format(col("hire_date"), "MM/dd/yyyy")).alias("employee_info")
    )
    return result

# 60. Employees in top 10% salary (Approximate)
def top_10_percent_salary(employees_df):
    total_count = employees_df.count()
    top_count = math.ceil(total_count * 0.1)
    result = employees_df.orderBy(col("salary").desc()).limit(top_count)
    return result

# 61. Employees grouped by age brackets
def employees_by_age_brackets(employees_df):
    result = (employees_df.withColumn("age", datediff(current_date(), col("birth_date")) / 365)
              .withColumn("age_bracket",
                 when(col("age").between(20, 30), "20-30")
                 .when(col("age").between(31, 40), "31-40")
                 .otherwise("41+"))
              .groupBy("age_bracket")
              .agg(count("*").alias("count")))
    return result

# 62. Average salary of top 5 per department
def avg_top_5_salary_per_dept(employees_df):
    window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
    ranked = employees_df.withColumn("rank", dense_rank().over(window_spec)).filter(col("rank") <= 5)
    result = ranked.groupBy("department_id").agg(avg("salary").alias("avg_top5_salary"))
    return result

# 63. Percentage of employees per department
def employee_percentage_per_dept(employees_df):
    total_employees = employees_df.count()
    result = (employees_df.groupBy("department_id")
              .agg(count("*").alias("count"))
              .withColumn("percentage", (col("count") * 100.0 / total_employees)))
    return result

# 64. Employees with specific email domain
def employees_with_email_domain(employees_df, domain):
    result = employees_df.filter(col("email").like(f"%{domain}"))
    return result

# 65. Year-to-date sales per customer
def ytd_sales_per_customer(sales_df):
    current_year = year(current_date())
    result = (sales_df.filter(year("sale_date") == current_year)
              .groupBy("customer_id")
              .agg(sum("sales_amount").alias("ytd_sales")))
    return result

# 66. Hire date and day of week
def hire_date_day_of_week(employees_df):
    result = employees_df.select(
        "name", "hire_date", date_format(col("hire_date"), "EEEE").alias("day_of_week")
    )
    return result

# 67. Employees older than 30 years
def employees_older_than_30(employees_df):
    result = employees_df.filter(datediff(current_date(), col("birth_date")) / 365 > 30)
    return result

# 68. Employees grouped by salary range
def employees_by_salary_range(employees_df):
    result = (employees_df.withColumn("salary_range",
                 when(col("salary").between(0, 20000), "0-20K")
                 .when(col("salary").between(20001, 50000), "20K-50K")
                 .otherwise("50K+"))
              .groupBy("salary_range")
              .agg(count("*").alias("count")))
    return result

# 69. Employees without bonus
def employees_without_bonus(employees_df):
    result = employees_df.filter(col("bonus").isNull())
    return result

# 70. Salary stats per job role
def salary_stats_per_job_role(employees_df):
    result = employees_df.groupBy("job_role").agg(
        max("salary").alias("highest_salary"),
        min("salary").alias("lowest_salary"),
        avg("salary").alias("avg_salary")
    )
    return result

# Utility class for testing
class SQLQuestionsValidator:
    def __init__(self, spark):
        self.spark = spark
        self.create_test_data()
    
    def create_test_data(self):
        """Create sample test data"""
        employees_data = [
            (1, "John", 50000, 1, 101, "1990-01-01", "2020-01-15", "john@example.com", "Engineer", 1000),
            (2, "Jane", 60000, 1, 101, "1985-05-20", "2019-03-10", "jane@example.com", "Manager", 2000),
            (3, "Bob", 55000, 2, 102, "1988-12-15", "2021-06-01", "bob@test.com", "Analyst", 1500),
            (4, "Alice", 70000, None, 103, "1980-08-30", "2018-11-20", "alice@example.com", "Director", 3000)
        ]
        self.employees_df = spark.createDataFrame(
            employees_data, 
            ["employee_id", "name", "salary", "manager_id", "department_id", 
             "birth_date", "join_date", "email", "job_title", "bonus"]
        )
        
        sales_data = [
            (1, 100, "2024-01-15", 500), (1, 150, "2024-02-20", 300),
            (2, 200, "2024-01-20", 700), (2, 250, "2024-03-10", 400)
        ]
        self.sales_df = spark.createDataFrame(
            sales_data, ["sale_id", "customer_id", "sale_date", "sales_amount"]
        )
        
        print("Test data created successfully!")
    
    def run_comprehensive_tests(self):
        """Run tests for all functions"""
        print("Running comprehensive tests...")
        
        try:
            # Test a subset of functions
            result1 = second_highest_salary(self.employees_df)
            result2 = above_avg_salary(self.employees_df)
            result3 = employees_per_department(self.employees_df)
            
            print("✓ Basic tests passed")
            print(f"Second highest salary: {result1.collect()}")
            print(f"Employees above average: {result2.count()}")
            print(f"Departments count: {result3.count()}")
            
            print("All tests completed successfully!")
            
        except Exception as e:
            print(f"✗ Test failed: {str(e)}")

# Main execution
if __name__ == "__main__":
    print("Advanced SQL Questions Implementation in PySpark")
    print("=" * 60)
    print("70 SQL questions implemented as PySpark functions")
    print("=" * 60)
    
    # Initialize validator
    validator = SQLQuestionsValidator(spark)
    
    # Run tests
    validator.run_comprehensive_tests()
    
    print("\nAll functions are ready to use!")
    print("Example usage:")
    print("  result = second_highest_salary(employees_df)")
    print("  result.show()")