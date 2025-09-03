# Employees with salary > employee ID 103

%sql
SELECT first_name, last_name FROM employees WHERE salary > (SELECT salary FROM employees WHERE employee_id = 103);

%python
salary_103 = employees.filter(col("employee_id") == 103).select("salary").first()[0]
result_1 = employees.filter(col("salary") > salary_103).select("first_name", "last_name")

# Employees with same job as employee ID 169

%sql
SELECT first_name, last_name, salary, department_id, job_id FROM employees WHERE job_id = (SELECT job_id FROM employees WHERE employee_id = 169);

%python
job_169 = employees.filter(col("employee_id") == 169).select("job_id").first()[0]
result_2 = employees.filter(col("job_id") == job_169).select("first_name", "last_name", "salary", "department_id", "job_id")

# Employees earning smallest department salary

%sql
SELECT first_name, last_name, salary, department_id FROM employees WHERE salary IN (SELECT MIN(salary) FROM employees GROUP BY department_id);

%python
min_salaries = employees.groupBy("department_id").agg(min("salary").alias("min_salary"))
result_3 = employees.join(min_salaries, 
                         (employees.department_id == min_salaries.department_id) & 
                         (employees.salary == min_salaries.min_salary))
                     .select("first_name", "last_name", "salary", "department_id")

# Employees earning more than average salary

%sql
SELECT employee_id, first_name, last_name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees);

%python
avg_salary = employees.agg(avg("salary")).first()[0]
result_4 = employees.filter(col("salary") > avg_salary).select("employee_id", "first_name", "last_name")

# Employees reporting to Payam

%sql
SELECT first_name, last_name, employee_id, salary FROM employees WHERE manager_id = (SELECT employee_id FROM employees WHERE first_name = 'Payam');

%python
payam_id = employees.filter(col("first_name") == "Payam").select("employee_id").first()[0]
result_5 = employees.filter(col("manager_id") == payam_id).select("first_name", "last_name", "employee_id", "salary")

# Employees in Finance department

%sql
SELECT e.department_id, e.first_name, e.last_name, e.job_id, d.department_name FROM employees e JOIN departments d ON e.department_id = d.department_id WHERE d.department_name = 'Finance';

%python
result_6 = (employees
    .join(departments, employees.department_id == departments.department_id)
    .filter(col("department_name") == "Finance")
    .select(employees.department_id, "first_name", "last_name", "job_id", "department_name"))

# Employee with salary 3000 and manager 121

%sql
SELECT * FROM employees WHERE (salary, manager_id) = (SELECT 3000, 121 FROM dual);

%python
result_7 = employees.filter((col("salary") == 3000) & (col("manager_id") == 121))

# Employees with IDs 134, 159, 183

%sql
SELECT * FROM employees WHERE employee_id IN (134, 159, 183);

%python
result_8 = employees.filter(col("employee_id").isin([134, 159, 183]))

# Employees with salary between 1000 and 3000

%sql
SELECT * FROM employees WHERE salary BETWEEN 1000 AND 3000;

%python
result_9 = employees.filter(col("salary").between(1000, 3000))

# Employees with salary between min and 2500

%sql
SELECT * FROM employees WHERE salary BETWEEN (SELECT MIN(salary) FROM employees) AND 2500;

%python
min_sal = employees.agg(min("salary")).first()[0]
result_10 = employees.filter(col("salary").between(min_sal, 2500))

# Employees not in departments with managers 100-200

%sql
SELECT * FROM employees WHERE department_id NOT IN (
SELECT department_id FROM departments WHERE manager_id BETWEEN 100 AND 200);

%python
dept_ids = departments.filter(col("manager_id").between(100, 200)).select("department_id")
result_11 = employees.filter(~col("department_id").isin([row.department_id for row in dept_ids.collect()]))

# Employees with second highest salary

%sql
SELECT * FROM employees WHERE employee_id IN (
SELECT employee_id FROM employees WHERE salary = (
SELECT MAX(salary) FROM employeesWHERE salary < (SELECT MAX(salary) FROM employees)));

%python
max_salary = employees.agg(max("salary")).first()[0]
second_max = employees.filter(col("salary") < max_salary).agg(max("salary")).first()[0]
result_12 = employees.filter(col("salary") == second_max)

# Employees in Clara's department excluding Clara

%sql
SELECT first_name, last_name, hire_date FROM employees WHERE department_id = (SELECT department_id FROM employees WHERE first_name = 'Clara') AND first_name <> 'Clara';

%python
clara_dept = employees.filter(col("first_name") == "Clara").select("department_id").first()[0]
result_13 = employees.filter((col("department_id") == clara_dept) & (col("first_name") != "Clara"))\
                    .select("first_name", "last_name", "hire_date")

# Employees in departments with names containing 'T'

%sql
SELECT employee_id, first_name, last_name FROM employees WHERE department_id IN (
SELECT department_id FROM employees WHERE first_name LIKE '%T%');

%python
depts_with_t = employees.filter(col("first_name").like("%T%")).select("department_id").distinct()
result_14 = employees.join(depts_with_t, "department_id").select("employee_id", "first_name", "last_name")

# High earners in departments with 'J' names

%sql
SELECT employee_id, first_name, last_name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) AND department_id IN (
SELECT department_id FROM employees WHERE first_name LIKE '%J%');

%python
avg_sal = employees.agg(avg("salary")).first()[0]
depts_with_j = employees.filter(col("first_name").like("%J%")).select("department_id").distinct()
result_15 = employees.filter(col("salary") > avg_sal)\
                    .join(depts_with_j, "department_id")\
                    .select("employee_id", "first_name", "last_name", "salary")

# Employees in Toronto location

%sql
SELECT first_name, last_name, employee_id, job_id FROM employees WHERE department_id = (
SELECT department_id FROM departments WHERE location_id = (SELECT location_id FROM locations WHERE city = 'Toronto'));

%python
toronto_loc = locations.filter(col("city") == "Toronto").select("location_id").first()[0]
toronto_dept = departments.filter(col("location_id") == toronto_loc).select("department_id").first()[0]
result_16 = employees.filter(col("department_id") == toronto_dept)\
                    .select("first_name", "last_name", "employee_id", "job_id")

# Employees earning less than any MK_MAN

%sql
SELECT employee_id, first_name, last_name, job_id FROM employees WHERE salary < ANY (SELECT salary FROM employees WHERE job_id = 'MK_MAN');

%python
mk_man_salaries = employees.filter(col("job_id") == "MK_MAN").select("salary")
max_mk_man = mk_man_salaries.agg(max("salary")).first()[0]
result_17 = employees.filter(col("salary") < max_mk_man).select("employee_id", "first_name", "last_name", "job_id")

# Employees earning less than any MK_MAN, excluding MK_MAN

%sql
SELECT employee_id, first_name, last_name, job_id FROM employees WHERE salary < ANY (SELECT salary FROM employees WHERE job_id = 'MK_MAN') AND job_id <> 'MK_MAN';

%python
result_18 = result_17.filter(col("job_id") != "MK_MAN")

# Employees earning more than all PU_MAN, excluding PU_MAN

%sql
SELECT employee_id, first_name, last_name, job_id FROM employees WHERE salary > ALL (SELECT salary FROM employees WHERE job_id = 'PU_MAN') AND job_id <> 'PU_MAN';

%python
pu_man_salaries = employees.filter(col("job_id") == "PU_MAN").select("salary")
min_pu_man = pu_man_salaries.agg(min("salary")).first()[0]
result_19 = employees.filter((col("salary") > min_pu_man) & (col("job_id") != "PU_MAN"))\
                    .select("employee_id", "first_name", "last_name", "job_id")

# Employees earning more than all department averages

%sql
SELECT employee_id, first_name, last_name, job_id FROM employees WHERE salary > ALL (
SELECT AVG(salary) FROM employees GROUP BY department_id);

%python
dept_avg = employees.groupBy("department_id").agg(avg("salary").alias("avg_salary"))
max_dept_avg = dept_avg.agg(max("avg_salary")).first()[0]
result_20 = employees.filter(col("salary") > max_dept_avg).select("employee_id", "first_name", "last_name", "job_id")

# Employees if any employee earns > 3700

%sql
SELECT first_name, last_name, department_id FROM employees WHERE EXISTS (SELECT 1 FROM employees WHERE salary > 3700);

%python
has_high_earner = employees.filter(col("salary") > 3700).count() > 0
result_21 = employees.select("first_name", "last_name", "department_id") if has_high_earner else spark.createDataFrame([], schema)

# Departments with total salary (at least one employee)

%sql
SELECT d.department_id, SUM(e.salary) as total_salary FROM departments d JOIN employees e ON d.department_id = e.department_id GROUP BY d.department_id HAVING COUNT(e.employee_id) >= 1;

%python
result_22 = (employees
    .groupBy("department_id")
    .agg(sum("salary").alias("total_salary"), count("employee_id").alias("employee_count"))
    .filter(col("employee_count") >= 1)
    .select("department_id", "total_salary"))

# Employees with modified job titles

%sql
SELECT employee_id, first_name, last_name,
CASE job_id WHEN 'ST_MAN' THEN 'SALESMAN' WHEN 'IT_PROG' THEN 'DEVELOPER' ELSE job_id END AS designation, salary FROM employees;

%python
result_23 = employees.withColumn(
    "designation",
    when(col("job_id") == "ST_MAN", "SALESMAN")
    .when(col("job_id") == "IT_PROG", "DEVELOPER")
    .otherwise(col("job_id"))
).select("employee_id", "first_name", "last_name", "designation", "salary")

# Employees with HIGH/LOW salary status

%sql
SELECT employee_id, first_name, last_name, salary, CASE WHEN salary >= (SELECT AVG(salary) FROM employees) THEN 'HIGH' ELSE 'LOW' END AS SalaryStatus FROM employees;

%python
avg_sal = employees.agg(avg("salary")).first()[0]
result_24 = employees.withColumn(
    "SalaryStatus",
    when(col("salary") >= avg_sal, "HIGH").otherwise("LOW")).select("employee_id", "first_name", "last_name", "salary", "SalaryStatus")

# Employees with detailed salary comparison

%sql
SELECT employee_id, first_name, last_name, salary AS SalaryDrawn, ROUND(salary - (SELECT AVG(salary) FROM employees), 2) AS AvgCompare,
CASE WHEN salary >= (SELECT AVG(salary) FROM employees) THEN 'HIGH' ELSE 'LOW' END AS SalaryStatus FROM employees;

%python
result_25 = employees.withColumn("AvgCompare", round(col("salary") - avg_sal, 2)).withColumn("SalaryStatus", when(col("salary") >= avg_sal, "HIGH").otherwise("LOW")).select("employee_id", "first_name", "last_name", 
         col("salary").alia("SalaryDrawn"), "AvgCompare", "SalaryStatus")
