Find customers who never placed any order

%sql
select cust_id, c.name from customer c left join orders o on c.cust_id = o.cust_id where o.order_id is null;

%python
from pyspark.sql.functions import col
result = customers.join(orders, on="customer_id", how="left") \
                   .filter(col("order_id").isNull()) \
                   .select("customer_id", "customer_name", "email")

Find employees whose salary is greater than their manager salary

%sql
select e1.emp_id, e1.emp_name, e2.emp_name as manager_name, e2.salary as manager_salary from emp e1 join emp e2 on e1.manager_id = e2.emp_id where e1.salary > e2.salary;

%python
# Self join
emp_alias = employees.alias("e")
mgr_alias = employees.alias("m")

result = emp_alias.join(mgr_alias, col("e.manager_id") == col("m.emp_id")) \
                  .filter(col("e.salary") > col("m.salary")) \
                  .select(
                      col("e.emp_id"),
                      col("e.emp_name"),
                      col("e.salary").alias("emp_salary"),
                      col("m.emp_name").alias("manager_name"),
                      col("m.salary").alias("manager_salary"))
result.show()

Find products that were never ordered

%sql
select p.product_id, p.product_name from products p left join orders o on p.product_id = o.product_id where o.product_id is null;

%python
ordered_products = order_items.select("product_id").distinct()
result = products.join(ordered_products, on="product_id", how="left_anti")
result.show()

Find total number of orders placed by each customer

%sql
select c.cust_id, c.name, count(o.order_id) as total_orders from customers c left join orders o on c.cust_id = o.cust_id group by c.cust_id, c.name order by total_orders desc;

%python
from pyspark.sql.functions import count

customer_orders = orders.groupBy("customer_id") \
                        .agg(count("order_id").alias("total_orders"))

result = customers.join(customer_orders, on="customer_id", how="left") \
                  .fillna(0, subset=["total_orders"]) \
                  .select("customer_id", "customer_name", "total_orders") \
                  .orderBy(col("total_orders").desc())
result.show()

Display order details along with customer names

%sql
select o.order_id, o.order_date, o.amount, c.cust_id, c.name from orders o join customers c on o.cust_id = c.cust_id;

%python
result = orders.join(customers, on="customer_id", how="inner") \
               .select(
                   "order_id", "order_date", "amount",
                   "customer_id", "customer_name", "email").orderBy(col("order_date").desc())
result.show()

Find pairs of employees working in same department

%sql
select e1.dept_id, e1.emp_name as emp1, emp2.emp_name as emp2 from emp e1 join emp e2 on e1.dept_id = e2.dept_id and e1.emp_id < e2.emp_id order by e1.dept_id;

%python
# Self join with condition to avoid duplicates
e1 = employees.alias("e1")
e2 = employees.alias("e2")

result = e1.join(e2, (col("e1.department") == col("e2.department")) & 
                    (col("e1.emp_id") < col("e2.emp_id"))) \
           .select(
               col("e1.emp_name").alias("employee1"),
               col("e2.emp_name").alias("employee2"),
               col("e1.department"))
           .orderBy("department", "employee1")
result.show()

Find customers who purchased all available products

%sql
select cust_id from orders group by cust_id having count(distinct product_id) = (select count(*) from products):

%python
from pyspark.sql.functions import countDistinct, count as spark_count

total_products = products.count()

customer_product_count = orders.join(order_items, "order_id") \
                               .groupBy("customer_id") \
                               .agg(countDistinct("product_id").alias("products_purchased"))

result = customers.join(customer_product_count, "customer_id", "inner") \
                  .filter(col("products_purchased") == total_products) \
                  .select("customer_id", "customer_name", "products_purchased")

result.show()

Find departments having no employees

%sql
select d.dept_id, d.dept_name from departments d left join emp e on d.dept_id = e.dept_id where e.emp_id is null;

%python
# Left anti join
result = departments.join(employees.select("department_id"), 
                          on="department_id", 
                          how="left_anti")
result.show()

Find latest order details for each customer

%sql
select cust_id, order_id, order_date, amount from (
select o.*, row_number() over (partiton by cust_id order by order_date desc, order_id desc) as rn from orders o) t where rn = 1;

%python
from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("customer_id") \
                    .orderBy(col("order_date").desc(), col("order_id").desc())

result = orders.withColumn("rn", row_number().over(window_spec)) \
               .filter(col("rn") == 1) \
               .select("customer_id", "order_id", "order_date", "amount") \
               .orderBy("customer_id")
result.show()

Display employee names along with manager names

%sql
select e1.emp_id, e1.emp_name as employee_name, e2.emp_name as manager_name from emp e1 left join emp e2 on e1.manager_id = e2.emp_id;

%python
# Self left join for hierarchical data
e = employees.alias("e")
m = employees.alias("m")

result = e.join(m, col("e.manager_id") == col("m.emp_id"), how="left") \
          .select(
              col("e.emp_id"),
              col("e.emp_name").alias("employee_name"),
              col("m.emp_name").alias("manager_name")).fillna("No Manager", subset=["manager_name"]).orderBy("emp_id")

result.show()