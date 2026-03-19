Easy

1.Salaries Difference
import your libraries

from pyspark.sql.functions import col,max,abs

df = db_employee.join(db_dept, db_employee['department_id'] == db_dept['id'],'left')
df_mkt = df.filter(col('department') == 'marketing').select(max('salary').alias('mkt')
df_eng = df.filter(col('department') == 'engineering').select(max('salary').alias('eng'))
df2 = df_mkt.join(df_eng).withColumn('salary_diff',abs(col('mkt')-col('eng'))).select('salary_diff")

ans = abs(df_mkt['max(salary)'] - df_eng['max(salary)'])
ans_df = pd.DataFrame([ans],['salary'])

convert your final pyspark df to a pandas df
df2.toPandas()

2. Finding updated records

from pysark.sql.window import Window
from pyspark.sql.functions import rank,desc,col


win_spec = Window.partitionBy('first_name','last_name').orderBy('salary'))
ms_employee_salary = ms_employee_salary.withColumn('rank',rank().over(win_spec)).filter(col('rank')==1).orderBy('id').drop('rank').select('id','first_name','last_name'.'department_id','salary')

convert final pyspark df to a pandas df
ms_employee_salary.toPandas()

3.Bikes last used

from pyspark.sql.functions import col,desc,rank
from pyspark.sql.window import Window

win = Window.partitionBy('bike_number').orderBy(desc('end_time'))
dc_bikeshare_q1_2012 = dc_bikeshare_q1_2012.withColumn('rn',rank().over(win)).filter(col('rn') == 1).select ('bike_number').sort(desc('end_time'))
dc_bikeshare_q1_2012.toPandas()

4.Reviews of Hotel Aerna

from pyspark.sql.functions import col,count

hotel_reviews = hotel_reviews.filter(col('hotel_name') == 'Hotel Arena').groupBy('hotel_name','reviewer_score').agg(count('reviewer_score').alias('n_reviews')).
select('reviewer_score','hotel_name','n_reviews')

hotel_reviews.toPandas()

5. Count the number of movies that abigail breslin nominated for oscar 

from pyspark.sql.functions import col,count,lower,countDistinct

oscar_nominees = oscar_nominees.filter(col('nominee') == 'Abigail Breslin').groupBy('nominee').agg('countDistinct('movie').alias('movie_cnt')).
select('movie_cnt')

oscar_nominees = oscar_nominees.filter(lower(col('nominee')).like('%abigail%'))

oscar_nominees.toPandas()

6. Find all post which were reacted to with a heart

from pyspark.sql.function import col

facebook_reactions = facebook_reactions.filter(col('reaction') == 'heart')
facebook_posts = facebook_posts.join(facebook_reaction,facebook_posts['post_id']==facebook_reactions['post_id'],'inner').select(facebook_posts['*']).distinct()
facebook_posts = facebook_posts.select('post_id','poster','post_text','post_keywords','post_date')

convert your final pyspark df to a pandas df
facebook_post.toPandas()

7.popularity of hack

import pyspark

df = facebook_employees.join(facebook_hack_survey,facebook_employees['id']==facebook_hack_survey['employee_id'],'left')
df = df.groupBy('location').avg('popularity')

convert your final pyspark df to a pandas df
df.toPandas()

8. Lyft driver wages
from pyspark.sql.functions import *

lyft_driver = lyft_driver.filter((col('yearly_salary') <= 30000) | (col('yearly_salary') >= 70000))
lyft_driver.toPandas()

9. how many times each artist appeared on the spotify ranking list

from pyspark.sql.function import *

spotify_worldwide_daily_song_ranking = spotify_worldwide_daily_song_ranking.groupBy('artist').agg(count('*').alias('n_occurences')).sort(desc('n_occurences'))
spotify_worldwide_daily_song_ranking.toPandas()

10. find the base pay for police captains

from pyspark.sql.function import col

sf_public_salaries = sf_public_salaries.filter(col('jobtitle') == "Captain III (police department)").
select('employeename','basepay')

convert your final pyspark df to pandas df
sf_public_salaries.toPandas()

11. FInd libraries who haven't provided the email address in circulation year 2016 but their notice preference definition is set to email

from pyspark.sql.functions import *

library_usage = library_usage.filter((col('notice_preference_definition') == 'email') &
(col('provided_email_address') == False) & (col('circulation_active_year') == '2016').
select ('home_liibrary_code').dropDuplicates()

library_usage.toPandas()

12. Average Salaries

from pyspark.sql.function import *
from pyspark.sql.window import window

win_spec = window.partitionBy('department')
employee = employee.withColumn('avg_sal',avg('salary').over(win_spec)).select('department','first_name','salary','avg-sal')
employee.toPandas()

13. Order Details

from pyspark.sql.functions import *

customers = customers.filter(col('first_name').isin(['jill','Eva']))
customers = customers.join(order,customer['id'] == orders['cust_id'],'left')
customers = customers.orderBy('cust_id').select('order_date','order_details','total_order_cost','first_name')
cutomers.toPandas()

14. customer details

import pyspark

customers = customers.join(orders,customers['id'] == orders['cust_id'],'left')
customers = customers.select('first_name','last_name','city','order_details').orderBy(['first_name','order_details'])
customer.toPandas()

15. Number of workers by department starting in april or later
from pyspark.sql.functions import *

worker = worker.filter(month('joining_date')>=4).groupBy('department').agg(countDistinct('worker_id'))
worker.toPandas()

16. admin department employees beginning in april or later

from pyspark.sql.functions import *

worker = worker.filter(month('joining_date')>=4).filter(col('department') == 'Admin').agg(count('*'))
worker.toPandas()

17. Churro Activity date

from pyspark.sql.function import col

los_angeles_restaurant_health_inspections = los_angeles_restaurant_health_inspections.filter((col('facility_name') == 'Street churros') & (col('score') < 95))
.select('activity_date','pe_description')

los_angeles_restaurant_health_inpsections.toPandas()


18. Find the most profitable company in the financial sector of the entire world along with its continent
 # Import your libraries
from pyspark.sql.functions import *
# Start writing code
forbes_global_2010_2014 = forbes_global_2010_2014.filter(col('sector') =='Financials').orderBy(desc('profits')).select('company','continent').limit(1)

forbes_global_2010_2014.toPandas()

19. Count the number of user events performed by MacBookPro users

from pyspark.sql.functions import col,desc

playbook_events = playbook_events.filter(col('device')=='macbook pro')
playbook_events = playbook_events.groupBy(col('event_name')).count().sort(desc('count'))

playbook_events.toPandas()

20. Number Of Bathrooms And Bedrooms

from pyspark.sql.functions import *

airbnb_search_details =
airbnb_search_details.groupBy('city','property_type').agg(avg('bedrooms').alias('n_bedrooms_avg'), avg('bathrooms').alias('n_bathrooms_avg'))

airbnb_search_details.toPandas()

21. Most Lucrative Products

from pyspark.sql.functions import *
from pyspark.sql.window import Window

online_orders = online_orders.withColumn('Revenue', col('cost_in_dollars')*col('units_sold'))
online_orders = online_orders.filter(year('date') == '2022').filter(month('date').between(1,6))
online_orders = online_orders.groupBy('product_id').agg(sum('Revenue').alias('Total'))
win_spec = Window.orderBy(desc('Total'))
online_orders = online_orders.withColumn('rnk',dense_rank().over(win_spec)).filter(col('rnk')<6).drop('rnk')

online_orders.toPandas()


22. Number of Shipments Per Month

from pyspark.sql.functions import *

amazon_shipment = amazon_shipment.withColumn('Shipment', concat('shipment_id',
'sub_id')).withColumn('year_month', date_format('shipment_date', 'yyyyMM')).groupBy('year_month').agg(count('Shipment').alias('n_ship')).select('year_month','n_ship')

amazon_shipment.toPandas()

23. Unique Users Per Client Per Month

from pyspark.sql.functions import *

fact_events = fact_events.groupBy('client_id',month('time_id')).agg(countDistinct('user_id'))

fact_events.toPandas()






















































































































































































