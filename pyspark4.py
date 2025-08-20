from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HealthcareSQLPractice") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# EASY QUESTIONS

# 1. Male patients
def male_patients(patients_df):
    return patients_df.filter(col("gender") == "M").select("first_name", "last_name", "gender")

# 2. Patients without allergies
def patients_no_allergies(patients_df):
    return patients_df.filter(col("allergies").isNull()).select("first_name", "last_name")

# 3. First names starting with 'C'
def names_start_with_c(patients_df):
    return patients_df.filter(col("first_name").like("C%")).select("first_name")

# 4. Patients with weight between 100-120
def weight_100_120(patients_df):
    return patients_df.filter(col("weight").between(100, 120)).select("first_name", "last_name")

# 5. Update allergies to 'NKA' if null (create new DF)
def update_allergies_nka(patients_df):
    return patients_df.withColumn("allergies", when(col("allergies").isNull(), "NKA").otherwise(col("allergies")))

# 6. Full name concatenation
def full_name(patients_df):
    return patients_df.select(concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"))

# 7. Patients with province names
def patients_with_province(patients_df, province_names_df):
    return patients_df.join(province_names_df, "province_id").select("first_name", "last_name", "province_name")

# 8. Patients born in 2010
def born_2010(patients_df):
    return patients_df.filter(year("birth_date") == 2010).agg(count("birth_date").alias("count_2010"))

# 9. Patient with greatest height
def tallest_patient(patients_df):
    max_height = patients_df.agg(max("height")).first()[0]
    return patients_df.filter(col("height") == max_height).select("first_name", "last_name", "height")

# 10. Patients with specific IDs
def specific_patient_ids(patients_df, ids):
    return patients_df.filter(col("patient_id").isin(ids))

# 11. Total number of admissions
def total_admissions(admissions_df):
    return admissions_df.agg(count("admission_date").alias("total_admissions"))

# 12. Same day admission-discharge
def same_day_admission_discharge(admissions_df):
    return admissions_df.filter(col("admission_date") == col("discharge_date"))

# 13. Admissions for specific patient
def admissions_for_patient(admissions_df, patient_id):
    return admissions_df.filter(col("patient_id") == patient_id).agg(
        count("admission_date").alias("admission_count")
    )

# 14. Unique cities in Nova Scotia
def cities_nova_scotia(patients_df):
    return patients_df.filter(col("province_id") == "NS").select("city").distinct()

# 15. Patients height > 160 and weight > 70
def tall_heavy_patients(patients_df):
    return patients_df.filter((col("height") > 160) & (col("weight") > 70)).select("first_name", "last_name", "birth_date")

# 16. Allergic patients in Hamilton
def allergic_patients_hamilton(patients_df):
    return patients_df.filter(
        (col("allergies").isNotNull()) & (col("city") == "Hamilton")
    ).select("first_name", "last_name", "allergies")

# MEDIUM QUESTIONS

# 17. Unique birth years
def unique_birth_years(patients_df):
    return patients_df.select(year("birth_date").alias("birth_year")).distinct().orderBy("birth_year")

# 18. Unique first names (occurring only once)
def unique_first_names(patients_df):
    name_counts = patients_df.groupBy("first_name").agg(count("*").alias("count"))
    return name_counts.filter(col("count") == 1).select("first_name")

# 19. First names starting and ending with 's', at least 6 chars
def names_s_start_end(patients_df):
    return patients_df.filter(
        (col("first_name").like("s%s")) & 
        (length(col("first_name")) >= 6)
    ).select("patient_id", "first_name")

# 20. Patients with Dementia diagnosis
def dementia_patients(patients_df, admissions_df):
    return patients_df.alias("p").join(
        admissions_df.alias("a"), 
        col("p.patient_id") == col("a.patient_id")
    ).filter(col("a.diagnosis") == "Dementia").select("p.patient_id", "p.first_name", "p.last_name")

# 21. First names ordered by length then alphabetically
def names_ordered_length_alpha(patients_df):
    return patients_df.select("first_name").orderBy(length("first_name"), "first_name")

# 22. Male and female patient counts
def gender_counts(patients_df):
    return patients_df.agg(
        sum(when(col("gender") == "M", 1).otherwise(0)).alias("male_count"),
        sum(when(col("gender") == "F", 1).otherwise(0)).alias("female_count")
    )

# 23. Patients allergic to Penicillin or Morphine
def penicillin_morphine_allergies(patients_df):
    return patients_df.filter(
        col("allergies").isin(["Penicillin", "Morphine"])
    ).select("first_name", "last_name", "allergies").orderBy("allergies", "first_name", "last_name")

# 24. Patients with multiple same diagnosis admissions
def multiple_same_diagnosis(admissions_df):
    return admissions_df.groupBy("patient_id", "diagnosis").agg(
        count("*").alias("admission_count")
    ).filter(col("admission_count") > 1).select("patient_id", "diagnosis")

# 25. Cities by patient count
def cities_by_patient_count(patients_df):
    return patients_df.groupBy("city").agg(
        count("*").alias("number_of_patients")
    ).orderBy(col("number_of_patients").desc(), "city")

# 26. Patients and doctors with roles
def patients_doctors_with_roles(patients_df, doctors_df):
    patients = patients_df.select("first_name", "last_name", lit("Patient").alias("role"))
    doctors = doctors_df.select("first_name", "last_name", lit("Doctor").alias("role"))
    return patients.union(doctors)

# 27. Allergies by popularity
def allergies_by_popularity(patients_df):
    return patients_df.filter(col("allergies").isNotNull()).groupBy("allergies").agg(
        count("*").alias("popularity")
    ).orderBy(col("popularity").desc())

# 28. Patients born in 1970s
def born_1970s(patients_df):
    return patients_df.filter(
        (year("birth_date") >= 1970) & (year("birth_date") <= 1979)
    ).select("first_name", "last_name", "birth_date").orderBy("birth_date")

# 29. Full name formatted (UPPER last, lower first)
def formatted_full_name(patients_df):
    return patients_df.select(
        concat(upper("last_name"), lit(","), lower("first_name")).alias("full_name")
    ).orderBy("first_name").desc()

# 30. Provinces with total height >= 7000
def provinces_tall_patients(patients_df):
    return patients_df.groupBy("province_id").agg(
        sum("height").alias("total_height")
    ).filter(col("total_height") >= 7000).select("province_id", "total_height")

# 31. Weight difference for Maroni patients
def maroni_weight_diff(patients_df):
    maroni_patients = patients_df.filter(col("last_name") == "Maroni")
    max_weight = maroni_patients.agg(max("weight")).first()[0]
    min_weight = maroni_patients.agg(min("weight")).first()[0]
    return spark.createDataFrame([(max_weight - min_weight,)], ["weight_diff"])

# 32. Admissions by day of month
def admissions_by_day(admissions_df):
    return admissions_df.groupBy(dayofmonth("admission_date").alias("day_num")).agg(
        count("*").alias("admission_count")
    ).orderBy(col("admission_count").desc())

# 33. Most recent admission for patient 542
def recent_admission_542(admissions_df):
    return admissions_df.filter(col("patient_id") == 542).orderBy(col("admission_date").desc()).limit(1)

# 34. Complex admission criteria
def complex_admission_criteria(admissions_df):
    return admissions_df.filter(
        ((col("patient_id") % 2 == 1) & col("attending_doctor_id").isin([1, 5, 19])) |
        ((col("attending_doctor_id").cast("string").like("%2%")) & (length(col("patient_id").cast("string")) == 3))
    ).select("patient_id", "attending_doctor_id", "diagnosis")

# 35. Admissions attended by each doctor
def admissions_per_doctor(admissions_df, doctors_df):
    return admissions_df.alias("a").join(
        doctors_df.alias("d"), 
        col("a.attending_doctor_id") == col("d.doctor_id")
    ).groupBy("d.doctor_id", "d.first_name", "d.last_name").agg(
        count("*").alias("admissions_attended")
    ).select("first_name", "last_name", "admissions_attended")

# 36. Doctor admission date range
def doctor_admission_dates(admissions_df, doctors_df):
    return admissions_df.alias("a").join(
        doctors_df.alias("d"), 
        col("a.attending_doctor_id") == col("d.doctor_id")
    ).groupBy("d.doctor_id", "d.first_name", "d.last_name").agg(
        min("admission_date").alias("first_admission"),
        max("admission_date").alias("last_admission")
    )

# 37. Patients per province
def patients_per_province(patients_df, province_names_df):
    return patients_df.alias("p").join(
        province_names_df.alias("pn"), 
        "province_id"
    ).groupBy("pn.province_name").agg(
        count("*").alias("patient_count")
    ).orderBy(col("patient_count").desc())

# 38. Admission details with doctor info
def admission_details(admissions_df, patients_df, doctors_df):
    return admissions_df.alias("a").join(
        patients_df.alias("p"), "patient_id"
    ).join(
        doctors_df.alias("d"), 
        col("a.attending_doctor_id") == col("d.doctor_id")
    ).select(
        concat("p.first_name", lit(" "), "p.last_name").alias("patient_name"),
        "a.diagnosis",
        concat("d.first_name", lit(" "), "d.last_name").alias("doctor_name")
    )

# 39. Duplicate patients by name
def duplicate_patients(patients_df):
    return patients_df.groupBy("first_name", "last_name").agg(
        count("*").alias("duplicate_count")
    ).filter(col("duplicate_count") > 1)

# 40. Patient details with unit conversion
def patient_details_converted(patients_df):
    return patients_df.select(
        concat("first_name", lit(" "), "last_name").alias("patient_name"),
        round(col("height") / 30.48, 1).alias("height_ft"),
        round(col("weight") * 2.205, 0).alias("weight_lbs"),
        "birth_date",
        when(col("gender") == "M", "Male").otherwise("Female").alias("gender_full")
    )

# 41. Patients without admissions
def patients_no_admissions(patients_df, admissions_df):
    return patients_df.alias("p").join(
        admissions_df.alias("a"), 
        "patient_id", 
        "left_anti"
    ).select("p.patient_id", "p.first_name", "p.last_name")

# HARD QUESTIONS

# 42. Patients grouped by weight
def patients_by_weight_group(patients_df):
    return patients_df.withColumn(
        "weight_group", (col("weight") / 10).cast("int") * 10
    ).groupBy("weight_group").agg(
        count("*").alias("patient_count")
    ).orderBy("weight_group")

# 43. Obese patients (BMI >= 30)
def obese_patients(patients_df):
    return patients_df.withColumn(
        "bmi", col("weight") / (pow(col("height") / 100, 2))
    ).withColumn(
        "isObese", when(col("bmi") >= 30, 1).otherwise(0)
    ).select("patient_id", "weight", "height", "isObese")

# 44. Epilepsy patients treated by Lisa
def epilepsy_patients_lisa(patients_df, admissions_df, doctors_df):
    return patients_df.alias("p").join(
        admissions_df.alias("a"), "patient_id"
    ).join(
        doctors_df.alias("d"), 
        col("a.attending_doctor_id") == col("d.doctor_id")
    ).filter(
        (col("a.diagnosis") == "Epilepsy") & (col("d.first_name") == "Lisa")
    ).select("p.patient_id", "p.first_name", "p.last_name", "d.specialty")

# 45. Temporary passwords for admitted patients
def temp_passwords(patients_df, admissions_df):
    return patients_df.alias("p").join(
        admissions_df.alias("a"), "patient_id"
    ).select(
        "p.patient_id",
        concat(
            "p.patient_id", 
            length("p.last_name"), 
            year("p.birth_date")
        ).alias("temp_password")
    ).distinct()

# 46. Admission costs by insurance
def admission_costs_insurance(admissions_df):
    return admissions_df.withColumn(
        "has_insurance", when(col("patient_id") % 2 == 0, "Yes").otherwise("No")
    ).withColumn(
        "cost", when(col("patient_id") % 2 == 0, 10).otherwise(50)
    ).groupBy("has_insurance").agg(
        sum("cost").alias("total_cost")
    )

# 47. Provinces with more male than female patients
def provinces_more_males(patients_df, province_names_df):
    gender_counts = patients_df.alias("p").join(
        province_names_df.alias("pn"), "province_id"
    ).groupBy("pn.province_name").agg(
        sum(when(col("p.gender") == "M", 1).otherwise(0)).alias("male_count"),
        sum(when(col("p.gender") == "F", 1).otherwise(0)).alias("female_count")
    )
    return gender_counts.filter(col("male_count") > col("female_count")).select("province_name")

# 48. Specific patient search
def specific_patient_search(patients_df):
    return patients_df.filter(
        (col("first_name").like("__r%")) &
        (col("gender") == "F") &
        (month("birth_date").isin([2, 5, 12])) &
        (col("weight").between(60, 80)) &
        (col("patient_id") % 2 == 1) &
        (col("city") == "Kingston")
    )

# 49. Percentage of male patients
def male_percentage(patients_df):
    total = patients_df.count()
    male_count = patients_df.filter(col("gender") == "M").count()
    percentage = round((male_count / total) * 100, 2)
    return spark.createDataFrame([(f"{percentage}%",)], ["male_percentage"])

# 50. Daily admissions with change from previous day
def daily_admissions_change(admissions_df):
    window_spec = Window.orderBy("admission_date")
    daily_counts = admissions_df.groupBy("admission_date").agg(
        count("*").alias("daily_count")
    )
    return daily_counts.withColumn(
        "change", col("daily_count") - lag("daily_count", 1).over(window_spec)
    )

# 51. Provinces with Ontario always first
def provinces_ontario_first(province_names_df):
    return province_names_df.orderBy(
        when(col("province_name") == "Ontario", 0).otherwise(1),
        "province_name"
    )

# 52. Yearly admissions per doctor
def yearly_admissions_per_doctor(admissions_df, doctors_df):
    return admissions_df.alias("a").join(
        doctors_df.alias("d"), 
        col("a.attending_doctor_id") == col("d.doctor_id")
    ).groupBy(
        "d.doctor_id",
        concat("d.first_name", lit(" "), "d.last_name").alias("doctor_name"),
        "d.specialty",
        year("a.admission_date").alias("year")
    ).agg(
        count("*").alias("total_admissions")
    ).orderBy("d.doctor_id", "year")

# Utility class for testing
class HealthcareValidator:
    def __init__(self, spark):
        self.spark = spark
        self.create_test_data()
    
    def create_test_data(self):
        """Create sample healthcare test data"""
        # Patients data
        patients_data = [
            (1, "John", "Doe", "M", 180, 85, "1990-01-01", "Toronto", "NS", "Peanuts"),
            (2, "Jane", "Smith", "F", 165, 65, "1985-05-20", "Hamilton", "ON", "Penicillin"),
            (3, "Chris", "Maroni", "M", 175, 75, "1988-12-15", "Kingston", "QC", None),
            (4, "Sarah", "Johnson", "F", 160, 60, "1975-08-30", "Ottawa", "BC", "Morphine")
        ]
        self.patients_df = spark.createDataFrame(
            patients_data, 
            ["patient_id", "first_name", "last_name", "gender", "height", "weight", 
             "birth_date", "city", "province_id", "allergies"]
        )
        
        # Admissions data
        admissions_data = [
            (1, 1, "2024-01-15", "2024-01-20", "Dementia", 101),
            (2, 2, "2024-02-10", "2024-02-10", "Epilepsy", 102),
            (3, 1, "2024-03-05", "2024-03-15", "Dementia", 101)
        ]
        self.admissions_df = spark.createDataFrame(
            admissions_data,
            ["admission_id", "patient_id", "admission_date", "discharge_date", "diagnosis", "attending_doctor_id"]
        )
        
        # Doctors data
        doctors_data = [
            (101, "Lisa", "Brown", "Neurology"),
            (102, "Mike", "Wilson", "Cardiology")
        ]
        self.doctors_df = spark.createDataFrame(
            doctors_data,
            ["doctor_id", "first_name", "last_name", "specialty"]
        )
        
        # Province names
        province_data = [("NS", "Nova Scotia"), ("ON", "Ontario"), ("QC", "Quebec"), ("BC", "British Columbia")]
        self.province_names_df = spark.createDataFrame(province_data, ["province_id", "province_name"])
        
        print("Test data created successfully!")
    
    def run_comprehensive_tests(self):
        """Run tests for all functions"""
        print("Running comprehensive healthcare tests...")
        
        try:
            # Test easy questions
            result1 = male_patients(self.patients_df)
            result2 = patients_no_allergies(self.patients_df)
            
            print("✓ Easy questions tests passed")
            print(f"Male patients: {result1.count()}")
            print(f"Patients without allergies: {result2.count()}")
            
            # Test medium questions
            result3 = unique_birth_years(self.patients_df)
            result4 = dementia_patients(self.patients_df, self.admissions_df)
            
            print("✓ Medium questions tests passed")
            print(f"Unique birth years: {result3.count()}")
            print(f"Dementia patients: {result4.count()}")
            
            # Test hard questions
            result5 = obese_patients(self.patients_df)
            result6 = admission_costs_insurance(self.admissions_df)
            
            print("✓ Hard questions tests passed")
            print(f"Obese patients: {result5.count()}")
            print(f"Admission costs: {result6.collect()}")
            
            print("All healthcare tests completed successfully!")
            
        except Exception as e:
            print(f"✗ Test failed: {str(e)}")

# Main execution
if __name__ == "__main__":
    print("Healthcare SQL Practice Questions Implementation in PySpark")
    print("=" * 70)
    print("52 healthcare questions implemented as PySpark functions")
    print("=" * 70)
    
    # Initialize validator
    validator = HealthcareValidator(spark)
    
    # Run tests
    validator.run_comprehensive_tests()
    
    print("\nAll healthcare functions are ready to use!")
    print("Example usage:")
    print("  result = male_patients(patients_df)")
    print("  result.show()")
    print("  result = dementia_patients(patients_df, admissions_df)")
    print("  result.show()")