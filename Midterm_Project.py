# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# Helper function to extract clean ZIP
def clean_zip(colname):
    return regexp_extract(col(colname), r'(\d{5})', 1)

# ---------------- CHICAGO ----------------
df_chi = (
    spark.read.option("header", True)
    .csv("/Volumes/workspace/damg7370/datastore/Cleaned_Chicago.csv")
    .select([col(c).cast(StringType()).alias(c.strip()) 
             for c in spark.read.csv("/Volumes/workspace/damg7370/datastore/Cleaned_Chicago.csv", header=True).columns])
    .withColumnRenamed("InspectionID", "inspection_id")
    .withColumnRenamed("DBA_Name", "restaurant_name")
    .withColumnRenamed("Facility_Type", "facility_type")
    .withColumnRenamed("Inspection_Date", "inspection_date")
    .withColumnRenamed("Inspection_Type", "inspection_type")
    .withColumnRenamed("Results", "inspection_result")
    .withColumnRenamed("Violation_Description", "violation_description")
    .withColumnRenamed("Risk", "risk")
    .withColumnRenamed("City", "city")
    .withColumnRenamed("State", "state")
    .withColumnRenamed("Zip", "zip_code")
    .withColumnRenamed("Latitude", "latitude")
    .withColumnRenamed("Longitude", "longitude")
    .withColumn("inspection_score", col("DerivedScore"))  # Added to normalize with Dallas
    .withColumn("source_city", lit("Chicago"))
)

# ---------------- DALLAS ----------------
df_dal = (
    spark.read.option("header", True)
    .csv("/Volumes/workspace/damg7370/datastore/Cleaned_Dallas.csv")
    .select([col(c).cast(StringType()).alias(c.strip()) 
             for c in spark.read.csv("/Volumes/workspace/damg7370/datastore/Cleaned_Dallas.csv", header=True).columns])
    .withColumnRenamed("RecordID", "inspection_id")
    .withColumnRenamed("restaurant_name", "restaurant_name")
    .withColumnRenamed("Facality_Type", "facility_type")
    .withColumnRenamed("Inspection Date", "inspection_date")
    .withColumnRenamed("Inspection Type", "inspection_type")
    .withColumnRenamed("Results", "inspection_result")
    .withColumnRenamed("Violation_desc", "violation_description")
    .withColumnRenamed("Risk", "risk")
    .withColumnRenamed("City", "city")
    .withColumnRenamed("State", "state")
    .withColumnRenamed("Zip Code", "zip_code")
    .withColumnRenamed("Street Address", "street_address")
    .withColumnRenamed("Description ", "description")
    .withColumnRenamed("Latitude", "latitude")
    .withColumnRenamed("Longitude", "longitude")
    .withColumnRenamed("inspection_score", "inspection_score")  # Ensure consistent schema
    .withColumn("source_city", lit("Dallas"))
)

# ---------------- COMBINE BOTH ----------------
silver_df = df_chi.unionByName(df_dal, allowMissingColumns=True)

# ---------------- CLEAN + STANDARDIZE ----------------
silver_df = (
    silver_df
    # Filter important fields
    .filter("restaurant_name IS NOT NULL AND inspection_date IS NOT NULL AND inspection_type IS NOT NULL AND zip_code IS NOT NULL")
    # Clean ZIP
    .withColumn("zip_code", clean_zip("zip_code"))
    # Risk stays as string
    .withColumn("risk", col("risk").cast("string"))
    # Safe numeric cast for lat/long
    .withColumn("latitude", expr("try_cast(latitude AS double)"))
    .withColumn("longitude", expr("try_cast(longitude AS double)"))
    # Date parsing (handles multiple formats)
    .withColumn(
        "inspection_date",
        coalesce(
            expr("try_to_date(inspection_date, 'MM/dd/yyyy')"),
            expr("try_to_date(inspection_date, 'yyyy-MM-dd')"),
            expr("try_to_date(inspection_date, 'yyyy/MM/dd')"),
            expr("try_to_date(inspection_date, 'dd-MMM-yy')")
        )
    )
    # Derived score for consistent metric
    .withColumn(
        "derived_score",
        when(col("inspection_result") == "Pass", 90)
        .when(col("inspection_result") == "Pass w/ Conditions", 80)
        .when(col("inspection_result") == "Fail", 70)
        .when(col("inspection_result") == "No Entry", 0)
        .otherwise(None)
    )
    # Remove any accidental null or duplicate inspections
    .dropDuplicates(["inspection_id"])
    .filter(col("inspection_id").isNotNull())
)

# ---------------- WRITE TO DELTA ----------------
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("damg7370.food_silver")

print("✅ Silver layer created successfully — clean, deduplicated, and schema-stable.")


# COMMAND ----------

from pyspark.sql.functions import *
silver_df = spark.read.table("damg7370.food_silver")


# COMMAND ----------

dim_city = (
    silver_df
    .select(col("city").alias("City_Name"), col("state").alias("State"))
    .dropDuplicates()
    .withColumn("City_Key", monotonically_increasing_id())
)
dim_city.write.format("delta").mode("overwrite").saveAsTable("damg7370.Dim_City")


# COMMAND ----------

dim_date = (
    silver_df
    .select(to_date(col("inspection_date")).alias("Full_Date"))
    .dropna()
    .dropDuplicates()
    .withColumn("Date_Key", monotonically_increasing_id())
    .withColumn("Day", dayofmonth("Full_Date"))
    .withColumn("Month", month("Full_Date"))
    .withColumn("Year", year("Full_Date"))
    .withColumn("Quarter", quarter("Full_Date"))
)
dim_date.write.format("delta").mode("overwrite").saveAsTable("damg7370.Dim_Date")


# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

# ------------------- DIM_TYPE -------------------

# Drop the old table to prevent schema conflicts
spark.sql("DROP TABLE IF EXISTS damg7370.Dim_Type")

dim_type = (
    silver_df
    .select(
        col("inspection_type").alias("Inspection_Type"),
        col("facility_type").alias("Facility_Type")
    )
    .filter(col("Inspection_Type").isNotNull() | col("Facility_Type").isNotNull())
    .dropDuplicates(["Inspection_Type", "Facility_Type"])
    .withColumn("Inspection_Type_Key", monotonically_increasing_id())
)

# Save to catalog
dim_type.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("damg7370.Dim_Type")

print("✅ Dim_Type created successfully with Inspection_Type, Facility_Type, and Inspection_Type_Key.")


# COMMAND ----------

dim_result = (
    silver_df
    .select(col("inspection_result").alias("Result_Name"))
    .dropDuplicates()
    .withColumn("Inspection_Result_Key", monotonically_increasing_id())
    .withColumn(
        "Derived_Score",
        when(col("Result_Name") == "Pass", 90)
        .when(col("Result_Name") == "Pass w/ Conditions", 80)
        .when(col("Result_Name") == "Fail", 70)
        .when(col("Result_Name") == "No Entry", 0)
        .otherwise(None)
    )
)
dim_result.write.format("delta").mode("overwrite").saveAsTable("damg7370.Dim_Result")


# COMMAND ----------

from pyspark.sql.functions import col, trim, when, monotonically_increasing_id

silver_df = spark.table("damg7370.food_silver")

dim_violation = (
    silver_df
    .select(trim(col("violation_description")).alias("Violation_Description"))
    .filter(col("Violation_Description").isNotNull() & (col("Violation_Description") != ""))
    .dropDuplicates(["Violation_Description"])
    .withColumn(
        "Violation_Points",
        when(col("Violation_Description").rlike("(?i)critical|temperature|storage"), 10)
        .when(col("Violation_Description").rlike("(?i)handwash|clean|sanitize|contamination"), 7)
        .when(col("Violation_Description").rlike("(?i)label|record|minor"), 4)
        .otherwise(1)
    )
    .withColumn(
        "Severity",
        when(col("Violation_Points") >= 8, "High")
        .when(col("Violation_Points") >= 5, "Medium")
        .otherwise("Low")
    )
    .withColumn("Violation_Key", monotonically_increasing_id())
    .select("Violation_Key", "Violation_Description", "Violation_Points", "Severity")
)

spark.sql("DROP TABLE IF EXISTS damg7370.Dim_Violation")
dim_violation.write.format("delta").mode("overwrite").saveAsTable("damg7370.Dim_Violation")


# COMMAND ----------

# create a single unified Address and License column
silver_df = silver_df.withColumn(
    "unified_address",
    coalesce(col("address"), col("street_address"))
)

silver_df = silver_df.withColumn(
    "unified_license",
    coalesce(col("license"), lit("N/A"))
)


# COMMAND ----------

dim_restaurant = (
    silver_df
    .select(
        col("restaurant_name").alias("Restaurant_Name"),
        col("zip_code").alias("Zip_Code"),
        col("unified_address").alias("Address"),
        col("unified_license").alias("License_Number")
    )
    .dropDuplicates()
    .withColumn("Restaurant_Key", monotonically_increasing_id())
    .withColumn("start_dt", current_timestamp())
    .withColumn("end_dt", lit(None).cast("timestamp"))
    .withColumn("is_active", lit("Y"))
)

dim_restaurant.write.format("delta").mode("overwrite").saveAsTable("damg7370.Dim_Restaurant")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Read old and new restaurant data
new_data = dim_restaurant.alias("new")
try:
    old_data = spark.table("damg7370.Dim_Restaurant").alias("old")
except:
    old_data = spark.createDataFrame([], dim_restaurant.schema)

# Detect changes in key attributes
changed = new_data.join(old_data, "License_Number", "left") \
    .filter(
        old_data["Restaurant_Name"].isNull() |
        (old_data["Restaurant_Name"] != new_data["Restaurant_Name"]) |
        (old_data["Address"] != new_data["Address"]) |
        (old_data["Zip_Code"] != new_data["Zip_Code"])
    ).select("new.*")

# Expire old records and insert new versions
if changed.count() > 0:
    old_data = old_data.withColumn("end_dt", current_timestamp()).withColumn("is_active", lit("N"))
    combined = old_data.unionByName(changed)
    combined.write.format("delta").mode("overwrite").saveAsTable("damg7370.Dim_Restaurant")
else:
    print("No changes detected for SCD2 update.")


# COMMAND ----------

silver_df = silver_df.withColumnRenamed("derived_score", "inspection_score")


# COMMAND ----------

from pyspark.sql.functions import col, countDistinct, sum as _sum

# ------------------- LOAD ALL TABLES -------------------
silver_df = spark.read.table("damg7370.food_silver")
dim_city = spark.read.table("damg7370.Dim_City")
dim_date = spark.read.table("damg7370.Dim_Date")
dim_restaurant = spark.read.table("damg7370.Dim_Restaurant")
dim_result = spark.read.table("damg7370.Dim_Result")
dim_type = spark.read.table("damg7370.Dim_Type")
dim_violation = spark.read.table("damg7370.Dim_Violation")

# ------------------- BUILD FACT TABLE -------------------
fact_df = (
    silver_df.alias("s")
    .join(dim_city.alias("c"), col("s.city") == col("c.City_Name"), "left")
    .join(dim_date.alias("d"), col("s.inspection_date") == col("d.Full_Date"), "left")
    .join(dim_restaurant.alias("r"), col("s.restaurant_name") == col("r.Restaurant_Name"), "left")
    .join(dim_result.alias("res"), col("s.inspection_result") == col("res.Result_Name"), "left")
    .join(dim_type.alias("t"), col("s.inspection_type") == col("t.Inspection_Type"), "left")
    .join(dim_violation.alias("v"), col("s.violation_description") == col("v.Violation_Description"), "left")
    .groupBy(
        "s.inspection_id",
        "d.Date_Key",
        "v.Violation_Key",
        "c.City_Key",
        "t.Inspection_Type_Key",
        "res.Inspection_Result_Key",
        "r.Restaurant_Key",
        "s.source_city"
    )
    .agg(
        _sum("v.Violation_Points").alias("Total_Points"),
        countDistinct("v.Violation_Key").alias("Violation_Count"),
        _sum("v.Violation_Points").alias("Violation_Score")
    )
    .withColumnRenamed("s.inspection_id", "Inspection_Key")
    .withColumnRenamed("s.source_city", "Record_Source")
)

# ------------------- SAVE DIRECTLY AS MANAGED TABLE -------------------
spark.sql("DROP TABLE IF EXISTS damg7370.Fact_Food_Inspection")

fact_df.write.format("delta").mode("overwrite").saveAsTable("damg7370.Fact_Food_Inspection")

print("✅ Fact_Food_Inspection successfully created and visible in Catalog under damg7370.")


# COMMAND ----------

fact_path = "/Volumes/workspace/damg7370/datastore/fact_food_inspection_delta"

fact_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(fact_path)

print(f"✅ Fact data saved to Delta path: {fact_path}")


# COMMAND ----------

fact_path = "/Volumes/workspace/damg7370/datastore/fact_food_inspection_csv"

fact_sample.write.mode("overwrite").option("header", True).csv(fact_path)

print(f"✅ CSV export completed successfully at {fact_path}")


# COMMAND ----------

fact_csv = spark.read.option("header", True).csv(fact_path)
display(fact_csv.groupBy("Source").count())


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM damg7370.Dim_City;
# MAGIC SELECT COUNT(*) FROM damg7370.Dim_Date;
# MAGIC SELECT COUNT(*) FROM damg7370.Dim_Result;
# MAGIC SELECT COUNT(*) FROM damg7370.Dim_Type;
# MAGIC SELECT COUNT(*) FROM damg7370.Dim_Restaurant;
# MAGIC SELECT COUNT(*) FROM damg7370.Dim_Violation;
# MAGIC SELECT COUNT(*) FROM damg7370.Fact_Food_Inspection;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.damg7370.dim_city

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.damg7370.dim_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.damg7370.dim_restaurant
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.damg7370.dim_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.damg7370.dim_type

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.damg7370.dim_violation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.damg7370.fact_food_inspection;
