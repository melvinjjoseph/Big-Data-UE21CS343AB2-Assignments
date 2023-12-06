#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder.appName("Task1").getOrCreate()

# Define command line arguments for input files
import sys
cases_2012_file = sys.argv[1]
cases_2013_file = sys.argv[2]
cases_2014_file = sys.argv[3]
cases_state_key_file = sys.argv[4]
judges_merged_file = sys.argv[6]
acts_sections_file = sys.argv[7]

output=sys.argv[8]
# Load the data from CSV files
cases_2012 = spark.read.csv(cases_2012_file, header=True, inferSchema=True)
cases_2013 = spark.read.csv(cases_2013_file, header=True, inferSchema=True)
cases_2014 = spark.read.csv(cases_2014_file, header=True, inferSchema=True)
judges_merged=spark.read.csv(judges_merged_file, header=True, inferSchema=True)
cases_state_key = spark.read.csv(cases_state_key_file, header=True, inferSchema=True)
acts_section=spark.read.csv(acts_sections_file, header=True, inferSchema=True)

# Combine all three years' data
all_cases = cases_2012.union(cases_2013).union(cases_2014)

# Join with state codes to get state names
all_cases = all_cases.join(cases_state_key, (all_cases.state_code == cases_state_key.state_code), 'inner')

# Group by state and count cases to get crime rates
crime_rates = all_cases.groupBy('state_name').count().withColumnRenamed('count', 'crime_rate')

# Sort in descending order based on crime rates
sorted_crime_rates = crime_rates.orderBy(col('crime_rate').desc())

# Get the top 10 states
top_10_states = sorted_crime_rates.limit(10)

# Extract and print the list of top 10 states
top_10_states_list = top_10_states.select('state_name').rdd.flatMap(lambda x: x).collect()

#Task 1.2

#filter judges_merged where ddl_decision_judge_id is not null
judges_merged=judges_merged.filter(col("ddl_decision_judge_id").isNotNull())

#change the column name in judges_merged from ddl_case_id to case_id to avoid ambiguity while joining
judges_merged=judges_merged.withColumnRenamed("ddl_case_id","case_id")

#join all_cases with judges_merged on ddl_case_id

all_cases_judges=all_cases.join(judges_merged, (all_cases.ddl_case_id == judges_merged.case_id), 'inner')

#remove duplicate column ddl_case_id
all_cases_judges=all_cases_judges.drop("ddl_case_id")

#join acts section with all_cases_judges on ddl_case_id
all_cases_judges_acts=all_cases_judges.join(acts_section, (all_cases_judges.case_id == acts_section.ddl_case_id), 'inner')

#filter all_cases_judges_acts where criminal=1
criminal_cases=all_cases_judges_acts.filter(col("criminal") == 1)

#group by ddl_decision_judge_id and count the cases
judge_case_counts = criminal_cases.groupBy('ddl_decision_judge_id').count().withColumnRenamed('count', 'case_count')

#find the judge with the maximum count
most_cases_judge = judge_case_counts.orderBy(col('case_count').desc()).limit(1)

#extract and print the judge_id with the maximum count
most_cases_judge_id = most_cases_judge.select('ddl_decision_judge_id').rdd.flatMap(lambda x: x).collect()[0]

print(most_cases_judge_id)

# Write the output to the file specified in command line as a tuple that contains the list of top 10 states and the judge_id with the maximum count
with open(output,'w') as filedata:
	filedata.write("%s" %str((top_10_states_list,most_cases_judge_id)))
filedata.close()

# Stop SparkSession
spark.stop()