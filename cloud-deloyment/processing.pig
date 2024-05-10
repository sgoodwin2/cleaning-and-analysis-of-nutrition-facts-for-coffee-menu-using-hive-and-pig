
fs -rm -f -r -R outputs/pigQueries.csv;

-- Section 1: Load, Sort, and Display Top 10 Products by Protein Content

-- Load the cleaned data from the 'cleanDrinks.csv' file
nutrition = LOAD 'gs://nutrition-bucket2/outputs/cleanDrinks.csv/part-m-00000' USING org.apache.pig.piggybank.storage.CSVLoader() AS (product:chararray, calories:double, fat_g:double, carb_g:double, fiber_g:double, protein:double, sodium:double);

-- Sort the data by protein content in descending order
sorted_data = ORDER nutrition BY protein DESC;

-- Select the top 10 products with the highest protein content
top_10 = LIMIT sorted_data 10;

-- Display the top 10 products with the highest protein content
DUMP top_10;

STORE top_10 INTO 'gs://nutrition-bucket2/outputs/pigProtein.csv' USING PigStorage(',');

--- Section 2: Filter and Display Products with 'chocolate' in the Name

-- Filter products that have 'chocolate' (case-insensitive) in their name
filtered_data = FILTER nutrition BY LOWER(product) MATCHES '.*chocolate.*';

-- Extract and display product names and their respective calories
result = FOREACH filtered_data GENERATE product, calories;

-- Display the products containing 'chocolate' in their name along with their calories
DUMP result;

STORE result INTO 'gs://nutrition-bucket2/outputs/pigChocolate.csv' USING PigStorage(',');