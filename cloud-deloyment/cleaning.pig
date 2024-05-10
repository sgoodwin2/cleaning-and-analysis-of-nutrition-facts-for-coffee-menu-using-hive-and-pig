-- Remove existing 'outputs' directory if it exists
fs -rm -f -r -R outputs;

-- starbucks-menu-nutrition-drinks.csv

-- Register the Piggybank library for CSVLoader
REGISTER 'gs://nutrition-bucket2/data/piggybank.jar';

-- Load and clean data from 'starbucks-menu-nutrition-drinks.csv'
data = LOAD 'gs://nutrition-bucket2/data/archive/starbucks-menu-nutrition-drinks.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (product:chararray, calories:chararray, fat_g:chararray, carb_g:chararray, fiber_g:chararray, protein:chararray, sodium:chararray);

-- Filter out header rows
filter_out_headers = FILTER data BY (product != '') AND (calories != 'Calories') AND (fat_g != 'Fat (g)') AND (carb_g != 'Carb. (g)') AND (fiber_g != 'Fiber (g)') AND (protein != 'Protein') AND (sodium != 'Sodium');

-- Further filter rows with '-' values
filtered_data = FILTER filter_out_headers BY fat_g != '-' AND carb_g != '-' AND fiber_g != '-' AND protein != '-' AND sodium != '-';

-- Clean and format the data
cleaned_data = FOREACH filtered_data GENERATE product, (calories == '' ? 0.0 : (double)calories) AS calories, (fat_g == '' ? 0.0 : (double)fat_g) AS fat, (carb_g == '' ? 0.0 : (double)carb_g) AS carb, (fiber_g == '' ? 0.0 : (double)fiber_g) AS fiber, (protein == '' ? 0.0 : (double)protein) AS protein, (sodium == '' ? 0.0 : (double)sodium) AS sodium;

-- Store the cleaned data into a CSV file
STORE cleaned_data INTO 'gs://nutrition-bucket2/outputs/cleanDrinks.csv' USING PigStorage(',');

-- starbucks_drinkMenu_expanded.csv

-- Load and clean data from 'starbucks_drinkMenu_expanded.csv'
data = LOAD 'gs://nutrition-bucket2/data/archive/starbucks_drinkMenu_expanded.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (bev_cat:chararray, bev:chararray, bev_prep:chararray, calories:chararray, tot_fat:chararray, trans_fat:chararray, sat_fat:chararray, sodium:chararray, tot_carb:chararray, chol:chararray, diet_fat:chararray, sugar:chararray, protein:chararray, vit_a:chararray, vit_c:chararray, calc:chararray, iron:chararray, caff:chararray);

-- Filter out header rows
filter_out_headers = FILTER data BY (bev_cat != 'Beverage_category') AND (bev != 'Beverage') AND (bev_prep != 'Beverage_prep') AND (calories != 'Calories') AND (tot_fat != 'Total Fat (g)') AND (trans_fat != 'Trans Fat (g)') AND (sat_fat != 'Saturated Fat (g)') AND (sodium != 'Sodium (mg)') AND (tot_carb != 'Total Carbohydrates (g)') AND (chol != 'Cholesterol (mg)') AND (diet_fat != 'Dietary Fibre (g)') AND (sugar != 'Sugars (g)') AND (protein != 'Protein (g)') AND (vit_a != 'Vitamin A (% DV)') AND (vit_c != 'Vitamin C (% DV)') AND (calc != 'Calcium (% DV)') AND (iron != 'Iron (% DV)') AND (caff != 'Caffeine (mg)');

-- Clean and format the data
cleaned_data = FOREACH filter_out_headers GENERATE (bev) AS product, (calories) AS calories, (tot_fat == '' ? 0.0 : (double)tot_fat) AS fat, (tot_carb == '' ? 0.0 : (double)tot_carb) AS carb, (diet_fat == '' ? 0.0 : (double)diet_fat) AS fiber, (protein == '' ? 0.0 : (double)protein) AS protein, (sodium == '' ? 0.0 : (double)sodium) AS sodium;

-- Store the cleaned data into a CSV file
STORE cleaned_data INTO 'gs://nutrition-bucket2/outputs/cleanExpandedDrinks.csv' USING PigStorage(',');
