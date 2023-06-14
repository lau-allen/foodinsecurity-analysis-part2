
#importing libraries 
import pyspark 
from pyspark.sql import SparkSession
import sys
import pandas as pd
from pyproj import Transformer

#function to open csv file to read in rdd 
def open_csv(file):
  import csv
  #open file 
  with open(file) as f:
    #read using csv reader 
    data = csv.reader(f)
    for i,row in enumerate(data):
      #skip header 
      if i == 0:
        continue
      #return row as tuple 
      yield tuple(row)

#function for getting the lat/long from cbg 
def get_pos(cbg,dic):
  return dic.value[cbg]

#function to extract needed data from weekly_patterns 
def extract_weekly_patterns(index, rows):
  #skip header
  if index == 0:
    next(rows)
  #read csv data yielding specific rows of interest 
  import csv 
  reader = csv.reader(rows)
  for row in reader:
    yield (row[0],(row[18],row[19],row[12],row[13]))

#function for filtering on specific year-month 
def timeframe_filter(start,end):
  #splitting to year-month format 
  start = start.split('-')[:2]
  end = end.split('-')[:2]
  #if the year and month are within the specified months, return true 
  if (start[0] in ['2020','2019']) and (start[1] in ['03','10']):
    return True
  elif (end[0] in ['2020','2019']) and (end[1] in ['03','10']):
    return True
  else:
    return False

#function for returning just the year-month
def timeframe_reducer(rows):
  for row in rows: 
    #splitting to year-month format
    start = row[1][2].split('-')[:2]
    end = row[1][3].split('-')[:2]
    #return only the year-month if it is within the specified months 
    if (start[0] in ['2020','2019']) and (start[1] in ['03','10']):
      yield (row[0]),((row[1][0],),row[1][1],(start[0]+'-'+start[1]))
    elif (end[0] in ['2020','2019']) and (end[1] in ['03','10']):
      yield (row[0]),((row[1][0],),row[1][1],(end[0]+'-'+end[1]))
  
#function for filtering on just the cbg in nyc_cbg_centroids
def GBG_FIPS_NYC_Filter(rows,nyc_cbg_centroids_filter_list):
  #importing libraries
  import json
  import copy
  
  for row in rows:
    cbgs = json.loads(row[1][1])
    #remove cbg if not in nyc_cbg_centroids 
    for key, value in copy.deepcopy(cbgs).items():
      if key not in nyc_cbg_centroids_filter_list:
        cbgs.pop(key)
    #return reduced cbg list
    yield (row[0]),(row[1][0],tuple(cbgs.items()),row[1][2],row[1][3])


#function for calculating the distance in miles 
def euclidean_d(from_loc,to_loc):
  #split data
  from_x,from_y = from_loc
  to_x,to_y = to_loc
  #conversion constant from ftUS to miUS
  conv_const = 0.0001893939
  #convert from ftUS to miUS
  from_x = from_x * conv_const
  from_y = from_y * conv_const
  to_x = to_x * conv_const
  to_y = to_y * conv_const 
  #calculate the distance and return 
  return ((to_x-from_x)**2 + (to_y-from_y)**2)**(1/2)

#function to calculate distance from one cbg to another cbg
#return data in (from cbg, total distance) form
def get_distance(rows,dic):
  for row in rows:
    cbg_tuple,date = row
    #unpack
    cbg_to, cbg_from, num_visitors = cbg_tuple
    #get x,y from cbg
    x_y_to = get_pos(cbg_to,dic)
    x_y_from = get_pos(cbg_from,dic)
    #calculate total distance based on num vistors  
    tot_dist = euclidean_d(x_y_from,x_y_to)*num_visitors
    yield ((cbg_from,date),(tot_dist,num_visitors))

#function for formatting
def flat_formatter(rows):
  for row in rows:
    _,data = row
    cbgs,date = data
    for tup in cbgs:
      yield (tup,date)

#function for formatting
def final_formatter(rows):
  for row in rows:
    yield (row[0][0],row[0][1],row[1])

def main():

  #spark session
  sc = pyspark.SparkContext.getOrCreate()
  spark = SparkSession(sc)

  #read nyc_supermarkets.csv and get the safegraph_placekey for filtering
  nyc_supermarkets = sc.parallelize(open_csv('nyc_supermarkets.csv')).map(lambda x: x[9])
  #creating broadcast variable used for filtering
  nyc_supermarkets = sc.broadcast(nyc_supermarkets.collect())

  #read csv as pandas df 
  nyc_centroids_df = pd.read_csv('nyc_cbg_centroids.csv')
  #define transformer
  transf = Transformer.from_crs(4326,2263,always_xy=True)
  #transform lat and long to x, y 
  x,y = transf.transform(nyc_centroids_df['longitude'].values,nyc_centroids_df['latitude'].values)
  #add data to df 
  nyc_centroids_df['x'] = x
  nyc_centroids_df['y'] = y
  #convert pandas df to pyspark rdd
  nyc_cbg_centroids = spark.createDataFrame(nyc_centroids_df).rdd\
    .map(lambda x: (str(x[0]),(x[3],x[4])))
  
  #create nyc_cbg_centroids_dict broadcast variable used for getting lat/long from cbg 
  nyc_cbg_centroids_dict = sc.broadcast(nyc_cbg_centroids.collectAsMap())
  #create nyc_cbg_centroids broadcast variable used for filtering
  nyc_cbg_centroids_filter = sc.broadcast(nyc_cbg_centroids.map(lambda x: x[0]).collect())

  #read weekly-patterns sample, getting relevant columns
  weekly_patterns = sc.textFile('gs://bdma/data/weekly-patterns-nyc-2019-2020/part-*',use_unicode=True).mapPartitionsWithIndex(extract_weekly_patterns)

  #rdd and associated manipulations to get desired output
  output = weekly_patterns.filter(lambda x: x[0] in nyc_supermarkets.value)\
  .filter(lambda x: timeframe_filter(x[1][2],x[1][3]))\
  .mapPartitions(lambda x: GBG_FIPS_NYC_Filter(x,nyc_cbg_centroids_filter.value))\
  .mapPartitions(lambda x: timeframe_reducer(x))\
  .mapPartitions(lambda x: (((row[0]),(list(map(lambda y: row[1][0]+y,row[1][1])),row[1][2])) for row in x))\
  .filter(lambda x: len(x[1][0])>0).mapPartitions(flat_formatter)\
  .mapPartitions(lambda x: get_distance(x,nyc_cbg_centroids_dict))\
  .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))\
  .mapValues(lambda x: round(x[0]/x[1],2)).mapPartitions(final_formatter)

  #convert rdd to dataframe, pivot date, sort by cbg_fibs to get desired output into correct format
  output_df = output.toDF(['cbg_fips','date','avg_dist']).groupby('cbg_fips').pivot('date').sum('avg_dist').sort('cbg_fips')

  #writing df to csv in defined output path 
  output_df.write.option("header",True).csv(sys.argv[1])

   

if __name__ == '__main__':
    main()
