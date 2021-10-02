#!/usr/bin/env python
# coding: utf-8

# Before you turn this problem in, make sure everything runs as expected. First, **restart the kernel** (in the menubar, select Kernel$\rightarrow$Restart) and then **run all cells** (in the menubar, select Cell$\rightarrow$Run All).
# 
# Make sure you fill in any place that says `YOUR CODE HERE` or "YOUR ANSWER HERE". You can run all the tests with the validate button. If the validate command takes too long, you can also confirm that you pass all the tests if you can run through the whole notebook without getting validation errors.

# For this problem set, we'll be using the Jupyter notebook:
# 
# ![](jupyter.png)

# ## RDD exercises
# 
# In this notebook you will implement multiple small methods that process and analyze country, city and location data.
# 
# We will use a sample data of "allCountries.txt" data from http://download.geonames.org/export/dump/allCountries.zip.  
# 
# You can test your functions in the cell below them. The variable `testFile` contains the data.
# 
# Read https://spark.apache.org/docs/latest/rdd-programming-guide.html for a guide about RDDs.
# 
# ### Data schema
# 
# Name | Description
# ------ | :-----
# geonameid         | integer id of record in geonames database  
# name              | name of geographical point (utf8) varchar(200)  
# asciiname         | name of geographical point in plain ascii characters, varchar(200)  
# alternatenames    | alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)  
# latitude          | latitude in decimal degrees (wgs84)  
# longitude         | longitude in decimal degrees (wgs84)  
# feature class     | see http://www.geonames.org/export/codes.html, char(1)  
# feature code      | see http://www.geonames.org/export/codes.html, varchar(10)  
# country code      | ISO-3166 2-letter country code, 2 characters  
# cc2               | alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters  
# admin1 code       | fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)  
# admin2 code       | code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)   
# admin3 code       | code for third level administrative division, varchar(20)  
# admin4 code       | code for fourth level administrative division, varchar(20)  
# population        | bigint (8 byte int)   
# elevation         | in meters, integer  
# dem               | digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or   30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.  
# timezone          | the iana timezone id (see file timeZone.txt) varchar(40)  
# modification date | date of last modification in yyyy-MM-dd format  
# 
# 

# In[3]:


from pyspark import SparkContext, SparkConf
sc = SparkContext("local","GeoProcessor")
testFile = sc.textFile("allCountries_sample.txt")


# ## Extract Data
# `extractData` removes unnecessary fields and splits the data so that the RDD looks like  RDD(Array("name","countryCode","dem"),...)).
# 
# Fields to include:  
# * name  
# * counryCode  
# * dem (digital elevation model)  
# 
# 
# param `data`: data set loaded into spark as RDD[String]  
# 
# `return`: RDD containing filtered location data. There should be an Array for each location.
# 
# 
# Hint: you can first split each line into an array. Columns are separated by tab ("\t") character. Finally you should take the appropriate fields. The fields will be numbered by the location they are ordered in the original data scheme. Despite the method's name, you might only need the `map` function.
# 
# 
# 

# In[17]:


def extractData(data):
    return data          .map(lambda line: line.split("\t"))          .map(lambda record: [record[1], record[8], record[16]])

    raise NotImplementedError()


# In[18]:


#Example print

extractData(testFile).take(5)


# In[19]:


'''extractData tests'''
filtered = extractData(testFile)
testObject = filtered.collect()[1]
assert testObject[0] == "Riu de la Llosada", "the name value of the object was expected to be 'Riu de la Llosada' but it was %s" % testObject[0]
assert testObject[1] == "AD", "the country code value of the object was expected to be 'AD' but it was %s" % testObject[1]
assert testObject[2] == "1900", "the dem value of the object was expected to be 1900 but it was %s" % testObject[2]
assert len(testObject) == 3, "the length of the array was expected to be 3 but it was %s" % len(testObject)
assert type(testObject) is list, "the type of the RDD element was expected to be list but it was %s" % type(testObject)


# ## Filter Elevation
# 
# `filterElevation` is used to filter an RDD to given country code and return an RDD containing only dem information. You will have to convert the dem information to `int` values.
# 
# param `countryCode`: country code e.g "AD"  
# param `data`: an RDD containing multiple Array["name", "countryCode", "dem"] (as in it was returned by the `extractData` function)   
# 
# `return`: RDD[int] containing only dem information related to the country code  
# 

# In[20]:


def filterElevation(countryCode, data):
    return data             .filter(lambda record, param=countryCode: record[1] == param)             .map(lambda record: int(record[2]))
    raise NotImplementedError()


# In[21]:


#Example print

filterElevation("AD", extractData(testFile)).take(5)


# In[22]:


'''filterElevation tests'''
filtered = extractData(testFile)
first = filterElevation("SE", filtered).first()
assert type(first) is int, "the type of the RDD element was expected to be int but it was %s" % type(first)
assert first == 56, "the value of the RDD element was expected to be 56 but it was %s" % first
object = filterElevation("AD", filtered).collect()[4]
assert object == 2321, "the value of the RDD element was expected to be 2321 but it was %s" % object


# ## Elevation Average
# 
# `elevationAverage` calculates the dem average to specific dataset.  
# 
# param `data`: RDD[int] containing only dem information  
# 
# `return`: The average elevation  
# 

# In[23]:


def elevationAverage(data):
    return data         .map(lambda dem: (1, (dem, 1)))         .reduceByKey(lambda x, y: ((x[0] + y[0], x[1] + y[1])))         .mapValues(lambda total: total[0]/total[1])         .collectAsMap().get(1, 0.0)

    raise NotImplementedError()


# In[24]:


#Example print

elevationAverage(sc.parallelize(filterElevation("AD", extractData(testFile)).take(5)))


# In[25]:


'''elevationAverage tests'''
avg = elevationAverage(sc.parallelize([1, 2, 3 ,4])) 
assert abs(avg - 2.5) < 0.00001, "the average was expected to be 2.5 but it was %s" % avg
filtered = extractData(testFile)
elevations = filterElevation("AD", filtered)
avg2 = elevationAverage(elevations)
assert abs(avg2 - 1792.25) < 0.00001, "the average was expected to be 1792.25 but it was %s" % avg2
assert type(avg2) is float, "the type of the RDD element was expected to be float but it was %s" % type(avg2)


# ## Most Common Words
# 
# `mostCommonWords` calculates what is the most common  word in place names and returns an RDD[(String,Int)]. You can assume that words are separated by a single space ' '.
# 
# param `data`: an RDD containing multiple Array["name", "countryCode", "dem"].  
# 
# `return`: RDD[(String,Int)] where string is the word and Int number of occurances. RDD should be in descending order (sorted by number of occurances). e.g ("hotel", 234), ("airport", 120), ("new", 12). 
# 
# Example:  
# Assume that the place name is "Andorra la Vella Heliport". We split the name so that we have 4 seperate words "Andorra", "la", "Vella" and "Heliport".
# 

# In[27]:


def mostCommonWords(data):
    return data         .flatMap(lambda record: record[0].split(" "))         .map(lambda word: (word, 1))         .reduceByKey(lambda x, y: x + y).sortBy(lambda a: a[1], ascending=False)

    raise NotImplementedError()


# In[28]:


#Example print

mostCommonWords(extractData(testFile)).take(5)


# In[29]:


'''mostCommonWords tests'''
filtered = extractData(testFile)
words = mostCommonWords(filtered).collect()
first = words[0]
second = words[1]
third = words[2]
assert type(first[0]) is str, "the type of the first value in array was expected to be str but it was %s" % type(first[0])
assert type(first[1]) is int, "the type of the second value in array was expected to be int but it was %s" % type(first[1])
assert first[1] >= second[1], "the first element in RDD was expected to have more occurances than the second"
assert first[0] == "Hotel", "the first element was expected to be named Hotel but it was %s" % first[0]
assert first[1] == 22, "the count of the first element was expected to be 22 but it was %s" % first[1]
assert third[0] == "la", "the third element was expected to be named 'la' but it was %s" % third[0]


# ## Most Common Country
# 
# `mostCommonCountry` tells which country has the most entries in geolocation data. The correct name for specific countrycode can be found from countrycodes.csv. The columns in countrycodes.csv are seperated by ",". More specifially, the file is structured like this:
# 
# Fiji,FJ  
# Finland,FI  
# France,FR  
# 
# param `data`: an RDD containing multiple Array["name", "countryCode", "dem"].  
# param `codeData`: data from countrycodes.csv file  
# 
# `return`: most common country as String e.g Finland or empty string "" if countrycodes.csv doesn't have that entry.
# 

# In[30]:


countryCodes = sc.textFile("countrycodes.csv")

def mostCommonCountry(data, codeData):
    country_code = data                 .map(lambda record: (record[1], 1))                 .reduceByKey(lambda x, y: x + y)                 .sortBy(lambda key: key[1], ascending=False)                 .take(1)[0][0]
    pre_result = countryCodes.filter(lambda a, c=country_code: a[-2:] == c).map(lambda a: a[:-3]).collect()
    if len(pre_result):
        return pre_result[0]
    else:
        return ""
    
    raise NotImplementedError()


# In[31]:


#Example print

mostCommonCountry(extractData(testFile), countryCodes)


# In[32]:


'''mostCommonCountry tests'''
filtered = extractData(testFile)
mostCommon = mostCommonCountry(filtered, countryCodes)
assert type(mostCommon) is str, "the type of the returned object was expected to be str but it was %s" % type(mostCommon)
assert mostCommon == "Sweden", "the most common was expected to be Sweden but it was %s" % mostCommon
mostCommon2 = mostCommonCountry(sc.parallelize(filtered.take(40)), countryCodes)
assert mostCommon2 == "Andorra", "the most common was expected to be Andorra but it was %s" % mostCommon2
false = sc.parallelize([["a", "AA", 123], ["b", "AA", 1234]])
assert mostCommonCountry(false, countryCodes) == "", "The method was expected to return empty when called with false data"


# ## Hotels In Area
# 
# `hotelsInArea` determines how many hotels there are within 10 km (<=10000.0) from given latitude and longitude.
# Use Haversine formula ( https://en.wikipedia.org/wiki/Haversine_formula ). Earth radius is 6371000 meters. 
# 
# In this exercise you should use the asciiname field instead of name. Start by reading the data and getting the correct fields (asciiname, latitude, longitude) similarly to the `extractData` function. After that you should use the Haversine formula to filter the places in 10 Km radius from the latitude and longitude. You will probably want to use a helper function, Python lets you create functions inside functions. Finally, you will want to filter the places that contain the word "hotel". Location is a hotel if the name contains the word "hotel" (can be "Hotel" or "hOtel" for instance). There can exist multiple hotels in the same location. You should not count the same hotel multiple times.
# 
# Note that both latitude and longitude in the data are in decimal degree so you have to change them to radians ( https://en.wikipedia.org/wiki/Decimal_degrees ). They should also be converted to double values. E.g `math.radians(float(x))`
# 
# param `lat`: latitude as Double  
# param `long`: longitude as Double  
# param `data`: the original data set loaded into spark as RDD[String].  
# 
# `return`: number of hotels in area
# 

# In[34]:


import math

def hotelsInArea(lat, long, data):
    
    def filter_distance(lat1, long1, lat2=lat, long2=long):
        from math import radians, cos, sin, asin, sqrt
        lat1, long1, lat2, long2 = map(lambda x: radians(float(x)), [lat1, long1, lat2, long2])
        dlong = long2 - long1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlong/2)**2
        c = 2 * asin(sqrt(a)) 
        m = 6371000 * c
        if m <=10000.0:
            return True
        else:
            return False
        
    
    return data.map(lambda line: line.split("\t"))          .map(lambda record: [record[2], float(record[4]), float(record[5])])          .filter(lambda record: filter_distance(record[1], record[2]))         .filter(lambda name: "hotel" in name[0].lower().split(" "))         .map(lambda record: record[0])         .distinct()         .count()
    
    raise NotImplementedError()


# In[35]:


#Example print
hotelsInArea(59.334591, 18.063240, testFile)


# In[36]:


'''hotelsInArea tests'''
a1 = hotelsInArea(42.5423, 1.5977, testFile)
a2 = hotelsInArea(59.334591, 18.063240, testFile)
a3 = hotelsInArea(63.8532, 15.5652, testFile)
assert a1 == 0, "the number of hotels was expected to be 0 but it was %s" % a1
assert a2 == 3, "the number of hotels was expected to be 3 but it was %s" % a2
assert a3 == 1, "the number of hotels was expected to be 1 but it was %s" % a3


# In[37]:


sc.stop()


# In[ ]:




