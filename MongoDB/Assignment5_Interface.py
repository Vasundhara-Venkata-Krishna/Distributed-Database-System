#
# Assignment5 Interface
# Name: 
#
from pymongo import MongoClient
from pymongo.collation import Collation
import os
import sys
import json
import math
import re

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    # pass
    BusinessDocs = collection.find({"city": re.compile('^' + cityToSearch + '$', re.IGNORECASE)})
    location = saveLocation1
    outputFile = open(location, 'w')

    for Docs in BusinessDocs:
        Docs_name = Docs["name"].upper()
        Docs_full_address = Docs["full_address"].upper()
        Docs_city = Docs["city"].upper()
        Docs_state = Docs["state"].upper()
        Docs_write_line = Docs_name + "$" + Docs_full_address + "$" + Docs_city + "$" + Docs_state + "\n"
        outputFile.write(Docs_write_line)
    outputFile.close()


def DistanceCalculation(lat2, long2, lat1, long1):
    R = 3959
    x1 = math.radians(lat1)
    # print("x1 = ", x1)
    x2 = math.radians(lat2)
    # print("x2 = ", x2)
    lat_diff = (lat2 - lat1)
    del_lat = math.radians(lat_diff)
    long_diff = (long2 - long1)
    del_long = math.radians(long_diff)

    a = (math.sin(del_lat / 2) * math.sin(del_lat / 2)) + (
                math.cos(x1) * math.cos(x2) * math.sin(del_long / 2) * math.sin(del_long / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    # print("c = ", c)
    d = R * c
    # print("d = ", d)

    return d


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    # pass
    BusinessDocs = collection.find({"categories": {"$in": categoriesToSearch}})
    # "$elemMatch" : { "$all" : categoriesToSearch }
    myLat = myLocation[0]
    myLat1 = float(myLat)
    myLong = myLocation[1]
    myLong1 = float(myLong)
    maxDistance1 = float(maxDistance)
    location1 = saveLocation2
    outputFile1 = open(location1, 'w')

    for Docs in BusinessDocs:
        Docs_name = Docs["name"].upper()
        Docs_long = Docs["longitude"]
        Docs_long1 = float(Docs_long)
        Docs_lat = Docs["latitude"]
        Docs_lat1 = float(Docs_lat)
        distance_calc = DistanceCalculation(myLat1, myLong1, Docs_lat1, Docs_long1)
        # print("orginal : ", distance_calc)
        distance_calc1 = abs(distance_calc)
        # print("Absolute : ", distance_calc1)
        if (distance_calc1 <= maxDistance1):
            outputFile1.write(Docs_name)
            outputFile1.write("\n")
    outputFile1.close()
    # distance_calc =

    # ,{" distance ":{ "$gt" : maxDistance},{"categories" : re.compile('^' + categoriesToSearch + '$', re.IGNORECASE)}}},{"name": 1})

