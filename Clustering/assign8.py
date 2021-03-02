import math
import re

import numpy
from pymongo import MongoClient


# q1
def pre():
    con = MongoClient("localhost")
    db = con['assign6']
    pre = db['pre']
    pipeline = [{"$match":
                     {"type": "movie",  # movies type is 'movie'
                      "numVotes": {"$gt": 10000}}},  # number of votes are greater than 10000
                {"$project":  # we only need startYear and avgRating
                     {"startYear": 1,
                      "avgRating": 1}}
                ]
    for row in (db.movies.aggregate(pipeline)):
        pre.insert_one(row)
    print("done")


# find the minimum and maximum value of avgRating and startYear
def Min_Max():
    con = MongoClient("localhost")
    db = con['assign6']
    pre = db['pre']
    # the minimum avgRating
    for min_ar in pre.aggregate([{"$sort": {"avgRating": 1}}, {"$limit": 1}]):
        print(min_ar)
        # {'_id': 7221896, 'startYear': 2017, 'avgRating': 1.3}
        # the maximum avgRating
    for max_ar in pre.aggregate([{"$sort": {"avgRating": -1}}, {"$limit": 1}]):
        print(max_ar)
        # {'_id': 252487, 'startYear': 1975, 'avgRating': 9.4}
        # the minimum startYear
    for min_sy in pre.aggregate([{"$sort": {"startYear": 1}}, {"$limit": 1}]):
        print(min_sy)
        # {'_id': 4972, 'startYear': 1915, 'avgRating': 6.7}
        # the maximum startYear
    for max_sy in pre.aggregate([{"$sort": {"startYear": -1}}, {"$limit": 1}]):
        print(max_sy)
        # {'_id': 360556, 'startYear': 2018, 'avgRating': 4.9}


# update the value in pre collections to normalized form
def update_to_normalized():
    con = MongoClient("localhost")
    db = con['assign6']
    pre = db['movie_moviegenre_genre']
    q1 = db['q1']
    # clear the data in collection
    q1.delete_many({})
    for row in pre.find({}):
        # movies id
        _id = int(re.findall('[0-9]+', str(row))[0])
        # normalized startYear
        nsy = ((float(re.findall('[0-9]+', str(row))[1]) - 2018) / (9.4 - 1915))
        # normalized avgRating
        nar = (float(re.findall('[0-9]+', str(row))[2]) - 1.3) / (9.4 - 1.3)
        # insert into collection
        q1.insert_one({"_id": _id, "kmeansNorm": [nsy, nar]})


# q2
def q2(k, g):
    con = MongoClient("localhost")
    db = con['assign6']
    c = db.centroids
    c.delete_many({})
    pipeline = [{"$lookup":  # join with genre field
                     {"from": "movies",
                      "localField": "_id",
                      "foreignField": "_id",
                      "as": "genre"}},
                {"$project":
                     {"_id": 1,
                      "kmeansNorm": 1,
                      "genre.genres": 1}},
                {"$match": {"genre.genres": g}},  # genre is input g
                {"$sample": {"size": k}}]  # select k documents ,k is from input
    for row in db.q1.aggregate(pipeline):
        c.insert_one(row)
        # assign the centroid documents IDs from 1 to k
    for i in range(k):
        c.update_one({"ID": None}, {"$set": {"ID": i}})
        i += 1
        # store in a list
    cens = []
    for row in c.find({}, {"kmeansNorm": 1}):
        cens.append(row)
    return cens


# q3
def q3(k, g):
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the assign6 database
    db = con['assign6']
    # centroids
    cens = q2(k, g)
    print(cens)
    # erase previous centroids
    db.centroids.delete_many({})
    # for every document
    for row in (db.q1.find({}, {"kmeansNorm": 1})):
        # id
        _id = int(re.findall('[0-9]+', str(row))[0])
        # normalized startYear
        nsy = float(re.findall('[0-9]+[.][0-9]+', str(row))[0])
        # normalized avgRating
        nar = float(re.findall('[0-9]+[.][0-9]+', str(row))[1])
        # initialize a two dimentianal list to store the distance to each centroid
        find_min = [([0] * 2) for m in range(k)]
        # mark each centroid
        i = 0
        # for each centroid
        for cen in cens:
            # centroid's id
            c_id = int(re.findall('[0-9]+', str(cen))[0])
            # centroid's startYear
            csy = float(re.findall('[0-9]+[.][0-9]+', str(cen))[0])
            # centroid's avgRating
            car = float(re.findall('[0-9]+[.][0-9]+', str(cen))[1])
            # the distance
            distance = numpy.sqrt(numpy.square(nsy - csy) + numpy.square(nar - car))
            # fill in the list
            find_min[i][0] = c_id
            find_min[i][1] = distance
            # count plus one
            i += 1
        # initialize the centroid
        min_distance = math.inf
        # for each centroid
        for m in find_min:
            # if the distance to this centroid is smaller than the min_distance
            if m[1] < min_distance:
                # then this is the min_distance
                min_distance = m[0]
        db.q1.update_one({"_id": _id}, {"$set": {"cluster": min_distance}})
        # the centroids'cluster should be themselves
    # for each centroid
    for cen in cens:
        # centroid's id
        c_id = int(re.findall('[0-9]+', str(cen))[0])
        # update their cluster to themselves
        db.q1.update_one({"_id": c_id}, {"$set": {"cluster": c_id}})
    # update the centroids
    # the centroids' ID
    id = 0
    # for each centroid
    for cen in cens:
        # centroids' ID
        c_id = int(re.findall('[0-9]+', str(cen))[0])
        # total number of normalized startYear
        total_nsy = 0.0
        # total number of normalized avgRating
        total_nar = 0.0
        # count the total number
        num = 0
        # for every document
        for row in (db.q1.find({"cluster": c_id}, {"kmeansNorm": 1})):
            # total number of normalized startYear accumulating
            total_nsy += float(re.findall('[0-9]+[.][0-9]+', str(row))[0])
            # total number of normalized avgRating accumulating
            total_nar += float(re.findall('[0-9]+[.][0-9]+', str(row))[1])
            # count plus one
            num += 1
            # the mean number of normalized startYear
        mean_nsy = total_nsy / num
        # the mean number of normalized avgRating
        mean_nar = total_nar / num
        # new centroids
        db.centroids.insert_one({"_id": id, "kmeansNorm": [mean_nsy, mean_nar]})
        id += 1
    print("done")


# if the centroids is not enough, fill the centroids collection
def fill_c(k, g):
    con = MongoClient("localhost")
    db = con['assign6']
    c = db.centroids
    pipeline = [{"$lookup":
                     {"from": "movies",
                      "localField": "_id",
                      "foreignField": "_id",
                      "as": "genre"}},
                {"$project":
                     {"_id": 1,
                      "kmeansNorm": 1,
                      "genre.genres": 1}},
                {"$match": {"genre.genres": g}},
                {"$sample": {"size": k}}]
    for row in db.q1.aggregate(pipeline):
        c.insert_one(row)
    for i in range(k):
        c.update_one({"ID": None}, {"$set": {"ID": i}})
        i += 1
    cens = []
    for row in c.find({}, {"kmeansNorm": 1}):
        cens.append(row)
    return cens


# q4
def q4(k, g):
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the assign6 database
    db = con['assign6']
    # initialize sse
    sse = 0
    # iterator times
    itera = 0
    # the centroids' ID
    id = 0
    # loop 100 times
    while itera < 100:
        # if this is the first loop
        if itera == 0:
            # centroids
            cens = q2(k, g)
        # else use the new centroids array
        else:
            # initialize cens
            cens = []
            for row in db.centroids.find({}, {"kmeansNorm": 1}):
                cens.append(row)
        #     # erase previous centroids
        # db.centroids.delete_many({})
        # for every document
        for row in (db.q1.find({}, {"kmeansNorm": 1})):
            # id
            _id = int(re.findall('[0-9]+', str(row))[0])
            # normalized startYear
            nsy = float(re.findall('[0-9]+[.][0-9]+', str(row))[0])
            # normalized avgRating
            nar = float(re.findall('[0-9]+[.][0-9]+', str(row))[1])
            # initialize a two dimensional list to store the distance to each centroid
            find_min = [([0] * 3) for m in range(k)]
            # mark each centroid
            i = 0
            # for each centroid
            for cen in cens:
                # centroid's id
                c_id = int(re.findall('[0-9]+', str(cen))[0])
                # centroid's startYear
                csy = float(re.findall('[0-9]+[.][0-9]+', str(cen))[0])
                # centroid's avgRating
                car = float(re.findall('[0-9]+[.][0-9]+', str(cen))[1])
                # the distance
                distance = numpy.sqrt(numpy.square(nsy - csy) + numpy.square(nar - car))
                #
                sse_candidate = numpy.square(nsy - csy) + numpy.square(nar - car)
                # fill in the list
                find_min[i][0] = c_id
                find_min[i][1] = distance
                find_min[i][2] = sse_candidate
                # count plus one
                i += 1
            # initialize the centroid
            min_distance = math.inf
            # for each centroid
            for m in find_min:
                # if the distance to this centroid is smaller than the min_distance
                if m[1] < min_distance:
                    # then this is the min_distance
                    min_distance = m[0]
                    sse_pre = m[2]
            sse += sse_pre
            db.q1.update_one({"_id": _id}, {"$set": {"cluster": min_distance}})
            # the centroids'cluster should be themselves
            # for each centroid
        for cen in cens:
            # centroid's id
            c_id = int(re.findall('[0-9]+', str(cen))[0])
            # update their cluster to themselves
            db.q1.update_one({"_id": c_id}, {"$set": {"cluster": c_id}})
        # update the centroids
        # erase previous centroids
        db.centroids.delete_many({})
        # for each centroid
        for cen in cens:
            # centroids' ID
            c_id = int(re.findall('[0-9]+', str(cen))[0])
            # total number of normalized startYear
            total_nsy = 0.0
            # total number of normalized avgRating
            total_nar = 0.0
            # count the total number
            num = 0
            # for every document
            for row in (db.q1.find({"cluster": c_id}, {"kmeansNorm": 1})):
                # total number of normalized startYear accumulating
                total_nsy += float(re.findall('[0-9]+[.][0-9]+', str(row))[0])
                # total number of normalized avgRating accumulating
                total_nar += float(re.findall('[0-9]+[.][0-9]+', str(row))[1])
                # count plus one
                num += 1
            if num != 0:
                # the mean number of normalized startYear
                mean_nsy = total_nsy / num
                # the mean number of normalized avgRating
                mean_nar = total_nar / num
                db.centroids.insert_one({"_id": id, "kmeansNorm": [mean_nsy, mean_nar]})
                # centroids' id plus one
            id += 1
        c_num = db.centroids.find({}).count()
        # fill the centroids collection
        fill_c(k - c_num, g)
        # iterate
        itera += 1
    print(sse)
    print("done")


# q5
def present():
    con = MongoClient("localhost")
    db = con['assign6']
    # show the final collection so that i can find the regulation
    for row in (db.q1.find({})):
        print(row)
    print("done")


def main():
    # build the  conditional collections  in description
    pre()
    # find the minimum and maximum value of avgRating and startYear
    Min_Max()
    # update the value in pre collections to normalized form
    update_to_normalized()
    # input k
    k = input("please enter how many random documents you want to select")
    # input g
    g = input("please enter what kind of genre you want to select from documents")
    # the second question
    q2(k, g)
    # the third question
    q3(k, g)
    # clear the collection
    update_to_normalized()
    # starting k=10 up to k=50 with a step of 5 using only movies of Action genre
    for k4 in range(10, 51, 5):
        q4(k4, "Action")
        # show the final collection so that i can find the regulation
    present()
    # clear the collection
    update_to_normalized()
    # starting k=10 up to k=50 with a step of 5 using only movies of Horror genre
    for k4 in range(10, 51, 5):
        q4(k4, "Horror")
        # show the final collection so that i can find the regulation
    present()
    # clear the collection
    update_to_normalized()
    # starting k=10 up to k=50 with a step of 5 using only movies of Romance genre
    for k4 in range(10, 51, 5):
        q4(k4, "Romance")
        # show the final collection so that i can find the regulation
    present()
    # clear the collection
    update_to_normalized()
    # starting k=10 up to k=50 with a step of 5 using only movies of Sci-Fi genre
    for k4 in range(10, 51, 5):
        q4(k4, "Sci-Fi")
        # show the final collection so that i can find the regulation
    present()
    # clear the collection
    update_to_normalized()
    # starting k=10 up to k=50 with a step of 5 using only movies of Thriller genre
    for k4 in range(10, 51, 5):
        q4(k4, "Thriller")
        # show the final collection so that i can find the regulation
    present()


if __name__ == '__main__':
    main()
