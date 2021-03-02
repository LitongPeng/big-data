import json
import re
import matplotlib.pyplot as plt

from pymongo import MongoClient


# load the json file given by professor to the imdb database
def load_extra():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    # built new collection "extra"
    myset = db['extra']
    # read the file and insert every line to the "extra" collection
    for line in open('/Users/penglitong/Desktop/extra-data.json'):
        data = json.loads(line)
        myset.insert(data)
        # close the collection
    con.close()


# add the information of given file to "movies" collection
def load_movies_and_extra():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    # build new collection "movies_and_extra"
    movies_and_extra = db['movies_and_extra']
    movies = db['movies']
    pipeline = [{"$lookup":  # join the "movies" collection and "extra" collection
                 # use "id" from "movies" collection
                 # and "IMDb_ID.value" from "extra" collection to match
                     {"from": "extra",
                      "localField": "id",
                      "foreignField": "IMDb_ID.value",
                      "as": "temp"}},
                {"$project":
                     {"id": 1,
                      "type": 1,
                      "title": 1,
                      "originaltitle": 1,
                      "startyear": 1,
                      "endyear": 1,
                      "runtime": 1,
                      "avgrating": 1,
                      "numvotes": 1,
                      "genres": 1,
                      "actors": 1,
                      "directors": 1,
                      "writers": 1,
                      "producers": 1,
                      # box office revenue in US dollars,
                      #and choose the first value if there are duplicates
                      "temp.[0].box_office_currencyLabel": 1,
                      # cost of the movie,
                      # and choose the first value if there are duplicates
                      "temp.[0].cost": 1,
                      # distributor,
                      # and choose the first value if there are duplicates
                      "temp.[0].distributorLabel": 1,
                      # rating,
                      # and choose the first value if there are duplicates
                      "temp.[0].MPAA_film_ratingLabel": 1}}]
    for doc in (movies.aggregate(pipeline)):
        movies_and_extra.insert_one(doc)
    print("done")


def load_movies_and_extra2():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    movies_and_extra = db['movies_and_extra']
    movies = db['movies']
    pipeline = [{"$lookup":  # join the "movies" collection and "extra" collection
                 # use "title" from "movies" collection
                 # and "titleLabel.value" from "extra" collection to match
                     {"from": "extra",
                      "localField": "title",
                      "foreignField": "titleLabel.value",
                      "as": "temp"}},
                {"$project":
                     {"type": 1,
                      "title": 1,
                      "originaltitle": 1,
                      "startyear": 1,
                      "endyear": 1,
                      "runtime": 1,
                      "avgrating": 1,
                      "numvotes": 1,
                      "genres": 1,
                      "actors": 1,
                      "directors": 1,
                      "writers": 1,
                      "producers": 1,
                      # box office revenue in US dollars,
                      # and choose the first value if there are duplicates
                      "temp.[0].box_office_currencyLabel": 1,
                      # cost of the movie,
                      # and choose the first value if there are duplicates
                      "temp.[0].cost": 1,
                      # distributor,
                      # and choose the first value if there are duplicates
                      "temp.[0].distributorLabel": 1,
                      # rating,
                      # and choose the first value if there are duplicates
                      "temp.[0].MPAA_film_ratingLabel": 1}}]
    for doc in (movies.aggregate(pipeline)):
        movies_and_extra.insert_one(doc)
    print("done")


def first_query():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    # build new collection "each_genre_movies"
    each_genre_movies = db['each_genre_movies']
    pipeline = [
        # unwind the "genres" array
        {"$unwind": "$genres"},
        # clear the empty list of "genres"
        {"$match": {"genres": {"$exists": True}}},
        # group average ratings by "genres.name"
        {"$group": {"_id":"$genres.name", "avgRating_after": {"$avg": "$avgRating"}}}]
    # a new collection with genres and their rating
    for doc in (db.movies.aggregate(pipeline)):
        each_genre_movies.insert_one(doc)
        # filter movies with more than 10k votes
    for doc in each_genre_movies.aggregate([{"$project": {"genres_after": 1}}]):
        if db.movies.count({"genres": doc}) > 10000:
            print(each_genre_movies.find({"genres_after": doc}))


def second_query():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    # build a new dictionary
    dic = {}
    # unwind the "genres" array and only need "genres.name"
    for doc in (db.movies.aggregate([{"$unwind": "$genres"}, {"$project": {"genres.name": 1, "_id": 0}}])):
        # transfer the result from list to string,
        # and clear all special symbols(includes"{","}",":","'")
        line = str(doc).replace("{", " ").replace("}", " ").replace(":", " ").replace("'", " ")
        # pick the third words from string
        doc = re.findall('[a-zA-Z]+', line)[2]
        # if this genre exists in dictionary,
        if doc in dic.keys():
            # then add its number
            dic[doc] += 1
            # if this genre does not exist in the dictionary
        else:
            # then make this genre's value to 1
            dic[doc] = 1
    print(dic)
    #plot the result
    #x axis is the key of the dic,
    # and y axis is the value of the dic
    plt.bar(dic.keys(),dic.values(),width=0.6,color='g')
    #the label of x axis
    plt.xlabel('genre')
    # the label of y axis
    plt.ylabel('number of actors')
    # the title of plot
    plt.title('4.2')
    # show the plot
    plt.show()


def third_query():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    # build a new dictionary
    dic = {}
    # we only need startyear
    for doc in (db.movies.aggregate([{"$project": {"startyear": 1, "_id": 0}}])):
        # transfer the result from list to string,
        # and clear all special symbols(includes"{","}",":","'")
        line = str(doc).replace("{", " ").replace("}", " ").replace(":", " ").replace("'", " ")
        # pick the second words (includes number) from string
        doc = re.findall('[a-zA-Z0-9]+', line)[1]
        # if this genre exists in dictionary,
        if doc in dic.keys():
            # then add its number
            dic[doc] += 1
            # if this genre does not exist in the dictionary
        else:
            # then make this genre's value to 1
            dic[doc] = 1
    print(dic)
    # plot the result
    # x axis is the key of the dic,
    plt.scatter(dic.keys(), dic.values(),alpha=0.6)
    # the label of x axis
    plt.xlabel('startyear')
    # the label of y axis
    plt.ylabel('number of movies')
    # the title of plot
    plt.title('4.3')
    # show the plot
    plt.show()

def main():
    load_extra()
    load_movies_and_extra()
    load_movies_and_extra2()
    print("the 4.1 query")
    first_query()
    print("the 4.2 query")
    second_query()
    print("the 4.3 query")
    third_query()


if __name__ == '__main__':
    main()
