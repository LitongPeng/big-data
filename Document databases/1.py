import time
from pymongo import MongoClient
import csv

# first, export the sql file from pgadmin to csv file
# load csv file of movie table into mongodb
def load_movie():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db.movie
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/movie.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# load csv file of genre table into mongodb
def load_genre():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db.genre
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/genre.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# load csv file of movie_genre table into mongodb
def load_moviegenre():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db.movie_genre
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/movie_genre.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# load csv file of member table into mongodb
def load_members():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db.members
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/member.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# load csv file of movie_actor table into mongodb
def load_movieactor():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db.movie_actor
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/movie_actor.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# load csv file of movie_writer table into mongodb
def load_moviewriter():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db.movie_writer
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/movie_writer.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# load csv file of movie_director table into mongodb
def load_moviedirector():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db.movie_director
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/movie_director.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# load csv file of movie_producer table into mongodb
def load_movieproducer():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db['movie_producer']
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/movie_producer.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# load csv file of role table into mongodb
def load_role():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db.role
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/role.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# load csv file of actor_movie_role table into mongodb
def load_actormovierole():
    con = MongoClient("localhost")
    db = con['imdb']
    myset = db.actor_movie_role
    myset.delete_many({})
    with open('/Users/penglitong/Desktop/untitled folder/actor_movie_role.csv', 'r', encoding='utf-8')as csvfile:
        reader = csv.DictReader(csvfile)
        counts = 0
        for each in reader:
            myset.insert_one(each)
            counts += 1
            print('成功添加了' + str(counts) + '条数据 ')


# combine the movie_actor table and actor_movie_role table
def load_movieactor_and_actormovierole():
    con = MongoClient("localhost")
    db = con['imdb']
    movieactor_and_actormovierole = db['movieactor_actormovierole']
    pipeline = [{"$lookup":
                     {"from": "actor_movie_role",
                      "localField": "id",
                      "foreignField": "movie",
                      "as": "temp"}}]
    for doc in (db.movie_actor.aggregate(pipeline)):
        movieactor_and_actormovierole.insert_one(doc)
    print("done")


# load the data for movies table
def load_movies_table():
    con = MongoClient("localhost")
    db = con['imdb']
    movies_table = db['movies']
    pipeline = [{"$lookup":  # combine movie table and movie_genre table
                     {"from": "movie_genre",
                      "localField": "id",
                      "foreignField": "movie",
                      "as": "movie_connect_genre"}},
                # combine the last table to genre table
                {"$lookup":
                     {"from": "genre",
                      "localField": "movie_connect_genre.genre",
                      "foreignField": "id",
                      "as": "genres"}},
                # combine the last table to movieactor_actormovierole table
                {"$lookup":
                     {"from": "movieactor_actormovierole",
                      "localField": "id",
                      "foreignField": "movie",
                      "as": "actors"}},
                # combine the last table to movie_director table
                {"$lookup":
                     {"from": "movie_director",
                      "localField": "id",
                      "foreignField": "movie",
                      "as": "directors"}},
                # combine the last table to movie_writer table
                {"$lookup":
                     {"from": "movie_writer",
                      "localField": "id",
                      "foreignField": "movie",
                      "as": "writers"}},
                # combine the last table to movie_producer table
                {"$lookup":
                     {"from": "movie_producer",
                      "localField": "id",
                      "foreignField": "movie",
                      "as": "producers"}},
                # select what movies table need
                {"$project":
                     {"type": 1,
                      "title": 1,
                      "originaltitle": 1,
                      "startyear": 1,
                      "endyear": 1,
                      "runtime": 1,
                      "avgrating": 1,
                      "numvotes": 1,
                      "genres.name": 1,
                      "actors.temp.actor": 1,
                      "actors.temp.role": 1,
                      "directors.director": 1,
                      "writers.writer": 1,
                      "producers.producer": 1}}]
    for doc in (db.movie.aggregate(pipeline)):
        movies_table.insert_one(doc)
    print("done")


# the first query
def first_query():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    pipeline = [{"$lookup":  # join movies table and members table
                     {"from": "members",
                      "localField": "actors.actor",  # select actors
                      "foreignField": "id",
                      "as": "all"}},
                # select alive actors
                {"$match": {"members.deathyear": None}},
                # select actors whose name starts "Phi"
                {"$match": {"members.name": {"$regex": "/^Phi/"}}},
                # select movies in 2014
                {"$match": {"movies.startyear": "2014"}},
                # select actors did not participate in any movie
                {"$match": {"movies.actors.role": None}}]
    start_time = time.time()
    for doc in (db.movies.aggregate(pipeline)):
        print(doc)
    end_time = time.time()
    print("the first query runs " + str(end_time - start_time) + "s")


# the second query
def second_query():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    pipeline = [{"$lookup":  # join movies table and members table
                     {"from": "members",
                      "localField": "movie_producer.producer",  # select producer
                      "foreignField": "id",
                      "as": "all"}},
                # select talk shows
                {"$match": {"movies.genre": "Talk-Show"}},
                # select whose name contains "Gill"
                {"$match": {"member.name": {"$regex": "Gill"}}},
                # select movies in 2017
                {"$match": {"movies.startyear": "2017"}}]
    # count the number of every conditional movies
    dic = dict()
    start_time = time.time()
    for doc in (db.movies.aggregate(pipeline)):
        if dic[doc] is None:
            dic[doc] = 1
        else:
            dic[doc] += 1
    # select conditional movies more than 50
    if dic:
        print({k: v for k, v in dic.items() if v > 50})
    else:
        print("nobody")
    end_time = time.time()
    print("the second query runs " + str(end_time - start_time) + "s")


# the third query
def third_query():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    pipeline = [{"$lookup":  # join movies table and members table
                     {"from": "members",
                      "localField": "movie_writer.writer",  # select writer
                      "foreignField": "id",
                      "as": "all"}},
                # select alive writers
                {"$match": {"members.deathyear": None}},
                # select writers whose name contains "Bhardwaj"
                {"$match": {"members.name": {"$regex": "Bhardwaj"}}},
                # count the average runtime of conditional movies
                {"$group": {"_id": "null", "avgRuntime": {"$avg": "$movies.runtime"}}}]
    start_time = time.time()
    for doc in (db.movies.aggregate(pipeline)):
        print(doc)
    end_time = time.time()
    print("the third query runs " + str(end_time - start_time) + "s")


# the fourth query
def fourth_query():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    pipeline = [{"$lookup":  # join movies table and members table
                     {"from": "members",
                      "localField": "movie_producer.producer",  # select producer
                      "foreignField": "id",
                      "as": "all"}},
                # select alive producers
                {"$match": {"member.deathyear": None}},
                # select long-run movies
                {"$match": {"movie.runtime": {"$gt": 120}}}]
    # count the number of every conditional movies
    dic = dict()
    start_time = time.time()
    for doc in (db.movies.aggregate(pipeline)):
        if dic[doc] is None:
            dic[doc] = 1
        else:
            dic[doc] += 1
    # select the maximum
    if dic:
        print(max(dic, key=dic.get))
    else:
        print("nobody")
    end_time = time.time()
    print("the fourth query runs " + str(end_time - start_time) + "s")


# the fifth query
def fifth_query():
    # connect to the mongodb
    con = MongoClient("localhost")
    # connect to the imdb database
    db = con['imdb']
    pipeline = [{"$lookup":  # join movies table and members table
                     {"from": "members",
                      "localField": "actors.actor",  # select actors
                      "foreignField": "id",
                      "as": "temp_all"}},
                # select the Sci-Fi movies
                {"$match": {"movies.genre": "Sci-Fi"}},
                # select the movies acted in by Sigourney
                {"$match": {"member.name": "Sigourney"}},
                # join the movies table and members table and select directors
                {"$lookup":
                     {"from": "members",
                      "localField": "movies.movie_director.director",
                      "foreignField": "id",
                      "as": "all"}},
                # select the movies directed by James Cameron
                {"$match": {"members.name": "James Cameron"}}]
    start_time = time.time()
    for doc in (db.movies.aggregate(pipeline)):
        print(doc)
    end_time = time.time()
    print("the fifth query runs " + str(end_time - start_time) + "s")


# the first query explanation
def first_query_explanation():
    con = MongoClient("localhost")
    db = con['imdb']
    pipeline = [{"$lookup":
                     {"from": "members",
                      "localField": "actors.actor",
                      "foreignField": "id",
                      "as": "all"}},
                {"$match": {"members.deathyear": None}},
                {"$match": {"members.name": {"$regex": "/^Phi/"}}},
                {"$match": {"movies.startyear": "2014"}},
                {"$match": {"movies.actors.role": None}}]
    print(db.command('aggregate', 'movies', pipeline=pipeline, explain=True))


# the second query explanation
def second_query_explanation():
    con = MongoClient("localhost")
    db = con['imdb']
    pipeline = [{"$lookup":
                     {"from": "members",
                      "localField": "movie_producer.producer",  # select producer
                      "foreignField": "id",
                      "as": "all"}},
                {"$match": {"movies.genre": "Talk-Show"}},
                {"$match": {"member.name": {"$regex": "Gill"}}},
                {"$match": {"movies.startyear": "2017"}}]
    print(db.command('aggregate', 'movies', pipeline=pipeline, explain=True))


# the third query explanation
def third_query_explanation():
    con = MongoClient("localhost")
    db = con['imdb']
    pipeline = [{"$lookup":
                     {"from": "members",
                      "localField": "movie_writer.writer",
                      "foreignField": "id",
                      "as": "all"}},
                {"$match": {"members.deathyear": None}},
                {"$match": {"members.name": {"$regex": "Bhardwaj"}}},
                {"$group": {"_id": "null", "avgRuntime": {"$avg": "$movies.runtime"}}}]
    print(db.command('aggregate', 'movies', pipeline=pipeline, explain=True))


# the fourth query explanation
def fourth_query_explanation():
    con = MongoClient("localhost")
    db = con['imdb']
    pipeline = [{"$lookup":
                     {"from": "members",
                      "localField": "movie_producer.producer",
                      "foreignField": "id",
                      "as": "all"}},
                {"$match": {"member.deathyear": None}},
                {"$match": {"movie.runtime": {"$gt": 120}}}]
    print(db.command('aggregate', 'movies', pipeline=pipeline, explain=True))


# the fifth query explanation
def fifth_query_explanation():
    con = MongoClient("localhost")
    db = con['imdb']
    pipeline = [{"$lookup":
                     {"from": "members",
                      "localField": "actors.actor",
                      "foreignField": "id",
                      "as": "temp_all"}},
                {"$match": {"movies.genre": "Sci-Fi"}},
                {"$match": {"member.name": "Sigourney"}},
                {"$lookup":
                     {"from": "members",
                      "localField": "movies.movie_director.director",
                      "foreignField": "id",
                      "as": "all"}},
                {"$match": {"members.name": "James Cameron"}}]
    print(db.command('aggregate', 'movies', pipeline=pipeline, explain=True))


# the first index query
def first_index_query():
    con = MongoClient("localhost")
    db = con['imdb']
    table_all = db.table_all
    pipeline1 = [{"$lookup":
                      {"from": "members",
                       "localField": "actors.actor",
                       "foreignField": "id",
                       "as": "table_all"}}]
    db.movies.aggregate(pipeline1)
    #create indexes
    table_all.create_index(
        [('members.deathyear', 1), ('members.name', 1), ('movies.startyear', 1), ('movies.actors.role', 1)])
    pipeline2 = {"members.deathyear": None,
                 "members.name": {"$regex": "/^Phi/"},
                 "movies.startyear": "2014",
                 "movies.actors.role": None}
    start_time = time.time()
    for doc in (db.movies.find(pipeline2)):
        print(doc)
    end_time = time.time()
    print("the first index query runs " + str(end_time - start_time) + "s")


# the second index query
def second_index_query():
    con = MongoClient("localhost")
    db = con['imdb']
    table_all = db.table_all
    pipeline1 = [{"$lookup":
                      {"from": "members",
                       "localField": "movie_producer.producer",
                       "foreignField": "id",
                       "as": "table_all"}}]
    db.movies.aggregate(pipeline1)
    # create indexes
    table_all.create_index([("movies.genre", 1), ('member.name', 1), ("movies.startyear", 1)])
    pipeline2 = {"movies.genre": "Talk-Show",
                 "member.name": {"$regex": "Gill"},
                 "movies.startyear": "2017"}
    dic = dict()
    start_time = time.time()
    for doc in (db.movies.find(pipeline2)):
        if dic[doc] is None:
            dic[doc] = 1
        else:
            dic[doc] += 1
    if dic:
        print({k: v for k, v in dic.items() if v > 50})
    else:
        print("nobody")
    end_time = time.time()
    print("the second index query runs " + str(end_time - start_time) + "s")


# the third index query
def third_index_query():
    con = MongoClient("localhost")
    db = con['imdb']
    table_all = db.table_all
    pipeline1 = [{"$lookup":
                      {"from": "members",
                       "localField": "movie_writer.writer",
                       "foreignField": "id",
                       "as": "table_all"}}]
    db.movies.aggregate(pipeline1)
    # create indexes
    table_all.create_index([("member.deathyear", 1), ("member.name", 1)])
    pipeline2 = [{"$match": {"members.deathyear": None}},
                 # select writers whose name contains "Bhardwaj"
                 {"$match": {"members.name": {"$regex": "Bhardwaj"}}},
                 # count the average runtime of conditional movies
                 {"$group": {"_id": "null", "avgRuntime": {"$avg": "$movies.runtime"}}}]
    start_time = time.time()
    for doc in (db.movies.aggregate(pipeline2)):
        print(doc)
    end_time = time.time()
    print("the third index query runs " + str(end_time - start_time) + "s")


# the fourth index query
def fourth_index_query():
    con = MongoClient("localhost")
    db = con['imdb']
    table_all = db.table_all
    pipeline1 = [{"$lookup":
                      {"from": "members",
                       "localField": "movie_producer.producer",
                       "foreignField": "id",
                       "as": "table_all"}}]
    db.movies.aggregate(pipeline1)
    # create indexes
    table_all.create_index([("member.deathyear", 1), ("movie.runtime", 1)])
    pipeline2 = {"member.deathyear": None,
                 "movie.runtime": {"$gt": 120}}
    dic = dict()
    start_time = time.time()
    for doc in (db.movies.find(pipeline2)):
        if dic[doc] is None:
            dic[doc] = 1
        else:
            dic[doc] += 1
    # select the maximum
    if dic:
        print(max(dic, key=dic.get))
    else:
        print("nobody")
    end_time = time.time()
    print("the fourth index query runs " + str(end_time - start_time) + "s")


# the fifth index query
def fifth_index_query():
    con = MongoClient("localhost")
    db = con['imdb']
    table_all = db.table_all
    pipeline1 = [{"$lookup":
                      {"from": "members",
                       "localField": "actors.actor",  # select actors
                       "foreignField": "id",
                       "as": "temp_all"}},
                 {"$lookup":
                      {"from": "members",
                       "localField": "movies.movie_director.director",
                       "foreignField": "id",
                       "as": "table_all"}}]
    db.movies.aggregate(pipeline1)
    # create indexes
    table_all.create_index([("temp_all.movies.genre", 1), ("temp_all.member.name", 1), ("members.name", 1)])
    pipeline2 = {"temp_all.movies.genre": "Sci-Fi",
                 "temp_all.member.name": "Sigourney",
                 "members.name": "James Cameron"}
    start_time = time.time()
    for doc in (db.movies.find(pipeline2)):
        print(doc)
    end_time = time.time()
    print("the fifth index query runs " + str(end_time - start_time) + "s")


def main():
    load_movie()
    load_genre()
    load_moviegenre()
    load_members()
    load_movieactor()
    load_moviewriter()
    load_moviedirector()
    load_movieproducer()
    load_role()
    load_actormovierole()
    load_movieactor_and_actormovierole()
    load_movies_table()
    first_query()
    second_query()
    third_query()
    fourth_query()
    fifth_query()
    first_query_explanation()
    second_query_explanation()
    third_query_explanation()
    fourth_query_explanation()
    fifth_query_explanation()
    first_index_query()
    second_index_query()
    third_index_query()
    fourth_index_query()
    fifth_index_query()


if __name__ == '__main__':
    main()
