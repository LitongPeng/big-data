import time

from pyspark.shell import spark
from pyspark.sql.functions import isnull


def q():
    # load the file of name.basics.tsv
    name_basics = spark.read \
        .option('delimiter', '\t') \
        .option('header', True) \
        .csv('/Users/penglitong/Desktop/name.basics.tsv')
    # load the file of title.principals.tsv
    title_principals = spark.read \
        .option('delimiter', '\t') \
        .option('header', True) \
        .csv('/Users/penglitong/Desktop/title.principals.tsv')
    # load the file of title.basics.tsv
    title_basics = spark.read \
        .option('delimiter', '\t') \
        .option('header', True) \
        .csv('/Users/penglitong/Desktop/title.basics.tsv')
    # mark the start time
    start = time.time()
    # the first query
    # in the name.basics.tsv file, deathYear should be NULL and primaryName should be start with 'Phi'
    # join the title.principals.tsv file, the same field is nconst, and category should be actor
    # join the title.basics.tsv file, the same field is tconst, and startYear should not be 2014
    name_basics.where("deathYear like '%N'").where("primaryName like 'Phi%'") \
        .join(title_principals, 'nconst').where("category='actor'") \
        .join(title_basics, 'tconst').where("startYear!=2014").show(10)
    # mark the first query end time
    first_end = time.time()
    # print the runtime of first query
    print("the first query runs " + str(first_end - start) + "s")
    # the second query in the name.basics.tsv file, primaryName should be include 'Gill'
    # join the title.principals.tsv file, the same field is nconst,
    # and category should be producer
    # join the title.basics.tsv file, the same field is tconst,
    # startYear should not be 2017 and genresshould include Talk-Show
    # count how many times each primaryName shows and sort them form most to least
    name_basics.where("primaryName like '%Gill%'") \
        .join(title_principals, 'nconst').where("category='producer'") \
        .join(title_basics, 'tconst').where("startYear=2017").where("genres like '%Talk-Show%'") \
        .groupby("primaryName").count().orderBy('count', ascending=0).show(10)
    # mark the second query end time
    second_end = time.time()
    # print the runtime of second query
    print("the second query runs " + str(second_end - first_end) + "s")
    # the third query
    # in the name.basics.tsv file, deathYear should be NULL
    # join the title.principals.tsv file, the same field is nconst, and category should be producer
    # join the title.basics.tsv file, and runtimeMinutes should be more than 120
    # count how many times each primaryName shows and sort them form most to least
    name_basics.where("deathYear like '%N'") \
        .join(title_principals, 'nconst').where("category='producer'") \
        .join(title_basics, 'tconst').where("runtimeMinutes>120") \
        .groupby("primaryName").count().orderBy('count', ascending=0).show(10)
    # mark the third query end time
    third_end = time.time()
    # print the runtime of third query
    print("the third query runs " + str(third_end - second_end) + "s")
    # the forth query in the name.basics.tsv file, deathYear should be NULL
    # join the title.principals.tsv file,
    # the same field is nconst, and category should be actor characters should be include Jesus,
    # also characters should be include Christ
    name_basics.where("deathYear like '%N'") \
        .join(title_principals, 'nconst').where("category='actor'").where("characters like'%Jesus%'").where(
        "characters like'%Christ%'").show(10)
    # mark the forth query end time
    forth_end = time.time()
    # print the runtime of forth query
    print("the forth query runs " + str(forth_end - third_end) + "s")


def main():
    q()


if __name__ == '__main__':
    main()
