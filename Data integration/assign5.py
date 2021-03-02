import time

import psycopg2


# the first question
def q1():
    # connect to the postgresql
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    # join the movie_genre table and genre table
    cur.execute("create table moviegenre_and_genre ("
                "movie int,"
                "name varchar(50));")
    cur.execute("insert into moviegenre_and_genre "
                "select a.movie,b.name from movie_genre as a "
                "join genre as b on a.genre=b.id")
    # create ComedyMovie view
    cur.execute("CREATE VIEW ComedyMovie AS"
                " SELECT id,title,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name='Comedy'")
    # create materialized ComedyMovie view
    cur.execute("CREATE MATERIALIZED VIEW ComedyMovie_materialized AS"
                " SELECT id,title,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name='Comedy'")
    # create NonComedyMovie view
    cur.execute("CREATE VIEW NonComedyMovie AS"
                " SELECT id,title,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name!='Comedy'")
    # create materialized NonComedyMovie view
    cur.execute("CREATE MATERIALIZED VIEW NonComedyMovie_materialized AS"
                " SELECT id,title,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name!='Comedy'")
    # join movie_actor table and moviegenre_and_genre table
    cur.execute("create table movieactor_and_moviegenre_and_genre ("
                "actor int,"
                "name varchar(50));")
    cur.execute("insert into movieactor_and_moviegenre_and_genre"
                " select a.actor,b.name from movie_actor as a"
                " join moviegenre_and_genre as b on a.movie=b.movie")
    # create ComedyActor view
    cur.execute("CREATE VIEW ComedyActor AS"
                " SELECT id,member.name,birthYear,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name='Comedy'")
    # create materialized ComedyActor view
    cur.execute("CREATE MATERIALIZED VIEW ComedyActor_materialized AS"
                " SELECT id,member.name,birthYear,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name='Comedy'")
    # create NonComedyActor view
    cur.execute("CREATE VIEW NonComedyActor AS"
                " SELECT id,member.name,birthYear,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name!='Comedy'")
    # create materialized NonComedyActor view
    cur.execute("CREATE MATERIALIZED VIEW NonComedyActor_materialized AS"
                " SELECT id,member.name,birthYear,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name!='Comedy'")
    # create ActedIn view
    cur.execute("CREATE VIEW ActedIn AS"
                " SELECT actor,movie FROM movie_actor")
    # create materialized ActedIn view
    cur.execute("CREATE MATERIALIZED VIEW ActedIn_materialized AS"
                " SELECT actor,movie FROM movie_actor")
    con.commit()
    con.close()
    print("done")


# the second question
def q2():
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    # create All_Movie view
    cur.execute("CREATE VIEW All_Movie AS"
                " SELECT id,title,startyear,'Comedy' AS genre FROM ComedyMovie"
                " UNION ALL"
                " SELECT id,title,startyear,'Non_Comedy' AS genre FROM NonComedyMovie")
    # create All_Actor view
    cur.execute("CREATE VIEW All_Actor AS"
                " SELECT id,name,birthyear,deathyear FROM ComedyActor"
                " UNION ALL"
                " SELECT id,name,birthyear,deathyear FROM NonComedyActor")
    # create All_Movie_Actor view
    cur.execute("CREATE VIEW All_Movie_Actor AS"
                " SELECT actor,movie FROM ActedIn")
    con.commit()
    con.close()
    print("done")


# the 3.1 question
def q3_1():
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT a.name,COUNT(*)"
                " FROM All_Movie AS m JOIN All_Movie_Actor AS ma"
                " ON m.id=ma.movie"
                " JOIN All_Actor AS a"
                " ON a.id=ma.actor"
                " WHERE a.deathyear IS NULL"
                " AND m.startyear<=2005"
                " AND m.startyear>=2000"
                " GROUP BY a.name")
    rows = cur.fetchall()
    for row in rows:
        # if more than 10 movies
        if row[1] > 10:
            print(row[0])
    con.commit()
    con.close()
    print("done")


# the 3.2 question
def q3_2():
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT DISTINCT a.name"
                " FROM All_Movie AS m JOIN All_Movie_Actor AS ma"
                " ON m.id=ma.movie"
                " JOIN All_Actor AS a"
                " ON a.id=ma.actor"
                " WHERE a.name LIKE 'Ja%'"
                " AND m.genre <> 'Comedy'")
    rows = cur.fetchall()
    for row in rows:
        print(str(row[0]))
    con.commit()
    con.close()
    print("done")


# the GAV of materialized view
def q4_materializedGAV():
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    # create the materialized All_Movie view
    cur.execute("CREATE VIEW All_Movie_materialized AS"
                " SELECT id,title,startyear,'Comedy' AS genre FROM ComedyMovie_materialized"
                " UNION ALL"
                " SELECT id,title,startyear,'Non_Comedy' AS genre FROM NonComedyMovie_materialized")
    # create the materialized All_Actor view
    cur.execute("CREATE VIEW All_Actor_materialized AS"
                " SELECT id,name,birthyear,deathyear FROM ComedyActor_materialized"
                " UNION ALL"
                " SELECT id,name,birthyear,deathyear FROM NonComedyActor_materialized")
    # create the materialized All_Movie_Actor view
    cur.execute("CREATE VIEW All_Movie_Actor_materialized AS"
                " SELECT actor,movie FROM ActedIn_materialized")
    con.commit()
    con.close()
    print("done")


# the resulting query for 3.1 using non-materialized view
def q4_1_nonMaterialized():
    start = time.time()
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT a.name,COUNT(*)"
                " FROM (SELECT id,title,startyear,'Comedy' AS genre"
                " FROM (SELECT id,title,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name='Comedy') AS ComedyMovieView"
                " UNION ALL"
                " SELECT id,title,startyear,'Non_Comedy' AS genre"
                " FROM (SELECT id,title,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name!='Comedy') AS NonComedyMovieView) AS m"
                " JOIN (SELECT actor,movie FROM (SELECT actor,movie FROM movie_actor) AS ActedInView) AS ma"
                " ON m.id=ma.movie"
                " JOIN (SELECT id,name,birthyear,deathyear"
                " FROM (SELECT id,member.name,birthYear,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name='Comedy') AS ComedyActorView"
                " UNION ALL"
                " SELECT id,name,birthyear,deathyear"
                " FROM (SELECT id,member.name,birthYear,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name!='Comedy') AS NonComedyActorView) AS a"
                " ON a.id=ma.actor"
                " WHERE a.deathyear IS NULL"
                " AND m.startyear<=2005"
                " AND m.startyear>=2000"
                " GROUP BY a.name")
    rows = cur.fetchall()
    for row in rows:
        # if more than 10 movies
        if row[1] > 10:
            print(row[0])
    con.commit()
    con.close()
    end = time.time()
    print("Non-materialized view of Q3.1 runs " + str(end - start) + " s")


# the resulting query for 3.1 using materialized view
def q4_1_Materialized():
    start = time.time()
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT a.name,COUNT(*)"
                " FROM All_Movie_materialized AS m JOIN All_Movie_Actor_materialized AS ma"
                " ON m.id=ma.movie"
                " JOIN All_Actor_materialized AS a"
                " ON a.id=ma.actor"
                " WHERE a.deathyear IS NULL"
                " AND m.startyear<=2005"
                " AND m.startyear>=2000"
                " GROUP BY a.name")
    rows = cur.fetchall()
    for row in rows:
        # if more than 10 movies
        if row[1] > 10:
            print(row[0])
    con.commit()
    con.close()
    end = time.time()
    print("Materialized view of Q3.1 runs " + str(end - start) + " s")


# the resulting query for 3.2 using non-materialized view
def q4_2_nonMaterialized():
    start = time.time()
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT DISTINCT a.name"
                " FROM (SELECT id,title,startyear,'Comedy' AS genre"
                " FROM (SELECT id,title,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name='Comedy') AS ComedyMovieView"
                " UNION ALL"
                " SELECT id,title,startyear,'Non_Comedy' AS genre"
                " FROM (SELECT id,title,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name!='Comedy') AS NonComedyMovieView) AS m"
                " JOIN (SELECT actor,movie FROM (SELECT actor,movie FROM movie_actor) AS ActedInView) AS ma"
                " ON m.id=ma.movie"
                " JOIN (SELECT id,name,birthyear,deathyear"
                " FROM (SELECT id,member.name,birthYear,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name='Comedy') AS ComedyActorView"
                " UNION ALL"
                " SELECT id,name,birthyear,deathyear"
                " FROM (SELECT id,member.name,birthYear,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name!='Comedy') AS NonComedyActorView) AS a"
                " ON a.id=ma.actor"
                " WHERE a.name LIKE 'Ja%'"
                " AND m.genre <> 'Comedy'")
    rows = cur.fetchall()
    for row in rows:
        print(str(row[0]))
    con.commit()
    con.close()
    end = time.time()
    print("Non-materialized view of Q3.2 runs " + str(end - start) + " s")


# the resulting query for 3.2 using materialized view
def q4_2_Materialized():
    start = time.time()
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT DISTINCT a.name"
                " FROM All_Movie_materialized AS m JOIN All_Movie_Actor_materialized AS ma"
                " ON m.id=ma.movie"
                " JOIN All_Actor_materialized AS a"
                " ON a.id=ma.actor"
                " WHERE a.name LIKE 'Ja%'"
                " AND m.genre <> 'Comedy'")
    rows = cur.fetchall()
    for row in rows:
        print(str(row[0]))
    con.commit()
    con.close()
    end = time.time()
    print("Materialized view of Q3.2 runs " + str(end - start) + " s")


# In the fifth question for materialized view,
# In the beginning, create the materialized view for sources
def optimized_materialized_view():
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    # create optimized materialized ComedyMovie view
    cur.execute("CREATE MATERIALIZED VIEW optimized_ComedyMovie_materialized AS"
                " SELECT id,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name='Comedy'")
    # create optimized materialized NonComedyMovie view
    cur.execute("CREATE MATERIALIZED VIEW optimized_NonComedyMovie_materialized AS"
                " SELECT id,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name!='Comedy'")
    # create optimized materialized ComedyActor view
    cur.execute("CREATE MATERIALIZED VIEW optimized_ComedyActor_materialized AS"
                " SELECT id,member.name,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name='Comedy'")
    # create optimized materialized NonComedyActor view
    cur.execute("CREATE MATERIALIZED VIEW optimized_NonComedyActor_materialized AS"
                " SELECT id,member.name,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name!='Comedy'")
    con.commit()
    con.close()
    print("done")


# Then create optimized GAV views for global schema
def optimized_materilizedGAV():
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    # create optimized materialized All_Movie view
    cur.execute("CREATE VIEW optimized_All_Movie_materialized AS"
                " SELECT id,startyear,'Comedy' AS genre FROM optimized_ComedyMovie_materialized"
                " UNION ALL"
                " SELECT id,startyear,'Non_Comedy' AS genre FROM optimized_NonComedyMovie_materialized")
    # create optimized materialized All_Actor view
    cur.execute("CREATE VIEW optimized_All_Actor_materialized AS"
                " SELECT id,name,deathyear FROM optimized_ComedyActor_materialized"
                " UNION ALL"
                " SELECT id,name,deathyear FROM optimized_NonComedyActor_materialized")
    con.commit()
    con.close()
    print("done")


# Optimized non-materialized view of Q3.1
def q5_1_nonMaterialized():
    start = time.time()
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT a.name,COUNT(*)"
                " FROM (SELECT id,startyear,'Comedy' AS genre"
                " FROM (SELECT id,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name='Comedy') AS ComedyMovieView"
                " UNION ALL"
                " SELECT id,startyear,'Non_Comedy' AS genre"
                " FROM (SELECT id,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name!='Comedy') AS NonComedyMovieView) AS m"
                " JOIN movie_actor AS ma"
                " ON m.id=ma.movie"
                " JOIN (SELECT id,name,deathyear"
                " FROM (SELECT id,member.name,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name='Comedy') AS ComedyActorView"
                " UNION ALL"
                " SELECT id,name,deathyear"
                " FROM (SELECT id,member.name,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name!='Comedy') AS NonComedyActorView) AS a"
                " ON a.id=ma.actor"
                " WHERE a.deathyear IS NULL"
                " AND m.startyear<=2005"
                " AND m.startyear>=2000"
                " GROUP BY a.name")
    rows = cur.fetchall()
    for row in rows:
        # if more than 10 movies
        if row[1] > 10:
            print(row[0])
    con.commit()
    con.close()
    end = time.time()
    print("Optimized non-materialized view of Q3.1 runs " + str(end - start) + " s")


# Optimized materialized view of Q3.1
def q5_1_Materialized():
    start = time.time()
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT a.name,COUNT(*)"
                " FROM optimized_All_Movie_materialized AS m JOIN movie_actor AS ma"
                " ON m.id=ma.movie"
                " JOIN optimized_All_Actor_materialized AS a"
                " ON a.id=ma.actor"
                " WHERE a.deathyear IS NULL"
                " AND m.startyear<=2005"
                " AND m.startyear>=2000"
                " GROUP BY a.name")
    rows = cur.fetchall()
    for row in rows:
        # if more than 10 movies
        if row[1] > 10:
            print(row[0])
    con.commit()
    con.close()
    end = time.time()
    print("Optimized materialized view of Q3.1 runs " + str(end - start) + " s")


# Optimized non-materialized view of Q3.2
def q5_2_nonMaterialized():
    start = time.time()
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT DISTINCT a.name"
                " FROM (SELECT id,startyear,'Comedy' AS genre"
                " FROM (SELECT id,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name='Comedy') AS ComedyMovieView"
                " UNION ALL"
                " SELECT id,startyear,'Non_Comedy' AS genre"
                " FROM (SELECT id,startYear"
                " FROM movie LEFT JOIN moviegenre_and_genre"
                " ON movie.id=moviegenre_and_genre.movie"
                " WHERE runtime>=75 AND name!='Comedy') AS NonComedyMovieView) AS m"
                " JOIN movie_actor AS ma"
                " ON m.id=ma.movie"
                " JOIN (SELECT id,name,deathyear"
                " FROM (SELECT id,member.name,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name='Comedy') AS ComedyActorView"
                " UNION ALL"
                " SELECT id,name,deathyear"
                " FROM (SELECT id,member.name,deathYear"
                " FROM member LEFT JOIN movieactor_and_moviegenre_and_genre"
                " ON member.id=movieactor_and_moviegenre_and_genre.actor"
                " WHERE movieactor_and_moviegenre_and_genre.name!='Comedy') AS NonComedyActorView) AS a"
                " ON a.id=ma.actor"
                " WHERE a.name LIKE 'Ja%'"
                " AND m.genre <> 'Comedy'")
    rows = cur.fetchall()
    for row in rows:
        print(str(row[0]))
    con.commit()
    con.close()
    end = time.time()
    print("Optimized non-materialized view of Q3.2 runs " + str(end - start) + " s")


# Optimized materialized view of Q3.2
def q5_2_Materialized():
    start = time.time()
    con = psycopg2.connect(database='555', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    cur.execute("SELECT DISTINCT a.name"
                " FROM optimized_All_Movie_materialized AS m JOIN movie_actor AS ma"
                " ON m.id=ma.movie"
                " JOIN optimized_All_Actor_materialized AS a"
                " ON a.id=ma.actor"
                " WHERE a.name LIKE 'Ja%'"
                " AND m.genre <> 'Comedy'")
    rows = cur.fetchall()
    for row in rows:
        print(str(row[0]))
    con.commit()
    con.close()
    end = time.time()
    print("Optimized materialized view of Q3.2 runs " + str(end - start) + " s")


def main():
    q1()
    q2()
    q3_1()
    q3_2()
    q4_materializedGAV()
    q4_1_nonMaterialized()
    q4_1_Materialized()
    q4_2_nonMaterialized()
    q4_2_Materialized()
    optimized_materialized_view()
    optimized_materilizedGAV()
    q5_1_nonMaterialized()
    q5_1_Materialized()
    q5_2_nonMaterialized()
    q5_2_Materialized()


if __name__ == '__main__':
    main()
