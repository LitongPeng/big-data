import re
from itertools import combinations

import psycopg2


def q1():
    # connect to the postgresql
    con = psycopg2.connect(database='imdb', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    #create Popular_Movie_Actors table
    cur.execute("create table Popular_Movie_Actors ("
                "actor int,"
                "movie int,"
                "primary key(movie,actor));")
    #insert the data based on the condition
    cur.execute("insert into Popular_Movie_Actors"
                " select ma.actor,ma.movie"
                " from movie_actor as ma inner join movie as m"
                " on ma.movie=m.id"
                " where m.type like '%movie%'"
                " and m.avgRating>5;")
    con.commit()
    con.close()
    print("done")


def q2():
    # connect to the postgresql
    con = psycopg2.connect(database='imdb', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    #create table to store the data without minimum support
    cur.execute("create table L1_1 ("
                "actor1_1 int,"
                "count_1 int);")
    cur.execute("insert into L1_1"
                " select actor,count(*)"
                " from Popular_Movie_Actors"
                " group by actor;")
    # create table to store the data with minimum support
    cur.execute("create table L1 ("
                "actor1 int,"
                "count int);")
    cur.execute("insert into L1"
                " select actor1_1,count_1 from L1_1"
                " where count_1>=5;")
    con.commit()
    con.close()
    print("done")

def l2():
    # connect to the postgresql
    con = psycopg2.connect(database='imdb', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    #create the table for level 2
    cur.execute("create table L2 ("
                "actor1 int,"
                "actor2 int,"
                "count int,"
                "primary key(actor1,actor2));")
    con.commit()
    con.close()
    print("done")

def q3():
    # connect to the postgresql
    con = psycopg2.connect(database='imdb', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    #build a new list to store the name of popular actor from l1
    my_list = []
    cur.execute("select actor1 from l1;")
    docs = cur.fetchall()
    for doc in docs:
        #only keep the number from result
        actor = re.findall('[0-9]+', str(doc))
        my_list.append(actor)
    #how many popular actor
    num = len(my_list)
    #apriori algorithm
    #the number of all kinds of combination for two actor
    row = int(0.5 * (num * (num - 1)))
    #biuld an array to srore these combination
    #like:
    # ___________________________________
    # |   actor1       |     actor2     |
    # ___________________________________
    # |   actor1       |     actor2     |
    # ___________________________________
    # |   actor1       |     actor2     |
    # ___________________________________
    # |   actor1       |     actor2     |
    # ___________________________________
    # |   .            |     .          |
    # |   .            |     .          |
    # |   .            |     .          |
    arr3 = [([0] * 2) for m in range(row)]
    a = 0 #row
    #actor1 from 0 to k-1
    for i in my_list[0:(len(my_list) - 1)]:
        #actor2 from the one behind actor1 to the last one
        for j in my_list[(num - my_list.index(i) - 1):]:
            arr3[a][0] = i
            arr3[a][1] = j
            a += 1
    #go through all combinations
    for i in range(len(arr3)):
        #transfer it to string and only keep the number
        actor1 =  str(arr3[i][0]).replace('[','').replace(']','')
        #serach the movies this actor played
        cur.execute("select movie from Popular_Movie_Actors where actor=" + actor1+";")
        movies = cur.fetchall()
        times = 0
        #for every movie
        for movie in movies:
            #transfer it to string and only keep the number
            mov = str(movie).replace('(','').replace(')','').replace(',','')
            #search for which actor played this movie
            cur.execute("select actor from Popular_Movie_Actors where movie=" + mov+";" )
            # all actors have played this movie
            actors2 = str(cur.fetchall())
            #transfer it to string and only keep the number
            actor2 = str(arr3[i][1]).replace('[','').replace(']','')
            # if the actor played this movie
            if actor2 in actors2:
                #count plus one
                times += 1
        #if this combination meets the minimum support
        if times >= 5:
            #insert into the l2 table
            cur.execute("insert into l2 (actor1,actor2,count) values (" + actor1 + "," + actor2 + "," + str(times) + ");")


        #####################################################

    con.commit()
    con.close()

def l3():
    # connect to the postgresql
    con = psycopg2.connect(database='imdb', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    #create the table for level 3
    cur.execute("create table L3 ("
                "actor1 int,"
                "actor2 int,"
                "actor3 int,"
                "count int,"
                "primary key(actor1,actor2,actor3));")
    con.commit()
    con.close()
    print("done")

def q4():
    # connect to the postgresql
    con = psycopg2.connect(database='imdb', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    # build a new list to store the name of popular actor from l2
    my_list = []
    cur.execute("select distinct actor1 from l2;")
    docs = cur.fetchall()
    for doc in docs:
        # only keep the number from result
        actor = re.findall('[0-9]+', str(doc))
        my_list.append(actor)
    # how many popular actor
    num = len(my_list)
    # apriori algorithm
    # the number of all kinds of combination for three actor
    row = int( (num * (num - 1)* (num - 2))/6)
    # biuld an array to srore these combination
    # like:
    # ____________________________________________________
    # |   actor1       |     actor2     |     actor3     |
    # ____________________________________________________
    # |   actor1       |     actor2     |     actor3     |
    # ____________________________________________________
    # |   actor1       |     actor2     |     actor3     |
    # ____________________________________________________
    # |   actor1       |     actor2     |     actor3     |
    # ____________________________________________________
    # |   .            |     .          |     .          |
    # |   .            |     .          |     .          |
    # |   .            |     .          |     .          |
    arr4 = [([0] * 3) for m in range(row)]
    a = 0  # row
    # actor1 from 0 to k-1
    for i in my_list[0:(len(my_list) - 1)]:
        # actor2 from the one behind actor1 to the last one
        for j in my_list[(num - my_list.index(i) - 1):]:
            # actor3 from the one behind actor2 to the last one
            for k in my_list[(num - my_list.index(j) - 1):]:
                arr4[a][0] = i
                arr4[a][1] = j
                arr4[a][2] = k
                a += 1
    # go through all combinations
    for i in range(len(arr4)):
        # transfer it to string and only keep the number
        actor1 = str(arr4[i][0]).replace('[', '').replace(']', '')
        # serach the movies this actor played
        cur.execute("select movie from Popular_Movie_Actors where actor=" + actor1 + ";")
        movies = cur.fetchall()
        times = 0
        # for every movie
        for movie in movies:
            # transfer it to string and only keep the number
            mov = str(movie).replace('(', '').replace(')', '').replace(',', '')
            # search for which actor played this movie
            cur.execute("select actor from Popular_Movie_Actors where movie=" + mov + ";")
            # all actors have played this movie
            actors2 = str(cur.fetchall())
            # transfer it to string and only keep the number
            actor2 = str(arr4[i][1]).replace('[', '').replace(']', '')
            actor3 = str(arr4[i][2]).replace('[', '').replace(']', '')
            # if these two actor both played this movie
            if (actor2 in actors2) and (actor3 in actors2):
                # count plus one
                times += 1
        # if this combination meets the minimum support
        if times >= 5:
            # insert into the l3 table
            cur.execute(
                "insert into l3 (actor1,actor2,actor3,count) values (" + actor1 + "," + actor2 + "," + actor3+ "," + str(times) + ");")
    con.commit()
    con.close()
    print("done")

#return l: the final level of the empty table
def q5():
    # connect to the postgresql
    con = psycopg2.connect(database='imdb', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    #build table for lever1
    cur.execute("create table q5_l1_1 ("
                "actor1_1 int,"
                "count_1 int);")
    cur.execute("insert into q5_l1_1"
                " select actor,count(*)"
                " from Popular_Movie_Actors"
                " group by actor;")
    cur.execute("create table q5_l1 ("
                "actor1 int,"
                "count int);")
    cur.execute("insert into q5_l1"
                " select actor1_1,count_1 from q5_l1_1"
                " where count_1>=5;")
    # build a new list to store the name of popular actor from l1
    my_list = []
    cur.execute("select actor1 from q5_l1;")
    docs = cur.fetchall()
    for doc in docs:
        # only keep the number from result
        actor = re.findall('[0-9]+', str(doc))
        my_list.append(actor)
    #check for all levels one by one from level2
    l=2
    while True:
        #build the table for this level
        actors="actor1 int,"
        m=2
        while m<l+1: #till the level number
            #build actors column like previous
            actors=actors+"actor"+str(m)+" int,"
            m+=1
        #create table
        cur.execute("create table L5_"+str(l)+" ("+actors+"count int);")
        #all combinations odf actors for this level
        arr=list(combinations(my_list,l))
        # go through all combinations
        for i in range(len(arr)):
            # transfer it to string and only keep the number
            actor1 = str(arr[i][0]).replace('[', '').replace(']', '')
            # serach the movies this actor played
            cur.execute("select movie from Popular_Movie_Actors where actor=" + actor1 + ";")
            movies = cur.fetchall()
            times = 0
            # for every movie
            for movie in movies:
                # transfer it to string and only keep the number
                mov = str(movie).replace('(', '').replace(')', '').replace(',', '')
                # search for which actor played this movie
                cur.execute("select actor from Popular_Movie_Actors where movie=" + mov + ";")
                #all actors have played this movie
                actors2 = str(cur.fetchall())
                # transfer it to string and only keep the number
                all_actors=arr[i]
                # if all actors in the combination played this movie
                if ifff(all_actors,actors2):
                    #count plus one
                    times+=1
             # if this combination meets the minimum support
            if times >= 5:
                # insert into the table in this level
                cur.execute(
                    "insert into l5_"+str(l)+" values (" + str(all_actors).replace('[', '').replace(']', '') + str(times) + ");")
        #check if this table is empty or not
        cur.execute("select * from l5_"+str(l)+";")
        result=cur.fetchall()
        #if this level is empty
        if result==[]:
            #return the level number and break
            return l
            break
            #else: go through the next level
        else:
            l+=1
    con.commit()
    con.close()
    print("done")

#report the fifth question
def report5():
    # connect to the postgresql
    con = psycopg2.connect(database='imdb', user='postgres', password='12345678',
                           host='127.0.0.1', port='5432')
    print("Opened database successfully")
    cur = con.cursor()
    #print the result except the last level table
    for i in range(1,q5()-2):
         cur.execute("select * from l5_"+str(i))
         print(cur.fetchall())
    #add a colomn "name":store the name of these actors
    cur.execute("alter table l5_"+str(q5()-1)+" add column name int;")
    #add a column "id" to mark the row
    cur.execute("alter table l5_" + str(q5() - 1) + " add column id serial;")
    actors=cur.execute("select * from l5_"+str(q5()-1))
    j=1 #mark rows
    #all actors on arow
    for actor in actors:
        new=""
        #each actor without the count
        for act in actor[0:len(actor)]:
            #find the name for all actors from the member table
            cur.execute("select * from member where id="+act+";")
            #all names
            new=new+str(cur.fetchall()).replace('(', '').replace(')', '')
        #insert the name to the name column based on the id
        cur.execute("insert into l5_"+str(q5()-1)+"(name) values ("+new+") where id="+str(j)+";")
        j+=1
    #print the last table
    cur.execute("select * from l5_" + str(q5() - 1))
    con.commit()
    con.close()
    print("done")


#chech if all actors in the combination played this movie
def ifff(all_actors,actors2):
    for all_actor in all_actors:
        if str(all_actor).replace('[', '').replace(']', '') in actors2:
            pass
        else:
            return False
    return True

def main():
    q1()
    q2()
    q3()
    q4()
    q5()
    report5()

if __name__ == '__main__':
    main()
