'''
@author-name: Rishab Katta

Python program for performing Itemset Mining on the IMDB database.
'''

import psycopg2
import time


class DatabaseConnection:

    def __init__(self,h,db,username,pwd):
        '''
        Constructor is used to connect to the database
        :param h: hostname
        :param db: database name
        :param username: Username
        :param pwd: password
        '''
        try:
            self.connection = psycopg2.connect(host=str(h), database=str(db), user=str(username), password=str(pwd))
            # self.connection = psycopg2.connect(host="localhost", database="imdb", user="user2", password="abcde")
            self.connection.autocommit=True
            self.cursor=self.connection.cursor()
        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            print(getattr(e, 'message', str(e)))

    def popular_movie_actor(self):
        '''
        Create and load popular_movie_actor table which is a subset of movie_actor table containing only type 'movie' and avgrating>5
        :return: None
        '''

        self.cursor.execute("CREATE TABLE Popular_Movie_Actors(actor INTEGER, movie INTEGER, FOREIGN KEY(movie) REFERENCES Movie(id), FOREIGN KEY(actor) REFERENCES Member(id))")

        self.cursor.execute("insert into Popular_Movie_Actors(actor, movie) "
                            "select actor, movie from movie_actor ma inner join movie m on ma.movie=m.id where m.type ilike 'movie' and m.avgrating > 5")
        print("Total number of rows inserted into PMA: " + str(self.cursor.rowcount))

    def l1(self):
        '''
        Create First Level of the Lattice with actor and the count of movies' they've appeared in
        :return: None
        '''

        self.cursor.execute("create table l1 as "
                            "select pma.actor as actor1, count(*) from popular_movie_actors pma group by actor having count(*) >= 5 ")

        print("Total number of rows inserted into l1: " + str(self.cursor.rowcount))

    def l2(self):
        '''
        Create Level 2 of the lattice where both actors have appeared together in movies
        :return: None
        '''

        #Commented because taking long time to run

        self.cursor.execute ("create table l2 as "
                            "select pma1.actor as actor1, pma2.actor as actor2, count(*) as count from "
                            "popular_movie_actors pma1 cross join popular_movie_actors pma2 inner join (select t1.actor1 as actor1 ,t2.actor1 as actor2 "
                            "from l1 t1 cross join l1 t2 where t1.actor1 < t2.actor1) t on t.actor1=pma1.actor and t.actor2=pma2.actor "
                            "where pma1.movie = pma2.movie and pma1.actor < pma2.actor group by pma1.actor, pma2.actor having count(*) >=5")

        # self.cursor.execute("create table l2 as "
        #                     "select pma1.actor as actor1, pma2.actor as actor2, count(*) as count from popular_movie_actors pma1 cross join popular_movie_actors pma2 "
        #                     "where pma1.movie = pma2.movie and pma1.actor < pma2.actor group by pma1.actor, pma2.actor having count(*) >=5")

        print("Total number of rows inserted into l2: " + str(self.cursor.rowcount))
    def l3(self):
        '''
        Create Level 3 of the lattice where three actors have appeared together in movies
        :return: None
        '''

        self.cursor.execute("create table l3 as "
                            "select pma1.actor as actor1, pma2.actor as actor2, pma3.actor as actor3, count(*) as count from popular_movie_actors pma1 "
                            "cross join popular_movie_actors pma2 cross join popular_movie_actors pma3 inner join (select actor1, actor2, t3.actor3 from l2 cross join (select actor1 as actor3 from l2 union "
                            "select actor2 as actor3 from l2) t3 where actor1<actor2 and actor2<actor3 and actor1<actor3) t on t.actor1=pma1.actor and t.actor2=pma2.actor and t.actor3=pma3.actor "
                            "where pma1.movie = pma2.movie  and pma3.movie = pma2.movie and pma1.actor < pma2.actor and pma2.actor < pma3.actor and pma1.actor<pma3.actor "
                            "group by pma1.actor, pma2.actor, pma3.actor having count(*) >=5")

        print("Total number of rows inserted into l3: " + str(self.cursor.rowcount))

    def generalize(self):
        '''
        Generalized code for generating all levels of lattice
        :return: None
        '''
        print(" ")
        print("Executing Generalized code...")

        self.cursor.execute("create table l1 as "
                            "select pma.actor as actor1, count(*) from popular_movie_actors pma group by pma.actor having count(*) >= 5 ")

        rows =self.cursor.rowcount
        k=2


        while rows!=0:

            query = "create table l" + str(k) + " as " \
                    "select " + self.q_p1(k) + " count(*) as count from " + self.q_p2(k) + " where " + self.q_p3(k)+ self.q_p4(k) + " group by " + self.q_p5(k) + " having count(*) >= 5"

            self.cursor.execute(query)
            rows = self.cursor.rowcount
            print("Total number of rows inserted into l" + str(k) +": "+ str(self.cursor.rowcount))
            k +=1

        self.cursor.execute("select actor1, m1.name, actor2, m2.name, actor3, m3.name, actor4, m4.name, actor5, m5.name, actor6, m6.name "
                            "from l6 left join member m1 on actor1=m1.id join member m2 on actor2=m2.id join member m3 on actor3=m3.id "
                            "join member m4 on actor4=m4.id join member m5 on actor5=m5.id join member m6 on actor6=m6.id")

        rows = self.cursor.fetchall()

        print("Final Level with non-empty rows is L6. The Names of actors in that level are")
        for row in rows:
            print(row)


    def q_p1(self,k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''
        q_string1=""

        for i in range(1,k+1):
            q_string1 += "pma"+str(i)+".actor as actor"+str(i) +", "

        return q_string1

    def q_p2(self, k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''

        q_string2=""

        for i in range(1,k+1):
            if i==k:
                q_string2 += "popular_movie_actors pma" + str(i)
            else:
                q_string2 += "popular_movie_actors pma" + str(i) + " cross join "
        return q_string2

    def q_p3(self, k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''
        q_string3=""

        for i in range(1,k):
            q_string3 += "pma" +str(i) +".movie = pma"+str(i+1) +".movie and "

        return q_string3

    def q_p4(self, k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''
        q_string4=""

        for i in range(1,k):
            for j in range (i+1,k+1):
                q_string4 += "pma" + str(i) + ".actor < pma" + str(j) + ".actor and "
        q_string4=q_string4.rstrip("and ")

        return q_string4

    def q_p5(self, k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''

        q_string5=""
        for i in range(1,k+1):
            if i==k:
                q_string5 += "pma" + str(i) + ".actor "
            else:
                q_string5 += "pma" + str(i) + ".actor, "
        return q_string5

    def drop_tables_with_pma(self):
        '''
        Drop all tables if they exist including popular_movie_actors
        :return: None
        '''
        self.cursor.execute("DROP TABLE IF EXISTS Popular_Movie_Actors, l1, l2, l3,l4,l5,l6,l7 CASCADE")
    def drop_tables_wo_pma(self):
        '''
        Drop all tables if they exist excluding popular_movie_actors
        :return: None
        '''
        self.cursor.execute("DROP TABLE IF EXISTS l1, l2, l3,l4,l5,l6,l7 CASCADE")


if __name__ == '__main__':
    h = str(input("Enter host name"))
    db = str(input("Enter Database Name"))
    username = str(input("Enter username"))
    pwd = str(input("Enter password"))
    db_con =DatabaseConnection(h,db,username,pwd)

    db_con.drop_tables_with_pma()

    start_time = time.time()
    db_con.popular_movie_actor()
    print("--- %s seconds for pma ---" % (time.time() - start_time))
    db_con.drop_tables_wo_pma()

    start_time = time.time()
    db_con.l1()
    print("--- %s seconds for l1 ---" % (time.time() - start_time))

    start_time = time.time()
    db_con.l2()
    print("--- %s seconds for l2 ---" % (time.time() - start_time))

    start_time = time.time()
    db_con.l3()
    print("--- %s seconds for l3 ---" % (time.time() - start_time))

    db_con.drop_tables_wo_pma()

    start_time = time.time()
    db_con.generalize()
    print("--- %s seconds for generalized code ---" % (time.time() - start_time))



