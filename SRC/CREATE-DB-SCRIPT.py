import mysql.connector
from mysql.connector import errorcode


# ------------------------- FUNCTIONS ------------------------- #


def init_connection():
# This function is connecting to our server and returns the cnx and cursor
    cnx = mysql.connector.connect(
    host="localhost", 
    user="",
    password="",
    database = "",
    port = 3305
    )
    cursor = cnx.cursor()
    return cnx, cursor


def createDB(cnx, cursor):
# This function creates our database
    TABLES = {}
    print(">> Creating our DB!")
    TABLES['movies'] = (
                        "CREATE TABLE movies ("
                        " id INT NOT NULL PRIMARY KEY,"
                        " title varchar(100),"
                        " budget BIGINT,"
                        " revenue BIGINT,"
                        " language varchar(50),"
                        " release_date DATE)")

    TABLES['productionCompanies'] = (
                        "CREATE TABLE productionCompanies ("
                        " company_id INT NOT NULL PRIMARY KEY,"
                        " name varchar(100),"
                        " origin_country varchar(50))")

    TABLES['movie_company'] = (
                        "CREATE TABLE movie_company ("
                        " movie_id INT,"
                        " FOREIGN KEY (movie_id) REFERENCES movies(id),"
                        " company_id INT,"
                        " FOREIGN KEY (company_id) REFERENCES productionCompanies(company_id) )")

    TABLES['genres'] = (
                        "CREATE TABLE genres ("
                        " genre_id INT NOT NULL PRIMARY KEY,"
                        " name varchar(50))")

    TABLES['movie_genre'] = (
                        "CREATE TABLE movie_genre ("
                        " movie_id INT,"
                        " FOREIGN KEY (movie_id) REFERENCES movies(id),"
                        " genre_id INT,"
                        " FOREIGN KEY (genre_id) REFERENCES genres(genre_id) )")

    TABLES['actors'] = (
                        "CREATE TABLE actors ("
                        " id INT NOT NULL PRIMARY KEY,"
                        " full_name varchar(75),"
                        " popularity FLOAT,"
                        " gender INT,"
                        " birthday DATE)")
                        
    TABLES['movie_actor'] = (
                        "CREATE TABLE movie_actor ("
                        " actor_id INT,"
                        " FOREIGN KEY (actor_id) REFERENCES actors(id),"
                        " movie_id INT,"
                        " FOREIGN KEY (movie_id) REFERENCES movies(id) )")



    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print(">> Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
            cnx.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print(">> " + table_name + " already exists.")
            else:
                print(err.msg)
        else:
            print("OK")
    print(">> All tables created.")


def createIndexes(cursor, cnx):
# This function adds the indexes to the table.
    index_list = {}
    index_list['companies_index'] = (
        "CREATE INDEX companies_country ON "
        "productionCompanies(origin_country) USING HASH")
    index_list['movies_index'] = (
        "CREATE INDEX movies_releaseDate ON "
        "movies(release_date)")
    index_list['actors_index'] = (
        "CREATE FULLTEXT INDEX actors_fulltext_name ON "
        "actors(full_name)")

    for idx in index_list:
        idx_description = index_list[idx]
        try:
            print(">> Creating idx {}: ".format(idx), end='')
            cursor.execute(idx_description)
            cnx.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print(">> " + idx + " already exists.")
            else:
                print(err.msg)
        else:
            print("OK")


def deleteDB(cursor):
# This function deletes all tables from our database.
    print(">> Deleting all tables from the DB")
    
    tables = ['movies', 'actors', 'movie_actor', 'genres',
        'movie_genre', 'productionCompanies', 'movie_company']
    
    for table in tables:
        try:
            deleteCommand = "DROP TABLE " + table
            cursor.execute(deleteCommand)
        except mysql.connector.Error as err:
            print(">> " + table + " doesn't exists.")
    print(">> Deleting Complete")


def clearDB(cursor):
# This function clears all rows from our database.
    print(">> Clearing the DB")
    tables = ['movies', 'actors', 'movie_actor', 'genres',
        'movie_genre', 'productionCompanies', 'movie_company']

    for table in tables:
        try:
            deleteCommand = "TRUNCATE TABLE " + table
            cursor.execute(deleteCommand)
        except mysql.connector.Error as err:
            print(err.msg)
    print(">> Clearing Complete")


# ------------------------- Program Logic ------------------------- #

def main():
    cnx, cursor = init_connection()
    
    #clearDB(cursor)
    #deleteDB(cursor)
    createDB(cnx, cursor)
    createIndexes(cursor, cnx)

    cursor.close()
    cnx.close()

if __name__ == '__main__':
    main()