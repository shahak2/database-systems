import requests
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import os

# ------------------------- FUNCTIONS ------------------------- #


def init_connection():
# This function is connecting to our server and returns the cursor
  cnx = mysql.connector.connect(
    host="localhost", 
    user="",
    password="",
    database = "",
    port = 3305
  )
  cursor = cnx.cursor()
  return cnx, cursor


def getQuery(queryNumber):
    script_dir = os.path.dirname(__file__)
    rel_path = "../queries/" + str(queryNumber) + ".sql"
    path = os.path.join(script_dir, rel_path)
    fd = open(path, 'r')
    query = fd.read()
    fd.close()
    return query


def query1(cursor):
    query = getQuery(1)

    full_names = []
    popularities = []
    try:
        cursor.execute(query)
        for (full_name, popularity) in cursor:
            full_names.append(full_name)
            popularities.append(popularity)
        results = pd.DataFrame({'full_name': full_names, 
            'popularity': popularities})
        return results
    except mysql.connector.Error as err:
        print(">> ERROR: Query failed")
        print(err.msg)


def query2(cursor):
    query = getQuery(2)

    full_names = []
    counters = []
    try:
        cursor.execute(query)
        for (full_name, counter) in cursor:
            full_names.append(full_name)
            counters.append(counter)
        results = pd.DataFrame({'full_name': full_names, 'counter': counters})
        return results
    except mysql.connector.Error as err:
        print(">> ERROR: Query failed")
        print(err.msg)


def query3(cursor):
    query = getQuery(3)

    full_names = []
    popularities = []
    try:
        cursor.execute(query)
        for (full_name, popularity) in cursor:
            full_names.append(full_name)
            popularities.append(popularity)
        results = pd.DataFrame({'full_name': full_names, 'popularity': popularities})
        return results
    except mysql.connector.Error as err:
        print(">> ERROR: Query failed")
        print(err.msg)


def query4(cursor):
    query = getQuery(4)

    full_names = []
    dates = []
    try:
        cursor.execute(query)
        for (full_name, popularity) in cursor:
            full_names.append(full_name)
            dates.append(popularity)
        results = pd.DataFrame({'full_name': full_names, 'latest_date': dates})
        return results
    except mysql.connector.Error as err:
        print(">> ERROR: Query failed")
        print(err.msg)


def query5(cursor):
    query = getQuery(5)
    companies_names = []
    movies_title = []
    earningsList = []
    try:
        cursor.execute(query)
        for (company_name, movie_title, earnings) in cursor:
            companies_names.append(company_name)
            movies_title.append(movie_title)
            earningsList.append(earnings)
        results = pd.DataFrame({'company_name': companies_names, 
            'movie_title': movies_title,
            'earnings': earningsList
            })
        return results
    except mysql.connector.Error as err:
        print(">> ERROR: Query failed")
        print(err.msg)


def query6(cursor):
    query = getQuery(6)
    genres = []
    companies_names = []
    employees = []
    rankings = []
    try:
        cursor.execute(query)
        for (genre, company_name, employee, ranking) in cursor:
            genres.append(genre)
            companies_names.append(company_name)
            employees.append(employee)
            rankings.append(ranking)
        results = pd.DataFrame({'genre': genres, 
            'company_name': companies_names,
            'employee': employees,
            'ranking': rankings,
            })
        return results
    except mysql.connector.Error as err:
        print(">> ERROR: Query failed")
        print(err.msg)


def query7(cursor):
    query = getQuery(7)

    full_names = []
    connections = []
    try:
        cursor.execute(query)
        for (full_name, connection) in cursor:
            full_names.append(full_name)
            connections.append(connection)
        results = pd.DataFrame({'full_name': full_names, 'connections': connections})
        return results
    except mysql.connector.Error as err:
        print(">> ERROR: Query failed")
        print(err.msg)



# ------------------------- Program Logic ------------------------- #


def main():
# This function asks for input of the query number and prints its result
    cnx, cursor = init_connection()

    print(">> Select a query from 1 to 7:")
    queryNumber = int(input())
    assert 1 <= queryNumber <= 7, '>> ERROR: illegal query number'
  
    if queryNumber == 1:
        print(query1(cursor))
    if queryNumber == 2:
        print(query2(cursor))
    if queryNumber == 3:
        print(query3(cursor))
    if queryNumber == 4:
        print(query4(cursor))
    if queryNumber == 5:
        print(query5(cursor))
    if queryNumber == 6:
        print(query6(cursor))
    if queryNumber == 7:
        print(query7(cursor))

    print(">> Data retrieved!")
    cursor.close()
    cnx.close()


if __name__ == '__main__':
    main()