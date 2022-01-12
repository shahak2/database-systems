import requests
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import time


# ------------------------- CONSTANTS ------------------------- #
api_key = ""
limit = 75

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


def print_progress(current, total):
  prec = current*100/total
  ranges = [(i - 0.05,i + 0.05) for i in range(5,100,5)]
  for r in ranges:
    if r[0] <= prec < r[1]:
      print("PROGRESS: " + str(int(current*100/total)) + "%")


def getMoviesIDs():
# This function gets all the movies IDs from the API and
# returns it as a set.
  print(">> Getting movies IDs")
  link = "https://api.themoviedb.org/3/movie/top_rated?api_key=" + \
     api_key + "&language=en-US&page={}"
  page = 1
  movies_ids = []
  while page < limit:
      response = requests.get(link.format(page))
      if not response.json()["results"]:
        break
      df = pd.DataFrame(response.json()["results"])[['id']]
      page += 1
      movies_ids += df['id'].tolist()
  return set(movies_ids)


def insertMovies(cnx, cursor, movies_ids):
# This function is getting cnx, cursor, movies_ids
# and inserts movies to movies table in our database.
# In addition, it returns two dictonaries and a set:
# D1. movie_company: key - movie_id, value - a set of its production companies
# D2. movie_genre: key - movie_id, value - a set of its genres
# S1. companies_ids
  print(">> Inserting movies to a table")
  add_movie = ("INSERT INTO movies "
                "(id, title, budget, revenue, language, release_date) "
                "values (%(id)s, %(title)s, %(budget)s, "
                " %(revenue)s, %(language)s, %(release_date)s)")

  link = "https://api.themoviedb.org/3/movie/{}?api_key=" +\
     api_key + "&language=en-US"

  movie_company = {}
  companies_ids = set()
  movie_genre = {}
  progress = 0

  for movie_id in movies_ids:  
    response = requests.get(link.format(movie_id))
    reponse_json = response.json()

    movie_keys = reponse_json.keys()
    if 'id' not in movie_keys or 'title' not in movie_keys or \
      'budget' not in movie_keys or 'revenue' not in movie_keys or \
      'original_language' not in movie_keys or 'release_date' not in movie_keys:
      continue

    row_dict = {
      'id':reponse_json['id'], 
      'title': reponse_json['title'],
      'budget': reponse_json['budget'],
      'revenue': reponse_json['revenue'],
      'language': reponse_json['original_language'],
      'release_date': reponse_json['release_date']
    }
    
    try:
      cursor.execute(add_movie, row_dict)
    except mysql.connector.Error as err:
      print(">> Skipping a faulty line")
      print(err.msg)
    
    # saving the production company for each movie
    companiesArray = reponse_json['production_companies']
    movie_company[movie_id] = set()
    for company in companiesArray:
      keys = company.keys()       
      if 'id' in keys:
        companies_ids.add(company['id'])
        movie_company[movie_id].add(company['id'])
      else:                       
        print(company)

    # saving the genre for each movie
    genresArray = reponse_json['genres']
    movie_genre[movie_id] = set()
    for genre in genresArray:
      keys = genre.keys()        
      if 'id' in keys:
        movie_genre[movie_id].add(genre['id'])
      else:                      
        print(genre)

    progress += 1
    print_progress(progress, len(movies_ids))
  
  try:
    cnx.commit()
    print(">> Movies inserted successfully")
  except:
    print(">> ERROR inserting data in movies table")
  
  return movie_company, movie_genre, companies_ids


def insertCompanies(cnx, cursor, companies_ids):
# This function is getting cnx, cursor, companies_ids
# and inserts companies to productionCompanies table in our database.
  print(">> Inserting production companies to a table")
  add_company = ("INSERT INTO productionCompanies "
                "(company_id, name, origin_country) "
                "values (%(company_id)s, %(name)s, %(origin_country)s)")

  link = "https://api.themoviedb.org/3/company/{}?api_key=" + api_key

  progress = 0
  for company_id in companies_ids:  
    response = requests.get(link.format(company_id))
    reponse_json = response.json()

    keys = reponse_json.keys()      
    if 'id' not in keys or 'name' not in keys or 'origin_country' \
        not in keys:
      continue
    
    row_dict = {
      'company_id':reponse_json['id'], 
      'name': reponse_json['name'],
      'origin_country': reponse_json['origin_country']
    }

    progress += 1
    print_progress(progress, len(companies_ids))

    try:
      cursor.execute(add_company, row_dict)
    except mysql.connector.Error as err:
      print(">> Skipping a faulty line")
      print(err.msg)

  try:
    cnx.commit()
    print(">> productionCompanies inserted successfully")
  except:
    print(">> ERROR inserting data in productionCompanies table")


def insertMovieCompany(cnx, cursor, movie_company):
# This function is getting cnx, cursor, a dictonary movie_company
# where key - movie_id, value - a set of its production companies
# and inserts movie_company relations into movie_company table.
  print(">> Inserting movie_company relations to a table")
  add_movie_company = ("INSERT INTO movie_company "
                "(movie_id, company_id) "
                "values (%(movie_id)s, %(company_id)s)")

  progress = 0
  for movie_id in movie_company.keys():
    for company_id in movie_company[movie_id]:
      row_dict = {
        'movie_id':movie_id, 
        'company_id': company_id
      }
      try:
        cursor.execute(add_movie_company, row_dict)
      except mysql.connector.Error as err:
        print(">> Skipping a faulty line")
        print(err.msg)
    progress += 1
    print_progress(progress, len(movie_company))
  try:
    cnx.commit()
    print(">> movie_company inserted successfully")
  except:
    print(">> ERROR inserting data in movie_company table")


def insertGenres(cnx, cursor):
# This function is getting cnx and cursor
# and inserts genres to genres table in our database.
  print(">> Inserting genres to genres a table")
  add_genre = ("INSERT INTO genres "
                "(genre_id, name) "
                "values (%(genre_id)s   , %(name)s)")

  link = "https://api.themoviedb.org/3/genre/movie/list?api_key=" + \
    api_key + "&language=en-US"
  
  response = requests.get(link)
  genresArray = response.json()['genres']

  for genre in genresArray:
    row_dict = {
      'genre_id':genre['id'], 
      'name': genre['name']
    }
    try:
      cursor.execute(add_genre, row_dict)
    except mysql.connector.Error as err:
      print(">> Skipping a faulty line")
      print(err.msg)
  try:
    cnx.commit()
    print(">> genres inserted successfully")
  except:
    print(">> ERROR inserting data in genres table")


def insertMovieGenre(cnx, cursor, movie_genre):
# This function is getting cnx, cursor, a dictonary movie_genre
# where key - movie_id, value - a set of its genres
# and inserts movie_genre relations into movie_genre table.
  print(">> Inserting movie_genre relations to a table")
  add_movie_genre = ("INSERT INTO movie_genre "
                "(movie_id, genre_id) "
                "values (%(movie_id)s, %(genre_id)s)")

  progress = 0
  for movie_id in movie_genre.keys():
    for genre_id in movie_genre[movie_id]:
      row_dict = {
        'movie_id':movie_id, 
        'genre_id': genre_id
      }
      try:
        cursor.execute(add_movie_genre, row_dict)
      except mysql.connector.Error as err:
        print(">> Skipping a faulty line")
        print(err.msg)
    progress += 1
    print_progress(progress, len(movie_genre))
  try:
    cnx.commit()
    print(">> movie_genre inserted successfully")
  except:
    print(">> ERROR inserting data in movie_genre table")


def getActors(movies_ids):
# This function is getting movies_ids and returns a dictonary 
# and a set:
# D1. movie_actors: key - movie_id, value - a set of its personnel
# S1. actors_ids

  print(">> Getting actors information...")

  link = "https://api.themoviedb.org/3/movie/{}/credits?api_key=" + \
    api_key + "&language=en-US"

  movie_actor = {}
  actors_ids = set()

  progress = 0
  for movie_id in movies_ids:  
    response = requests.get(link.format(movie_id))
    reponse_json = response.json()

    castArray = reponse_json['cast']
    movie_actor[movie_id] = set()

    for castMember in castArray:
      keys = castMember.keys()
      if 'id' in keys:
        actors_ids.add(castMember['id'])
        movie_actor[movie_id].add(castMember['id'])

    progress += 1
    print_progress(progress, len(movies_ids))

  print(">> Done fetching actors information")
  return movie_actor, actors_ids


def insertActors(cnx, cursor, actors_ids):
# This function is getting cnx, cursor, people_ids
# and inserts people to people table in our database.

  print(">> Inserting actors to a table")

  add_actors = ("INSERT INTO actors "
                "(id, full_name, popularity, gender, birthday) "
                "values (%(id)s, %(full_name)s, %(popularity)s, "
                " %(gender)s, %(birthday)s)")

  link = "https://api.themoviedb.org/3/person/{}?api_key=" + \
     api_key + "&language=en-US"

  progress = 0
  for actor_id in actors_ids:  
    response = requests.get(link.format(actor_id))
    reponse_json = response.json()
    row_dict = {
        'id': reponse_json['id'], 
        'full_name': reponse_json['name'],
        'popularity': reponse_json['popularity'],
        'gender': reponse_json['gender'],
        'birthday': reponse_json['birthday']
      }
    try:
      cursor.execute(add_actors, row_dict)
    except mysql.connector.Error as err:
      print(">> Skipping a faulty line")
      print(err.msg)

    progress += 1
    print_progress(progress, len(actors_ids))

  try:
    cnx.commit()
    print(">> actors inserted successfully")
  except:
    print(">> ERROR inserting data in actor table")
  

def insertMovieActors(cnx, cursor, movie_actors):
# This function is getting cnx, cursor, a dictonary movie_actors
# where key - movie_id, value - a set of its people
# and inserts movie_people relations into movie_people table.
  print(">> Inserting movie_actors relations to a table")
  add_movie_genre = ("INSERT INTO movie_actor "
                "(movie_id, actor_id) "
                "values (%(movie_id)s, %(actor_id)s)")

  progress = 0
  for movie_id in movie_actors.keys():
    for actor_id in movie_actors[movie_id]:
      row_dict = {
        'movie_id':movie_id, 
        'actor_id': actor_id
      }
      try:
        cursor.execute(add_movie_genre, row_dict)
      except mysql.connector.Error as err:
        print(">> Skipping a faulty line")
        print(err.msg)
    progress += 1
    print_progress(progress, len(movie_actors))

  try:
    cnx.commit()
    print(">> movie_actors inserted successfully")
  except:
    print(">> ERROR inserting data in movie_actors table")

def insertData(cnx, cursor):
# This function inserts our information into the DB
  
  '''
  x = pd.read_csv("../ourDB/movies_ids_test2.csv")
  movies_ids = x.iloc[:,1].tolist()
  '''
  movies_ids = getMoviesIDs()

  movie_company, movie_genre, companies_ids = \
    insertMovies(cnx, cursor, movies_ids)

  insertGenres(cnx, cursor) 
  insertMovieGenre(cnx, cursor, movie_genre)
  insertCompanies(cnx, cursor, companies_ids)
  insertMovieCompany(cnx, cursor, movie_company)

  movie_actor, actors_ids = \
    getActors(movies_ids)

  insertActors(cnx, cursor, actors_ids)
  insertMovieActors(cnx, cursor, movie_actor)
  
  

# ------------------------- Program Logic ------------------------- #


def main():
  cnx, cursor = init_connection()
  
  start = time.time()

  print(">> Inserting Data into our tables!")
  insertData(cnx, cursor)
  print(">> Our DB is ready!")

  end = time.time()
  print(">> Running time: ", end="")
  print((end - start)/3600, end = " hours")

  cursor.close()
  cnx.close()


if __name__ == '__main__':
    main()