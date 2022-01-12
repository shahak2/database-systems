SELECT actors.full_name, Max(movies.release_date) AS latest_date
FROM movies, actors, movie_actor
WHERE movies.id = movie_actor.movie_id AND 
actors.id = movie_actor.actor_id AND 
actors.popularity < 0.7 AND 
actors.gender = 1
GROUP BY actors.id
HAVING Count(DISTINCT movies.id) <= 2
ORDER BY latest_date DESC
