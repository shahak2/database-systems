SELECT actors.full_name, actors.popularity
FROM actors
WHERE EXISTS (
	SELECT actors.id
	FROM movies, movie_actor, movie_genre, genres
	WHERE movies.id = movie_actor.movie_id AND 
    actors.id = movie_actor.actor_id AND 
    movies.id = movie_genre.movie_id AND
    movie_genre.genre_id = genres.genre_id AND
    genres.name = 'comedy' AND movies.language = 'fr'AND
    actors.gender  = 1 AND
    actors.birthday > 19710101 AND actors.birthday < 19810101)
ORDER BY actors.popularity
