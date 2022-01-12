SELECT *
FROM(
	SELECT  genres.name as genre, productionCompanies.name, count(DISTINCT actors.id) as employees,
	RANK() OVER (PARTITION BY genres.name ORDER BY count(DISTINCT actors.id) DESC) AS ranking
	FROM productionCompanies, movie_company, movies, movie_actor, actors, genres, movie_genre
	WHERE productionCompanies.company_id = movie_company.movie_id
	AND movie_company.movie_id = movies.id
	AND movies.id = movie_actor.movie_id
	AND movie_actor.actor_id = actors.id
    AND movies.id = movie_genre.movie_id
    AND movie_genre.genre_id = genres.genre_id
	Group By productionCompanies.company_id, genres.genre_id
) sub_query
WHERE sub_query.ranking <=3
