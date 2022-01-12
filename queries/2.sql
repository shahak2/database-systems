SELECT actors.full_name, Count(movie_filter.id) AS counter
FROM  (SELECT movies.id as id
	FROM movies, movie_company
	WHERE movies.id = movie_company.movie_id 
	AND (movies.release_date >= 20160101 and movies.release_date <= 20211227)
	GROUP BY movies.id
	HAVING Count(DISTINCT movie_company.company_id) > 3) AS movie_filter, actors, movie_actor
WHERE movie_filter.id = movie_actor.movie_id AND movie_actor.actor_id = actors.id
GROUP BY actors.id
ORDER BY counter DESC;
