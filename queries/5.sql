SELECT productionCompanies.name, movies.title, non_us_based.earnings
FROM(
	SELECT productionCompanies.company_id, Max(movies.revenue - movies.budget) as earnings
	FROM productionCompanies, movies, movie_company
	WHERE movies.id = movie_company.movie_id 
		and movie_company.company_id = productionCompanies.company_id
		and productionCompanies.origin_country <> "US"
		and movies.revenue - movies.budget > 10000000
	GROUP BY productionCompanies.company_id
	HAVING count(DISTINCT movies.id) > 3 )as non_us_based, movies, movie_company, 	productionCompanies
WHERE productionCompanies.company_id = non_us_based.company_id and 
	movies.id = movie_company.movie_id and
    movie_company.company_id = productionCompanies.company_id and
    non_us_based.earnings = (movies.revenue - movies.budget)
ORDER BY earnings DESC
