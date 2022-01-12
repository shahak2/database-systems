SELECT p1.name, Count(p2.company_id) as connections
FROM productionCompanies as p1, productionCompanies as p2,
	movie_company as mp1, movie_company as mp2
WHERE mp1.company_id = p1.company_id AND
    mp2.company_id = p2.company_id AND 
    p1.company_id != p2.company_id AND
    mp1.movie_id = mp2.movie_id
Group by p1.company_id
Order by Count(p2.company_id) DESC