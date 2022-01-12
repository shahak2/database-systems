SELECT actors.full_name, actors.popularity
FROM DbMysql28.actors
where match (full_name) against ('Zac*' IN BOOLEAN MODE)
ORDER BY popularity DESC
