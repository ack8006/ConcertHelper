SELECT pp.id, pv.pt_id, pv.value, foo.max
FROM popularity_point pp
JOIN popularity_value pv ON pv.pp_id=pp.id
JOIN
(SELECT pp.artist_id, pv.pt_id, MAX(pp.update_date)
FROM popularity_point pp
JOIN popularity_value pv ON pv.pp_id = pp.id
GROUP BY pp.artist_id, pv.pt_id
ORDER BY pp.artist_id) as foo
ON foo.artist_id=pp.artist_id 
	AND foo.pt_id=pv.pt_id
	AND pp.update_date = foo.max
