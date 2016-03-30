SELECT a.name, e.onsale_date, e.event_date, v.name, v.capacity, v.city, v.state, mr_pop.value
FROM artist a
JOIN event_artist ea 
	ON ea.artist_id=a.id
JOIN event e 
	ON e.id=ea.event_id
JOIN venue v 
	ON v.id=e.venue_id
JOIN popularity_point pp 
	ON pp.artist_id = a.id
JOIN (SELECT pp.id, pv.pt_id, pv.value, foo.max
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
		AND pp.update_date = foo.max) as mr_pop
	ON pp.id=mr_pop.id
WHERE mr_pop.pt_id=1 AND e.onsale_date > now() - interval '2 days' AND value > 45
ORDER BY e.onsale_date, mr_pop.value DESC;