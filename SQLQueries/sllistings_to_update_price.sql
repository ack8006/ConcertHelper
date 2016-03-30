SELECT sl.stubhubid FROM event e
JOIN stubhub_listing sl ON e.id=sl.event_id
JOIN 
(SELECT MAX(update_time), sl_id FROM stubhub_point sp
GROUP BY sl_id
ORDER BY sl_id) as m
ON m.sl_id = sl.id
WHERE e.event_date >= now() AND
(e.onsale_date > (now() - interval '1 day')
OR now()-m.MAX > interval '4 hours')
