SELECT a.name, e.onsale_date, e.event_date, v.city, sl.* FROM artist a
JOIN event_artist ea
ON ea.artist_id = a.id
JOIN event e
ON e.id=ea.event_id
JOIN venue v
ON v.id=e.venue_id
JOIN stubhub_listing sl
ON sl.event_id = e.id
ORDER BY e.event_date