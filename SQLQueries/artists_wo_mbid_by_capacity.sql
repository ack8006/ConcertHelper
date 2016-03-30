SELECT DISTINCT a.name, a.mbidid,v.capacity FROM artist a
JOIN event_artist ea ON ea.artist_id = a.id
JOIN event e ON e.id=ea.event_id
JOIN venue v ON v.id=e.venue_id
WHERE a.mbidid IS NULL AND v.capacity IS NOT NULL
ORDER BY v.capacity DESC
