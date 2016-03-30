SELECT foo.update_time, foo.min_price, foo.name FROM 
(SELECT sp.id, sp.update_time, sz.name, spr.* FROM stubhub_listing sl
JOIN stubhub_point sp 
ON sp.sl_id=sl.id
JOIN stubhub_price spr
ON spr.sp_id=sp.id
JOIN stubhub_zone sz
ON sz.id=spr.sz_id
WHERE sl.id=96) as foo
ORDER BY foo.name, foo.update_time