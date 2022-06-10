select objects.id
from objects
    inner join object_labels on objects.id = object_labels.object_id
    inner join labels on object_labels.label_id = labels.id
where dataset_id = 'a5683322-7891-4548-ad36-96f564da125c'
    AND (key, value) in (
        VALUES ('key-1-3', 'value-1-3')
    )
GROUP BY objects.id
HAVING COUNT(objects.id) = 1
ORDER BY objects.id asc;