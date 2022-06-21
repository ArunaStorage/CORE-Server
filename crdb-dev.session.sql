SELECT "objects"."id",
    "objects"."created_at",
    "objects"."updated_at",
    "objects"."deleted_at",
    "objects"."object_uuid",
    "objects"."filename",
    "objects"."filetype",
    "objects"."content_len",
    "objects"."status",
    "objects"."default_location_id",
    "objects"."upload_id",
    "objects"."project_id",
    "objects"."dataset_id"
FROM "objects"
    inner join object_group_revision_data_objects on object_group_revision_data_objects.object_id = objects.id
WHERE object_group_revision_id = '897699bf-6e30-40e9-8434-03b1b73514bc'
    AND "objects"."deleted_at" IS NULL;