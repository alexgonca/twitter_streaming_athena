// how to CTAS:
CREATE TABLE twitter_stream_temp
WITH (external_location = 's3://internetscholar-temp/twitter_stream/project=bolsonaro2019/creation_date=2019-07-08',
      format = 'ORC',
      bucketed_by = ARRAY['id_str'],
      bucket_count=1)
AS SELECT *
FROM twitter_stream
where project = 'bolsonaro2019' and creation_date = '2019-07-08';