// how to CTAS:
CREATE TABLE twitter_stream_temp
WITH (external_location = 's3://internetscholar-temp/twitter_stream/project=bolsonaro2019/creation_date=2019-07-08',
      format = 'ORC',
      bucketed_by = ARRAY['id_str'],
      bucket_count=1)
AS SELECT *
FROM twitter_stream
where project = 'bolsonaro2019' and creation_date = '2019-07-08';


CREATE EXTERNAL TABLE validated_url_raw (
url string,
validated_url string,
status_code int,
content_type string,
content_length int,
created_at string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar"=",", "escapeChar" = "\\")
LOCATION 's3://internetscholar-raw/validated_url/';

drop table validated_url;

CREATE TABLE validated_url
WITH (external_location = 's3://internetscholar/validated_url/',
      format = 'PARQUET',
      bucketed_by = ARRAY['url'],
      bucket_count=1) AS
select url, validated_url, status_code, content_type, content_length, cast(created_at as timestamp) as created_at
from (select t.*,
             row_number() over (partition by url order by created_at) as seqnum
      from validated_url_raw t
     ) t
where seqnum = 1;
