import googleapiclient.discovery  # https://developers.google.com/youtube/v3/docs/
import logging
from internetscholar import InternetScholar
import csv
import os
import json

# https://developers.google.com/youtube/v3/docs/videos#resource
# https://developers.google.com/youtube/v3/docs/channels#resource


create_youtube_video_dummy = """
CREATE EXTERNAL TABLE IF NOT EXISTS youtube_video
(id string)
LOCATION 's3://{}/youtube_video'
"""

create_youtube_video_orc = """
create external table if not exists youtube_video
(
    kind string,
    etag string,
    id   string,
    snippet struct<
        publishedAt:  timestamp,
        title:        string,
        description:  string,
        channelId:    string,
        channelTitle: string,
        categoryId:   string,
        tags:         array<string>,
        liveBroadcastContent: string,
        defaultlanguage:      string,
        defaultAudioLanguage: string,
        localized:  struct <title: string, description: string>,
        thumbnails: struct<
            default:  struct <url: string, width: int, height: int>,
            medium:   struct <url: string, width: int, height: int>,
            high:     struct <url: string, width: int, height: int>,
            standard: struct <url: string, width: int, height: int>,
            maxres:   struct <url: string, width: int, height: int>
        >
    >
) 
STORED AS ORC
LOCATION 's3://{}/youtube_video/'
tblproperties ("orc.compress"="ZLIB");
"""

create_youtube_video_json = """
create external table if not exists youtube_video_raw
(
    kind string,
    etag string,
    id   string,
    snippet struct<
        publishedAt:  timestamp,
        title:        string,
        description:  string,
        channelId:    string,
        channelTitle: string,
        categoryId:   string,
        tags:         array<string>,
        liveBroadcastContent: string,
        defaultlanguage:      string,
        defaultAudioLanguage: string,
        localized:  struct <title: string, description: string>,
        thumbnails: struct<
            default:  struct <url: string, width: int, height: int>,
            medium:   struct <url: string, width: int, height: int>,
            high:     struct <url: string, width: int, height: int>,
            standard: struct <url: string, width: int, height: int>,
            maxres:   struct <url: string, width: int, height: int>
        >
    >
) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://{}/youtube_video/'
TBLPROPERTIES ('has_encrypted_data'='false')
"""

structure_youtube_video_orc = "".join("""\"
struct<
    kind: string,
    etag: string,
    id:   string,
    snippet: struct<
        publishedAt:  timestamp,
        title:        string,
        description:  string,
        channelId:    string,
        channelTitle: string,
        categoryId:   string,
        tags:         array<string>,
        liveBroadcastContent: string,
        defaultlanguage:      string,
        defaultAudioLanguage: string,
        localized:  struct <title: string, description: string>,
        thumbnails: struct<
            default:  struct <url: string, width: int, height: int>,
            medium:   struct <url: string, width: int, height: int>,
            high:     struct <url: string, width: int, height: int>,
            standard: struct <url: string, width: int, height: int>,
            maxres:   struct <url: string, width: int, height: int>
        >
    >
>
\"""".split())

select_youtube_videos = """
select distinct
  url_extract_parameter(validated_url, 'v') as video_id
from
  validated_url
where
  url_extract_host(validated_url) = 'www.youtube.com'
  and url_extract_parameter(validated_url, 'v') not in (select id from youtube_video)
"""


class YoutubeVideo(InternetScholar):
    def _init_(self, **kwargs):
        super(YoutubeVideo, self)._init_(prefix='youtube_video')
        self.youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                       version="v3",
                                                       developerKey=self.config['youtube']['developer_key'])

    def collect_video_info(self):
        # Create a dummy table in case a real one does not exist (to avoid that the next SELECT statement breaks)
        logging.info("Create dummy table in case there is not a youtube_video table on Athena")
        self.query_athena_and_wait(query_string=self.prepare(create_youtube_video_dummy, placeholder=self.s3_temp))

        logging.info("Download IDs for all Youtube videos that have not been processed yet")
        video_ids_csv = self.query_athena_and_download(query_string=self.prepare(select_youtube_videos),
                                                       filename='video_ids.csv')

        output_json = os.path.join(self.temp_dir, 'youtube_video.json')
        with open(video_ids_csv, newline='') as csv_reader:
            with open(output_json, 'w') as json_writer:
                reader = csv.DictReader(csv_reader)
                num_videos = 0
                for video_id in reader:
                    response = self.youtube.videos().list(part="snippet",id=video_id).execute()
                    for item in response.get('items', []):
                        num_videos = num_videos + 1
                        json_writer.write("{}\n".format(json.dumps(item)))

        self.upload_raw_file(local_filename=output_json,
                             s3_filename="youtube_video/{}.json.bz2".format(num_videos),
                             delete_original=True)


if __name__ == '__main__':
    main()