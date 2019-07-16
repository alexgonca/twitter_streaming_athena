import configparser
import os
import googleapiclient.discovery  # https://developers.google.com/youtube/v3/docs/

# https://developers.google.com/youtube/v3/docs/videos#resource
# https://developers.google.com/youtube/v3/docs/channels#resource

# select distinct url_extract_parameter(validated_url, 'v') as video_id
# from validated_url
# where url_extract_host(validated_url) = 'www.youtube.com';

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))

youtube = googleapiclient.discovery.build(serviceName="youtube",
                                          version="v3",
                                          developerKey=config['youtube']['developer_key'])
video_id = '618xnPr7Kd8'
request = youtube.videos().list(
    part="snippet,contentDetails,statistics",
    id=video_id
)
response = request.execute()

print(response)

request = youtube.channels().list(
    part="snippet,statistics,contentDetails,contentDetails.relatedPlaylists",
    id="UCYZsxv_KrU0QjUsRYWcnbzQ"
)
response = request.execute()
