import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from youtube_transcript_api import YouTubeTranscriptApi
from postgres_utils import PGConn

def get_youtube_transcript(video_id):
    ytt_api = YouTubeTranscriptApi()
    fetched_transcript = ytt_api.fetch(video_id)
    
    transcript_text = " ".join(snippet['text'] for snippet in fetched_transcript)
    return transcript_text

def get_playlist_videos(playlist_id, api_key, max_results=None):
    """
    Retrieves all videos from a YouTube playlist.
    """
    # Initialize the YouTube API client
    youtube = googleapiclient.discovery.build(
        "youtube", "v3", developerKey=api_key
    )
    
    videos = []
    next_page_token = None
    total_retrieved = 0
    
    # Continue fetching pages until there are no more results or we've hit max_results
    while True:
        # Make the API request for playlist items
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,  # Maximum allowed per request
            pageToken=next_page_token
        )
        response = request.execute()
        
        # Process each video in the current page
        for item in response["items"]:
            video_data = {
                "id": item["contentDetails"]["videoId"],
                "title": item["snippet"]["title"]
            }
            videos.append(video_data)
            total_retrieved += 1
            
            # Stop if we've reached the max_results limit
            if max_results and total_retrieved >= max_results:
                return videos
        
        # Check if there are more pages
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    
    return videos

if __name__ == "__main__":
    API_KEY = "AIzaSyB5x0F4nohY7KIH-2JKiaFKnNyMRMrzIVQ"
    PLAYLIST_ID = "PLE25ZovyAdMdOFj-eclPLvdeUvgkQZqhQ"
    quarter = "Q3"
    financial_year = "FY20"
    pgconn = PGConn({
                "database": "datadump",
                "host": "localhost",
                "port": "5432",
                "user": "sparsh",
                "password": "algobulls"
            })
    try:
        videos = get_playlist_videos(PLAYLIST_ID, API_KEY)
        print(f"Found {len(videos)} videos in playlist:")
        values_list = []
        for i, video in enumerate(videos, 1):
            print(f"{i}. Title: {video['title']}")
            print(f"   ID: {video['id']}")
            print("---")
            safe_title = video["title"].replace("'", "''")
            values_list.append(f"('{quarter}', '{financial_year}', '{PLAYLIST_ID}', '{video['id']}', '{safe_title}')")

        if len(videos) > 0:
            value_str = ", ".join(values_list)
            sql_query = f"""
                insert into trendlyne.conference_calls (quarter, financial_year, playlist_id, video_id, video_title) 
                values {value_str};
            """
            pgconn.execute(sql_query)
            
    except googleapiclient.errors.HttpError as e:
        print(f"An API error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")