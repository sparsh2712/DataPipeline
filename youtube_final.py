import googleapiclient.discovery
import googleapiclient.errors
from youtube_transcript_api import YouTubeTranscriptApi
import requests
from bs4 import BeautifulSoup

class YoutubeTool:
    """A tool to interact with YouTube data."""
    
    def __init__(self, serp_api_key=None, youtube_api_key = None):
        """Initialize with YouTube API key."""
        self.serp_api_key = serp_api_key
        self.youtube_api_key = youtube_api_key
        self._init_youtube_client()
        self.transcriptor = YouTubeTranscriptApi()
        
    def _init_youtube_client(self):
        """Initialize the YouTube API client."""
        if self.youtube_api_key:
            self.youtube = googleapiclient.discovery.build(
                "youtube", "v3", developerKey=self.youtube_api_key
            )
        else:
            self.youtube = None
            
    def get_transcript(self, video_id):
        """Get the transcript text for a YouTube video."""
        try:
            transcript = self.transcriptor.fetch(video_id)
            return " ".join(snippet.text for snippet in transcript)
        except Exception as e:
            print(f"Error fetching transcript: {e}")
            return None
    
    def get_playlists_from_channel(self, channel_id, max_results=50):
        """Get all playlists from a YouTube channel."""
        if not self.youtube:
            raise ValueError("API key not set. Use set_api_key() first.")
            
        playlists = []
        next_page_token = None
        
        try:
            while True:
                request = self.youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
                    maxResults=min(50, max_results),
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response["items"]:
                    playlist_data = {
                        "id": item["id"],
                        "title": item["snippet"]["title"],
                        "description": item["snippet"]["description"],
                        "video_count": item["contentDetails"]["itemCount"]
                    }
                    playlists.append(playlist_data)
                    
                    if len(playlists) >= max_results:
                        return playlists
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                    
            return playlists
        except googleapiclient.errors.HttpError as e:
            print(f"API error: {e}")
            return []
    
    def get_channel_id_by_name(self, channel_name):
        """Get channel ID from a channel name."""
        if not self.youtube:
            raise ValueError("API key not set. Use set_api_key() first.")
            
        try:
            request = self.youtube.search().list(
                part="snippet",
                q=channel_name,
                type="channel",
                maxResults=1
            )
            response = request.execute()
            
            if response["items"]:
                return response["items"][0]["id"]["channelId"]
            return None
        except googleapiclient.errors.HttpError as e:
            print(f"API error: {e}")
            return None
    
    def get_videos_from_playlist(self, playlist_id, max_results=50):
        """Get all videos from a YouTube playlist."""
        if not self.youtube:
            raise ValueError("API key not set. Use set_api_key() first.")
            
        videos = []
        next_page_token = None
        
        try:
            while True:
                request = self.youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=min(50, max_results - len(videos)),
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response["items"]:
                    video_data = {
                        "id": item["contentDetails"]["videoId"],
                        "title": item["snippet"]["title"],
                        "description": item["snippet"]["description"],
                        "published_at": item["snippet"]["publishedAt"]
                    }
                    videos.append(video_data)
                    
                    if len(videos) >= max_results:
                        return videos
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                    
            return videos
        except googleapiclient.errors.HttpError as e:
            print(f"API error: {e}")
            return []
    
    def get_video_details(self, video_id):
        """Get detailed information about a specific video."""
        if not self.youtube:
            raise ValueError("API key not set. Use set_api_key() first.")
            
        try:
            request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response = request.execute()
            
            if response["items"]:
                video = response["items"][0]
                details = {
                    "title": video["snippet"]["title"],
                    "description": video["snippet"]["description"],
                    "published_at": video["snippet"]["publishedAt"],
                    "channel_id": video["snippet"]["channelId"],
                    "channel_title": video["snippet"]["channelTitle"],
                    "duration": video["contentDetails"]["duration"],
                    "view_count": video["statistics"].get("viewCount", 0),
                    "like_count": video["statistics"].get("likeCount", 0),
                    "comment_count": video["statistics"].get("commentCount", 0)
                }
                return details
            return None
        except googleapiclient.errors.HttpError as e:
            print(f"API error: {e}")
            return None
    
    def search_videos(self, query, max_results=50, order="relevance"):
        """Search for YouTube videos with a query."""
        if not self.youtube:
            raise ValueError("API key not set. Use set_api_key() first.")
            
        videos = []
        next_page_token = None
        
        try:
            while True:
                request = self.youtube.search().list(
                    part="snippet",
                    q=query,
                    type="video",
                    order=order,
                    maxResults=min(50, max_results - len(videos)),
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response["items"]:
                    video_data = {
                        "id": item["id"]["videoId"],
                        "title": item["snippet"]["title"],
                        "description": item["snippet"]["description"],
                        "channel_title": item["snippet"]["channelTitle"],
                        "published_at": item["snippet"]["publishedAt"]
                    }
                    videos.append(video_data)
                    
                    if len(videos) >= max_results:
                        return videos
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                    
            return videos
        except googleapiclient.errors.HttpError as e:
            print(f"API error: {e}")
            return []

if __name__ == "__main__":
    # Load API key 
    API_KEY = "AIzaSyB5x0F4nohY7KIH-2JKiaFKnNyMRMrzIVQ"  # This is from your example, replace in real usage
    
    # Initialize the YouTube tool
    yt_tool = YoutubeTool(youtube_api_key=API_KEY)
    
    # Test various methods
    # 1. Search for videos on a topic
    print("\n--- Searching for videos ---")
    search_results = yt_tool.search_videos("python tutorial", max_results=3)
    for i, video in enumerate(search_results, 1):
        print(f"{i}. {video['title']} (ID: {video['id']})")
    
    # 2. Get a video transcript
    if search_results:
        test_video_id = search_results[0]['id']
        print(f"\n--- Getting transcript for video {test_video_id} ---")
        transcript = yt_tool.get_transcript(test_video_id)
        if transcript:
            print(f"Transcript snippet: {transcript[:150]}...")
        else:
            print("No transcript available.")
    
    # 3. Try to get channel ID for a popular channel
    print("\n--- Getting channel ID ---")
    channel_name = "Google Developers"
    channel_id = yt_tool.get_channel_id_by_name(channel_name)
    print(f"Channel ID for '{channel_name}': {channel_id}")
    
    # 4. Get playlists from the channel
    if channel_id:
        print(f"\n--- Getting playlists from channel {channel_id} ---")
        playlists = yt_tool.get_playlists_from_channel(channel_id, max_results=3)
        for i, playlist in enumerate(playlists, 1):
            print(f"{i}. {playlist['title']} ({playlist['video_count']} videos)")
        
        # 5. Get videos from a playlist
        if playlists:
            test_playlist_id = playlists[0]['id']
            print(f"\n--- Getting videos from playlist {test_playlist_id} ---")
            playlist_videos = yt_tool.get_videos_from_playlist(test_playlist_id, max_results=3)
            for i, video in enumerate(playlist_videos, 1):
                print(f"{i}. {video['title']} (ID: {video['id']})")