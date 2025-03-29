import os
import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build
from dotenv import load_dotenv

# ✅ 환경변수 로드 (여기에 AWS 및 YouTube API KEY가 저장된 .env 파일 경로 지정)
load_dotenv(r"C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/docker/.env")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# ✅ YouTube API 클라이언트 초기화
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# ✅ 1. 검색어로 video_id 가져오는 함수 (숏츠 우선 검색)
def get_video_id_by_keyword(keyword):
    search_response = youtube.search().list(
        q=keyword + " #shorts",
        part="snippet",
        type="video",
        order="relevance",
        maxResults=1
    ).execute()

    if search_response["items"]:
        video_id = search_response["items"][0]["id"]["videoId"]
        print(f"✅ '{keyword}' 검색으로 찾은 video_id: {video_id}")
        return video_id
    else:
        print(f"🚨 '{keyword}'에 대한 결과가 없습니다.")
        return None

# ✅ 2. 특정 video_id의 상세 정보를 가져오는 함수
def get_video_details(video_id):
    response = youtube.videos().list(
        part="snippet,statistics",
        id=video_id
    ).execute()

    if not response["items"]:
        print(f"🚨 video_id {video_id} 정보를 찾을 수 없습니다.")
        return None

    item = response["items"][0]
    snippet = item["snippet"]
    stats = item["statistics"]

    video_data = {
        "video_id": video_id,
        "title": snippet["title"],
        "description": snippet["description"],
        "published_at": snippet["publishedAt"],
        "channel_id": snippet["channelId"],
        "channel_title": snippet["channelTitle"],
        "view_count": int(stats.get("viewCount", 0)),
        "like_count": int(stats.get("likeCount", 0)),
        "comment_count": int(stats.get("commentCount", 0)),
        "tags": snippet.get("tags", [])
    }
    return video_data

# ✅ 3. 채널 정보를 가져오는 함수
def get_channel_info(channel_id):
    response = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    ).execute()

    if not response["items"]:
        print(f"🚨 channel_id {channel_id} 정보를 찾을 수 없습니다.")
        return None

    item = response["items"][0]
    stats = item["statistics"]
    channel_data = {
        "channel_id": channel_id,
        "channel_title": item["snippet"]["title"],
        "subscriber_count": int(stats.get("subscriberCount", 0)),
        "total_views": int(stats.get("viewCount", 0)),
        "video_count": int(stats.get("videoCount", 0))
    }
    return channel_data

# ✅ 4. 영상의 댓글 가져오기 함수
def get_video_comments(video_id, max_results=50):
    comments = []
    response = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=max_results
    ).execute()

    for item in response["items"]:
        comment = item["snippet"]["topLevelComment"]["snippet"]
        comments.append({
            "video_id": video_id,
            "author": comment["authorDisplayName"],
            "comment_text": comment["textDisplay"],
            "published_at": comment["publishedAt"],
            "like_count": comment["likeCount"]
        })
    return comments

# ✅ 5. 저장 함수 (날짜 폴더 → 서비스명 폴더 → 데이터 종류 폴더에 저장)
def save_to_folder(dataframe, root_dir, date_folder, service_folder, sub_folder_name):
    folder_path = os.path.join(root_dir, date_folder, service_folder, sub_folder_name)
    os.makedirs(folder_path, exist_ok=True)

    csv_path = os.path.join(folder_path, "data.csv")
    parquet_path = os.path.join(folder_path, "data.parquet")

    dataframe.to_csv(csv_path, index=False, encoding="utf-8-sig")
    dataframe.to_parquet(parquet_path, index=False)

    print(f"✅ '{sub_folder_name}' 저장 완료:\n{csv_path}\n{parquet_path}")

# ✅ 6. 실행
if __name__ == "__main__":
    search_keyword = "이온음료 추천"  # 분석할 키워드

    # ✅ 경로 및 폴더명 정보
    save_root_dir = "C:/project/sportsdrink-pipeline-spark-airflow/data_pipeline/data/raw/"
    service_name = "sportsdrink"  # 서비스명
    data_source = "youtube"  # 데이터 출처 (ex: Twitter, Naver, YouTube 등)
    data_type = "search"  # 데이터 성격 (ex: Search, Sales, Trend, Log 등)
    batch_cycle = "daily"  # 배치 주기 (ex: Hourly, Daily, Weekly, Monthly)
    date_str = datetime.now().strftime('%Y%m%d')  # 날짜 폴더 생성용

    # 날짜 및 서비스명 폴더명
    date_folder = date_str
    service_folder = f"{service_name}_{data_source}_{data_type}_{batch_cycle}"

    # ✅ video_id 가져오기
    video_id = get_video_id_by_keyword(search_keyword)

    if video_id:
        print("\n🔎 영상 정보 가져오는 중...")
        video_data = get_video_details(video_id)
        video_df = pd.DataFrame([video_data])
        save_to_folder(video_df, save_root_dir, date_folder, service_folder, "video_info")

        print("\n🔎 채널 정보 가져오는 중...")
        channel_info = get_channel_info(video_data["channel_id"])
        channel_df = pd.DataFrame([channel_info])
        save_to_folder(channel_df, save_root_dir, date_folder, service_folder, "channel_info")

        print("\n🔎 댓글 가져오는 중...")
        comments = get_video_comments(video_id, max_results=30)
        comments_df = pd.DataFrame(comments)
        save_to_folder(comments_df, save_root_dir, date_folder, service_folder, "comments")

    else:
        print("⚠️ video_id를 찾지 못했습니다.")
