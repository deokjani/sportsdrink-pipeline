import feedparser
from kafka import KafkaProducer
import json

# Kafka 설정
KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "news_rss_topic"

# ✅ 최종 검증된 RSS 피드 주소 (정상 응답 기준)
RSS_FEEDS = [
    "https://www.chosun.com/arc/outboundfeeds/rss/category/economy/?outputType=xml",
    "https://www.mk.co.kr/rss/30000001/",
    "https://www.yna.co.kr/rss/economy.xml",
    "https://www.yna.co.kr/rss/industry.xml",
    "http://rss.donga.com/economy.xml",
    "http://rss.segye.com/segye_economy.xml"
]


# ✅ 브랜드 키워드 (정식명, 띄어쓰기 포함, 영문 혼용 가능)
BRANDS = [
    "포카리", "포카리스웨트", "Pocari Sweat",
    "게토레이", "Gatorade",
    "파워에이드", "Powerade",
    "토레타", "Toreta",
    "링티", "Lingt",
    "이온음료", "이온 음료", "스포츠음료", "스포츠 음료", "기능성 음료"
]

# ✅ 시장 트렌드 / 소비 분석 키워드
FILTER_KEYWORDS = [
    # 📊 시장 점유율 / 경쟁 구도
    "시장 점유율", "브랜드 점유율", "업계 점유율",
    "판매량 증가", "판매량 감소", "판매 추이", "매출 추이",
    "성장률", "전년 대비 성장", "성장세", "실적 증가",
    "소비 트렌드", "소비 패턴", "음료 소비 분석", "소비 변화",
    "브랜드 경쟁", "경쟁 심화", "경쟁 브랜드", "음료업계 경쟁",
    "시장 진입", "시장 확대", "시장 동향", "업계 동향",
    "시장조사", "트렌드 리포트", "카테고리 리더", "브랜드 파워",

    # 📢 마케팅 / 제품 전략
    "마케팅 전략", "브랜드 마케팅", "디지털 마케팅", "SNS 마케팅",
    "신제품 출시", "음료 신제품", "패키지 리뉴얼", "제품 리뉴얼",
    "콜라보 제품", "이색 콜라보", "한정판 음료", "이벤트 한정",
    "건강 강조", "프리미엄 제품", "프리미엄 음료", "컨셉 음료",
    "기능성 음료", "타겟 마케팅", "광고 캠페인", "브랜드 모델",
    "리브랜딩", "제품 이미지 변화", "브랜드 이미지 강화",

    # 🛒 유통 / 채널 / 오프라인
    "유통채널", "온라인 판매", "이커머스", "온라인 플랫폼",
    "편의점 음료", "편의점 매출", "CU 음료", "GS25 음료",
    "오픈마켓", "네이버 스마트스토어", "SSG닷컴", "마켓컬리",
    "대형마트", "이마트 음료", "홈플러스", "롯데마트",
    "H&B 스토어", "올리브영 음료", "랄라블라", "롭스",
    "헬스스토어", "오프라인 채널", "전국 유통망", "리테일 전략",

    # 🧃 건강/헬스/기능성 관련
    "건강 음료", "스포츠 음료", "헬스케어 음료", "다이어트 음료",
    "운동 후 음료", "운동보충 음료", "피로회복 음료",
    "수분 보충", "수분 보충제", "에너지 보충", "피로 회복",
    "이온 밸런스", "전해질 보충", "비타민 음료", "영양 강화",
    "기능성 표시 식품", "건강기능식품", "면역력 강화", "체력 보충",

    # 👥 소비자 성향 / 트렌드
    "MZ세대 소비", "2030 소비 트렌드", "Z세대 음료",
    "소비자 선호도", "소비자 평가", "고객 반응",
    "웰빙 트렌드", "친환경 트렌드", "제로칼로리 음료", "제로 음료",
    "저당 음료", "노슈가", "무카페인", "비건 음료", "식물성 음료",
    "라이프스타일 변화", "건강 중시 소비", "소비심리", "음료 선택 기준"
]

# ✅ 전체 키워드 중복 제거
ALL_BRANDS = set(BRANDS)
ALL_MARKETS = set(FILTER_KEYWORDS)

# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

# 필터링 함수: 브랜드 + 시장 키워드 모두 포함된 경우만 수집
def is_relevant_article(content: str) -> bool:
    return any(b in content for b in ALL_BRANDS) or any(k in content for k in ALL_MARKETS)

# RSS 수집 함수
def fetch_and_send_rss():
    for feed_url in RSS_FEEDS:
        parsed_feed = feedparser.parse(feed_url)
        status = parsed_feed.get("status", "unknown")
        print(f"📥 {feed_url} → HTTP {status}, {len(parsed_feed.entries)}개 기사 수신됨")

        for entry in parsed_feed.entries:
            title = entry.get("title", "")
            summary = entry.get("summary", "")
            content = f"{title} {summary}"

            if is_relevant_article(content):
                news = {
                    "source": feed_url,
                    "title": title,
                    "link": entry.get("link"),
                    "summary": summary,
                    "published": entry.get("published", "")
                }

                producer.send(KAFKA_TOPIC, value=news)
                print(f"✅ 전송 완료: {title}")
            else:
                print(f"⛔️ 필터 불일치 (건너뜀): {title}")

if __name__ == "__main__":
    fetch_and_send_rss()
    producer.close()
