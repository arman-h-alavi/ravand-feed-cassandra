from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

@dataclass
class Post:
    post_id: str
    author_id: str
    published_at: datetime
    content: str
    topic: str

class Stage1FeedService:
    def __init__(self, session):
        self.session = session
        self.stmt_get_follows = self.session.prepare(
            "SELECT author_id FROM ravand.user_follows WHERE follower_id = ?"
        )
        self.stmt_get_posts = self.session.prepare("""
            SELECT post_id, author_id, published_at, content, topic
            FROM ravand.posts_by_author
            WHERE author_id = ?
              AND day_bucket = ?
              AND published_at >= ?
              AND published_at < ?
            LIMIT 20
        """)

    @staticmethod
    def _get_day_buckets(start_time: datetime, end_time: datetime) -> List[datetime.date]:
        """استخراج تمام روزهای تقویمی (UTC) که بازه [start_time, end_time) را پوشش می‌دهند."""
        buckets = []
        current_date = start_time.date()
        end_date = end_time.date()
        while current_date <= end_date:
            buckets.append(current_date)
            current_date += timedelta(days=1)
        return buckets

    def get_feed(self, user_id: str, ref_time: datetime, limit: int = 20) -> List[Post]:
        """
        مسیر خواندن مرحله اول (Fan-out on Read):
        بازگرداندن حداکثر ۲۰ پست مجاز در بازه [ref_time - 48h, ref_time)
        """
        # اطمینان از UTC بودن زمان مرجع
        if ref_time.tzinfo is None:
            ref_time = ref_time.replace(tzinfo=timezone.utc)
        start_time = ref_time - timedelta(hours=48)

        # ۱. دریافت فهرست نویسندگان دنبال‌شده
        follow_rows = self.session.execute(self.stmt_get_follows, (user_id,))
        followed_authors = [row.author_id for row in follow_rows]

        # حالت مرزی: کاربر هیچ‌کس را دنبال نمی‌کند
        if not followed_authors:
            return []

        # ۲. محاسبه باکت‌های روزانه برای پوشش پنجره ۴۸ ساعته
        day_buckets = self._get_day_buckets(start_time, ref_time)

        # ۳. خواندن پست‌های هر نویسنده از پارتیشن‌های مربوطه
        collected_posts: dict[str, Post] = {}
        for author_id in followed_authors:
            for bucket in day_buckets:
                rows = self.session.execute(
                    self.stmt_get_posts,
                    (author_id, bucket, start_time, ref_time)
                )
                for r in rows:
                    # تضمین عدم تکرار شناسه پست
                    if r.post_id not in collected_posts:
                        pub_at = r.published_at.replace(tzinfo=timezone.utc) if r.published_at.tzinfo is None else r.published_at
                        collected_posts[r.post_id] = Post(
                            post_id=r.post_id,
                            author_id=r.author_id,
                            published_at=pub_at,
                            content=r.content,
                            topic=r.topic
                        )

        # ۴. مرتب‌سازی در برنامه: زمان انتشار نزولی، سپس شناسه پست نزولی
        sorted_posts = sorted(
            collected_posts.values(),
            key=lambda p: (p.published_at, p.post_id),
            reverse=True
        )

        return sorted_posts[:limit]


# ==========================================
# بخش بارگذاری داده آزمون و بررسی صحت (Test Harness)
# ==========================================
def seed_stage1_reference_data(session):
    """ایجاد داده‌های نمونه شامل حالات مرزی ذکر شده در سند"""
    # پاکسازی برای تکرارپذیری
    session.execute("TRUNCATE ravand.user_follows")
    session.execute("TRUNCATE ravand.posts_by_author")

    # روابط دنبال‌کردن: سارا ۳ نویسنده را دنبال می‌کند (author_empty پستی ندارد)
    follows = [
        ("sara", "author_1"),
        ("sara", "author_2"),
        ("sara", "author_empty"),
    ]
    for follower, author in follows:
        session.execute(
            "INSERT INTO ravand.user_follows (follower_id, author_id) VALUES (%s, %s)",
            (follower, author)
        )

    # زمان مرجع آزمون: 2026-05-10 12:00:00 UTC
    # بازه مجاز: [2026-05-08 12:00:00 UTC, 2026-05-10 12:00:00 UTC)
    sample_posts = [
        # خارج از بازه (قدیمی‌تر از ۴۸ ساعت) -> نباید برگردد
        ("post_100", "author_1", datetime(2026, 5, 8, 11, 59, 59, tzinfo=timezone.utc), "Too old", "tech"),
        # دقیقاً روی مرز شروع بازه (شامل) -> باید برگردد
        ("post_101", "author_2", datetime(2026, 5, 8, 12, 0, 0, tzinfo=timezone.utc), "Exact start boundary", "travel"),
        # پست نویسنده دنبال‌نشده -> نباید برگردد
        ("post_102", "author_unfollowed", datetime(2026, 5, 9, 10, 0, 0, tzinfo=timezone.utc), "Unfollowed author", "tech"),
        # دو پست هم‌زمان برای تست Tie-breaking با شناسه پست نزولی
        ("post_103", "author_1", datetime(2026, 5, 9, 15, 0, 0, tzinfo=timezone.utc), "Tie timestamp A", "tech"),
        ("post_104", "author_2", datetime(2026, 5, 9, 15, 0, 0, tzinfo=timezone.utc), "Tie timestamp B", "travel"),
        # پست عادی داخل بازه
        ("post_105", "author_1", datetime(2026, 5, 10, 11, 30, 0, tzinfo=timezone.utc), "Recent post", "tech"),
        # دقیقاً روی مرز پایان بازه (غیرشامل) -> نباید برگردد
        ("post_106", "author_1", datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc), "Exact end boundary", "tech"),
    ]

    for post_id, author_id, pub_at, content, topic in sample_posts:
        day_bucket = pub_at.date()
        session.execute("""
            INSERT INTO ravand.posts_by_author
            (author_id, day_bucket, published_at, post_id, content, topic)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (author_id, day_bucket, pub_at, post_id, content, topic))


if __name__ == "__main__":
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # فرض بر این است که شِمای بالا از قبل اجرا شده است
    seed_stage1_reference_data(session)
    service = Stage1FeedService(session)

    ref_time = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)

    # ۱. بررسی فید سارا
    sara_feed = service.get_feed("sara", ref_time)
    sara_post_ids = [p.post_id for p in sara_feed]
    expected_sara_ids = ["post_105", "post_104", "post_103", "post_101"]
    assert sara_post_ids == expected_sara_ids, f"Mismatch! Got {sara_post_ids}"
    print("Sara's feed verified successfully:", sara_post_ids)

    # ۲. بررسی کاربر بدون دنبال‌شونده
    empty_feed = service.get_feed("user_no_follows", ref_time)
    assert empty_feed == [], "Expected empty feed for user with no follows!"
    print("Empty user feed verified successfully:", empty_feed)

    cluster.shutdown()