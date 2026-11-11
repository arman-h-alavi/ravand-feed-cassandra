import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List, Tuple, Dict
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

@dataclass
class Post:
    post_id: str
    author_id: str
    published_at: datetime
    content: str
    topic: str

@dataclass
class PublishEvent:
    """رویداد انتشار با شناسه و زمان پایدار تعیین‌شده توسط سامانه"""
    post_id: str
    author_id: str
    published_at: datetime
    content: str
    topic: str

@dataclass
class PublishMetrics:
    logical_writes: int
    followers_count: int
    accept_time_ms: float
    fanout_completion_ms: float
    last_recipient_visibility_lag_ms: float

@dataclass
class ReadMetrics:
    db_reads: int
    rows_fetched: int
    total_time_ms: float


class Stage3FeedService:
    def __init__(self, session):
        self.session = session

        # گزاره‌های مربوط به مسیر انتشار (Stage 3 Fan-out on Write)
        self.stmt_insert_author_post = self.session.prepare("""
            INSERT INTO ravand.posts_by_author
            (author_id, day_bucket, published_at, post_id, content, topic)
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        self.stmt_get_followers = self.session.prepare("""
            SELECT follower_id FROM ravand.author_followers WHERE author_id = ?
        """)
        self.stmt_insert_user_feed = self.session.prepare("""
            INSERT INTO ravand.user_feed
            (user_id, day_bucket, published_at, post_id, author_id, content, topic)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

        # گزاره مربوط به مسیر خواندن جدید از فید آماده (Stage 3)
        self.stmt_get_precomputed_feed = self.session.prepare("""
            SELECT post_id, author_id, published_at, content, topic
            FROM ravand.user_feed
            WHERE user_id = ?
              AND day_bucket = ?
              AND published_at >= ?
              AND published_at < ?
            LIMIT ?
        """)

        # گزاره‌های مربوط به نسخه قبلی (Stage 1/2) برای مقایسه صحت و هزینه
        self.stmt_get_follows = self.session.prepare(
            "SELECT author_id FROM ravand.user_follows WHERE follower_id = ?"
        )
        self.stmt_get_posts_by_author = self.session.prepare("""
            SELECT post_id, author_id, published_at, content, topic
            FROM ravand.posts_by_author
            WHERE author_id = ?
              AND day_bucket = ?
              AND published_at >= ?
              AND published_at < ?
            LIMIT 20
        """)

    @staticmethod
    def _get_day_buckets_desc(start_time: datetime, end_time: datetime) -> List[datetime.date]:
        """برگرداندن باکت‌های روزانه از جدیدترین روز به قدیمی‌ترین روز در پنجره ۴۸ ساعته"""
        buckets = []
        current_date = end_time.date()
        start_date = start_time.date()
        while current_date >= start_date:
            buckets.append(current_date)
            current_date -= timedelta(days=1)
        return buckets

    def process_publish_event(self, event: PublishEvent, concurrency: int = 16) -> PublishMetrics:
        """
        مسیر انتشار مرحله سوم (Fan-out on Write):
        ۱. ذخیره پست در جدول نویسنده
        ۲. دریافت فهرست گیرندگان (دنبال‌کنندگان)
        ۳. نوشتن پست در جدول user_feed تمام گیرندگان (Idempotent)
        """
        if event.published_at.tzinfo is None:
            event.published_at = event.published_at.replace(tzinfo=timezone.utc)
        day_bucket = event.published_at.date()

        t_start = time.perf_counter()

        # ۱. ثبت پست اصلی نویسنده (۱ نوشتن منطقی)
        self.session.execute(
            self.stmt_insert_author_post,
            (event.author_id, day_bucket, event.published_at, event.post_id, event.content, event.topic)
        )
        t_accepted = time.perf_counter()

        # ۲. دریافت فهرست دنبال‌کنندگان نویسنده
        follower_rows = list(self.session.execute(self.stmt_get_followers, (event.author_id,)))
        followers = [r.follower_id for r in follower_rows]

        # ۳. تحویل پست به فید آماده تک‌تک دنبال‌کنندگان (F نوشتن منطقی)
        if followers:
            write_params = [
                (follower_id, day_bucket, event.published_at, event.post_id, event.author_id, event.content, event.topic)
                for follower_id in followers
            ]
            results = execute_concurrent_with_args(
                self.session,
                self.stmt_insert_user_feed,
                write_params,
                concurrency=concurrency
            )
            for success, _ in results:
                if not success:
                    raise RuntimeError("Failed to deliver post to a follower feed")

        t_completed = time.perf_counter()

        logical_writes = 1 + len(followers)
        accept_ms = (t_accepted - t_start) * 1000
        fanout_ms = (t_completed - t_accepted) * 1000
        visibility_lag_ms = (t_completed - t_start) * 1000

        return PublishMetrics(
            logical_writes=logical_writes,
            followers_count=len(followers),
            accept_time_ms=accept_ms,
            fanout_completion_ms=fanout_ms,
            last_recipient_visibility_lag_ms=visibility_lag_ms
        )

    def get_feed_precomputed(self, user_id: str, ref_time: datetime, limit: int = 20) -> Tuple[List[Post], ReadMetrics]:
        """
        مسیر خواندن مرحله سوم:
        خواندن مستقیم از جدول user_feed کاربر از جدیدترین باکت روزانه به قدیم
        """
        t_start = time.perf_counter()
        if ref_time.tzinfo is None:
            ref_time = ref_time.replace(tzinfo=timezone.utc)
        start_time = ref_time - timedelta(hours=48)

        day_buckets = self._get_day_buckets_desc(start_time, ref_time)
        collected_posts: List[Post] = []
        seen_post_ids = set()
        db_reads = 0
        rows_fetched = 0

        for bucket in day_buckets:
            remaining = limit - len(collected_posts)
            if remaining <= 0:
                # چون باکت‌ها به ترتیب نزولی روز هستند و درون هر باکت نیز ترتیب نزولی است،
                # به محض تکمیل ۲۰ پست مجاز نیازی به خواندن باکت روزهای قبل نیست
                break

            rows = list(self.session.execute(
                self.stmt_get_precomputed_feed,
                (user_id, bucket, start_time, ref_time, remaining)
            ))
            db_reads += 1
            rows_fetched += len(rows)

            for r in rows:
                if r.post_id not in seen_post_ids:
                    seen_post_ids.add(r.post_id)
                    pub_at = r.published_at.replace(tzinfo=timezone.utc) if r.published_at.tzinfo is None else r.published_at
                    collected_posts.append(Post(
                        post_id=r.post_id,
                        author_id=r.author_id,
                        published_at=pub_at,
                        content=r.content,
                        topic=r.topic
                    ))

        # مرتب‌سازی قطعی نهایی (صرفاً برای اطمینان کامل در مرز باکت‌ها)
        collected_posts.sort(key=lambda p: (p.published_at, p.post_id), reverse=True)
        total_ms = (time.perf_counter() - t_start) * 1000

        return collected_posts[:limit], ReadMetrics(db_reads, rows_fetched, total_ms)

    def get_feed_legacy_stage2(self, user_id: str, ref_time: datetime, limit: int = 20) -> Tuple[List[Post], ReadMetrics]:
        """مسیر خواندن نسخه قبلی (مرحله اول/دوم) برای مقایسه درستی و هزینه"""
        t_start = time.perf_counter()
        if ref_time.tzinfo is None:
            ref_time = ref_time.replace(tzinfo=timezone.utc)
        start_time = ref_time - timedelta(hours=48)

        follow_rows = list(self.session.execute(self.stmt_get_follows, (user_id,)))
        db_reads = 1
        rows_fetched = len(follow_rows)
        followed_authors = [r.author_id for r in follow_rows]

        if not followed_authors:
            return [], ReadMetrics(db_reads, rows_fetched, (time.perf_counter() - t_start) * 1000)

        day_buckets = self._get_day_buckets_desc(start_time, ref_time)
        query_params = [
            (author_id, bucket, start_time, ref_time)
            for author_id in followed_authors
            for bucket in day_buckets
        ]
        db_reads += len(query_params)
        results = execute_concurrent_with_args(self.session, self.stmt_get_posts_by_author, query_params, concurrency=16)

        collected: Dict[str, Post] = {}
        for success, rows in results:
            row_list = list(rows)
            rows_fetched += len(row_list)
            for r in row_list:
                if r.post_id not in collected:
                    pub_at = r.published_at.replace(tzinfo=timezone.utc) if r.published_at.tzinfo is None else r.published_at
                    collected[r.post_id] = Post(r.post_id, r.author_id, pub_at, r.content, r.topic)

        sorted_posts = sorted(collected.values(), key=lambda p: (p.published_at, p.post_id), reverse=True)[:limit]
        return sorted_posts, ReadMetrics(db_reads, rows_fetched, (time.perf_counter() - t_start) * 1000)


# ==========================================
# ابزار تست صحت، سنجش انتشار و اجرای مجدد رویداد (Replay Test)
# ==========================================
def run_stage3_verification(session):
    session.execute("TRUNCATE ravand.user_follows")
    session.execute("TRUNCATE ravand.author_followers")
    session.execute("TRUNCATE ravand.posts_by_author")
    session.execute("TRUNCATE ravand.user_feed")

    service = Stage3FeedService(session)
    ref_time = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)

    # ۱. ثبت روابط دنبال‌کردن در هر دو جدول (سارا دنبال‌کننده ۳ نویسنده، به علاوه ۵۰ دنبال‌کننده دیگر برای author_1)
    follows = [("sara", "author_1"), ("sara", "author_2"), ("sara", "author_empty")]
    for i in range(1, 50):
        follows.append((f"user_{i:03d}", "author_1"))

    for follower_id, author_id in follows:
        session.execute("INSERT INTO ravand.user_follows (follower_id, author_id) VALUES (%s, %s)", (follower_id, author_id))
        session.execute("INSERT INTO ravand.author_followers (author_id, follower_id) VALUES (%s, %s)", (author_id, follower_id))

    # ۲. انتشار پست‌های نمونه از طریق مسیر جدید انتشار
    events = [
        PublishEvent("post_100", "author_1", datetime(2026, 5, 8, 11, 59, 59, tzinfo=timezone.utc), "Too old", "tech"),
        PublishEvent("post_101", "author_2", datetime(2026, 5, 8, 12, 0, 0, tzinfo=timezone.utc), "Boundary start", "travel"),
        PublishEvent("post_103", "author_1", datetime(2026, 5, 9, 15, 0, 0, tzinfo=timezone.utc), "Tie A", "tech"),
        PublishEvent("post_104", "author_2", datetime(2026, 5, 9, 15, 0, 0, tzinfo=timezone.utc), "Tie B", "travel"),
        PublishEvent("post_105", "author_1", datetime(2026, 5, 10, 11, 30, 0, tzinfo=timezone.utc), "Recent", "tech"),
    ]

    for ev in events:
        pub_metrics = service.process_publish_event(ev)
        if ev.post_id == "post_105":
            print(f"[Publish Metrics for {ev.post_id}] Followers: {pub_metrics.followers_count} | "
                  f"Logical Writes: {pub_metrics.logical_writes} | "
                  f"Accept: {pub_metrics.accept_time_ms:.2f}ms | "
                  f"Visibility Lag: {pub_metrics.last_recipient_visibility_lag_ms:.2f}ms")

    # ۳. آزمایش اجرای مجدد رویداد انتشار (Idempotency / Replay Test)
    duplicate_event = events[-1]  # اجرای دوباره رویداد post_105 با همان شناسه و زمان انتشار
    service.process_publish_event(duplicate_event)

    # ۴. مقایسه خروجی و هزینه فید آماده (Stage 3) با نسخه مرجع قبلی (Stage 1/2)
    feed_stage3, read_m3 = service.get_feed_precomputed("sara", ref_time)
    feed_legacy, read_m_legacy = service.get_feed_legacy_stage2("sara", ref_time)

    ids_stage3 = [p.post_id for p in feed_stage3]
    ids_legacy = [p.post_id for p in feed_legacy]

    assert ids_stage3 == ids_legacy == ["post_105", "post_104", "post_103", "post_101"], \
        f"Feed mismatch or duplicate detected! Got: {ids_stage3}"
    assert len(ids_stage3) == len(set(ids_stage3)), "Duplicate post_id found in feed!"

    print(f"[Read Comparison for Sara] Legacy DB Reads: {read_m_legacy.db_reads} vs Stage 3 DB Reads: {read_m3.db_reads}")
    print(f"[Replay & Correctness Check Passed] Final Feed IDs: {ids_stage3}")


if __name__ == "__main__":
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    run_stage3_verification(session)
    cluster.shutdown()