import time
from collections import deque
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import List, Dict
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

@dataclass
class PublishEvent:
    post_id: str
    author_id: str
    published_at: datetime
    content: str
    topic: str
    enqueued_at_perf: float = 0.0

@dataclass
class EventExecutionReport:
    post_id: str
    author_id: str
    followers_count: int
    logical_writes: int
    accept_time_ms: float
    queue_wait_ms: float
    distribution_time_ms: float
    visibility_lag_ms: float


class Stage4CelebrityAnalyzer:
    def __init__(self, session):
        self.session = session
        self.publish_queue: deque[PublishEvent] = deque()

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

    @staticmethod
    def calculate_capacity_scenario(followers: int = 1_000_000, posts_per_min: int = 2, rf: int = 3) -> Dict[str, float]:
        """تحویل ۱: محاسبه عملیات منطقی و فیزیکی سناریوی ظرفیت‌سنجی محصول"""
        logical_writes_per_post = 1 + followers  # 1 post table + N follower feeds
        logical_deliveries_per_min = followers * posts_per_min
        logical_writes_per_min = logical_writes_per_post * posts_per_min
        logical_deliveries_per_sec = logical_deliveries_per_min / 60.0
        replica_writes_per_sec = logical_deliveries_per_sec * rf

        return {
            "logical_writes_per_post": logical_writes_per_post,
            "logical_deliveries_per_min": logical_deliveries_per_min,
            "logical_writes_per_min": logical_writes_per_min,
            "logical_deliveries_per_sec": logical_deliveries_per_sec,
            "replica_writes_per_sec": replica_writes_per_sec,
        }

    def accept_publish_request(self, event: PublishEvent) -> float:
        """
        گام ۱ انتشار (API): ثبت پست اصلی در جدول نویسنده و قرار دادن کار توزیع در صف.
        زمان پذیرش (Accept Time) را برمی‌گرداند.
        """
        t0 = time.perf_counter()
        if event.published_at.tzinfo is None:
            event.published_at = event.published_at.replace(tzinfo=timezone.utc)

        self.session.execute(
            self.stmt_insert_author_post,
            (event.author_id, event.published_at.date(), event.published_at, event.post_id, event.content, event.topic)
        )
        t_accepted = time.perf_counter()
        event.enqueued_at_perf = t0
        self.publish_queue.append(event)
        return (t_accepted - t0) * 1000.0

    def process_queue_worker(self, accept_times_ms: Dict[str, float], concurrency: int = 32) -> List[EventExecutionReport]:
        """
        گام ۲ انتشار (Worker): پردازش ترتیبی رویدادهای صف و اندازه‌گیری زمان انتظار صف،
        زمان تکمیل توزیع و تأخیر دیده‌شدن (Visibility Lag) برای آخرین گیرنده.
        """
        reports: List[EventExecutionReport] = []

        while self.publish_queue:
            event = self.publish_queue.popleft()
            t_worker_start = time.perf_counter()
            queue_wait_ms = (t_worker_start - event.enqueued_at_perf) * 1000.0

            follower_rows = list(self.session.execute(self.stmt_get_followers, (event.author_id,)))
            followers = [r.follower_id for r in follower_rows]

            day_bucket = event.published_at.date()
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
                        raise RuntimeError(f"Write failed during fan-out of {event.post_id}")

            t_worker_end = time.perf_counter()
            dist_time_ms = (t_worker_end - t_worker_start) * 1000.0
            visibility_lag_ms = (t_worker_end - event.enqueued_at_perf) * 1000.0

            reports.append(EventExecutionReport(
                post_id=event.post_id,
                author_id=event.author_id,
                followers_count=len(followers),
                logical_writes=1 + len(followers),
                accept_time_ms=accept_times_ms[event.post_id],
                queue_wait_ms=queue_wait_ms,
                distribution_time_ms=dist_time_ms,
                visibility_lag_ms=visibility_lag_ms
            ))

        return reports

    def simulate_partial_failure_and_replay(self, event: PublishEvent, fail_after_count: int = 100):
        """
        چالش تکمیلی بخش مدرس:
        قطع پردازش توزیع پس از بخشی از گیرندگان و اجرای دوباره کل رویداد برای بررسی
        گیرندگان جامانده و تکرارپذیری (Idempotency) عملیات.
        """
        follower_rows = list(self.session.execute(self.stmt_get_followers, (event.author_id,)))
        followers = [r.follower_id for r in follower_rows]
        day_bucket = event.published_at.date()

        # ۱. اجرای ناقص (قطع شدن پس از fail_after_count گیرنده)
        partial_followers = followers[:fail_after_count]
        for f_id in partial_followers:
            self.session.execute(
                self.stmt_insert_user_feed,
                (f_id, day_bucket, event.published_at, event.post_id, event.author_id, event.content, event.topic)
            )

        # ۲. اجرای مجدد (Replay) کل رویداد از ابتدا
        for f_id in followers:
            self.session.execute(
                self.stmt_insert_user_feed,
                (f_id, day_bucket, event.published_at, event.post_id, event.author_id, event.content, event.topic)
            )


# ==========================================
# اجرای آزمایش کوچک کلاس (۱۰٬۰۰۰ دنبال‌کننده) و ثبت گزارش
# ==========================================
def seed_stage4_celebrity_scenario(session, celeb_followers_count: int = 10_000, normal_followers_count: int = 50):
    session.execute("TRUNCATE ravand.author_followers")
    session.execute("TRUNCATE ravand.posts_by_author")
    session.execute("TRUNCATE ravand.user_feed")

    stmt_follower = session.prepare(
        "INSERT INTO ravand.author_followers (author_id, follower_id) VALUES (?, ?)"
    )

    # ثبت ۱۰٬۰۰۰ دنبال‌کننده برای نویسنده مشهور (celeb_1) با اجرای هم‌زمان سریع
    celeb_params = [("celeb_1", f"user_{i:05d}") for i in range(1, celeb_followers_count + 1)]
    execute_concurrent_with_args(session, stmt_follower, celeb_params, concurrency=64)

    # ثبت ۵۰ دنبال‌کننده برای نویسنده عادی (normal_author)
    normal_params = [("normal_author", f"user_{i:05d}") for i in range(1, normal_followers_count + 1)]
    execute_concurrent_with_args(session, stmt_follower, normal_params, concurrency=32)


if __name__ == "__main__":
    # ۱. نمایش محاسبات ظرفیت‌سنجی مفهومی (تحویل ۱)
    cap = Stage4CelebrityAnalyzer.calculate_capacity_scenario()
    print("=== Stage 4 Capacity Math (1M Followers, 2 Posts/min) ===")
    print(f"Logical writes per post:       {cap['logical_writes_per_post']:,}")
    print(f"Logical deliveries per minute: {cap['logical_deliveries_per_min']:,}")
    print(f"Avg logical deliveries / sec:  {cap['logical_deliveries_per_sec']:,.2f}")
    print(f"Avg physical replica writes/s: {cap['replica_writes_per_sec']:,.2f} (with RF=3)\n")

    # ۲. اجرای آزمایش کوچک کلاس (۱۰٬۰۰۰ دنبال‌کننده)
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    seed_stage4_celebrity_scenario(session, celeb_followers_count=10_000, normal_followers_count=50)
    analyzer = Stage4CelebrityAnalyzer(session)

    ev_celeb = PublishEvent(
        post_id="post_celeb_01",
        author_id="celeb_1",
        published_at=datetime(2026, 5, 10, 11, 50, 0, tzinfo=timezone.utc),
        content="Hello from celebrity!",
        topic="news"
    )
    ev_normal = PublishEvent(
        post_id="post_normal_01",
        author_id="normal_author",
        published_at=datetime(2026, 5, 10, 11, 50, 1, tzinfo=timezone.utc),
        content="Normal user post right after celebrity",
        topic="tech"
    )

    # پذیرش هر دو پست پشت سر هم (پست مشهور و بلافاصله پست نویسنده عادی)
    accept_times = {}
    accept_times[ev_celeb.post_id] = analyzer.accept_publish_request(ev_celeb)
    accept_times[ev_normal.post_id] = analyzer.accept_publish_request(ev_normal)

    # پردازش صف و استخراج سنجه‌ها (تحویل ۲)
    reports = analyzer.process_queue_worker(accept_times, concurrency=32)

    print("=== Stage 4 Small Class Experiment (10,000 Followers) ===")
    print(f"{'Post ID':<16} | {'Author':<14} | {'Logical Writes':<14} | {'Accept(ms)':<10} | {'QueueWait(ms)':<13} | {'DistTime(ms)':<12} | {'VisibilityLag(ms)':<17}")
    print("-" * 110)
    for r in reports:
        print(f"{r.post_id:<16} | {r.author_id:<14} | {r.logical_writes:<14} | "
              f"{r.accept_time_ms:<10.2f} | {r.queue_wait_ms:<13.2f} | "
              f"{r.distribution_time_ms:<12.2f} | {r.visibility_lag_ms:<17.2f}")

    cluster.shutdown()