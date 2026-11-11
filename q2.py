import time
import random
import statistics
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List, Tuple, Dict
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

# استفاده از همان ساختار داده مرحله اول
@dataclass
class Post:
    post_id: str
    author_id: str
    published_at: datetime
    content: str
    topic: str

@dataclass
class ReadMetrics:
    db_reads: int
    rows_fetched: int
    db_time_ms: float
    sort_time_ms: float
    total_time_ms: float


class Stage2FeedService:
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
        buckets = []
        current_date = start_time.date()
        end_date = end_time.date()
        while current_date <= end_date:
            buckets.append(current_date)
            current_date += timedelta(days=1)
        return buckets

    def get_feed_sequential(self, user_id: str, ref_time: datetime, limit: int = 20) -> Tuple[List[Post], ReadMetrics]:
        """مسیر خواندن پایه (مرحله اول - ترتیبی) به همراه ثبت دقیق سنجه‌ها"""
        t_start = time.perf_counter()
        if ref_time.tzinfo is None:
            ref_time = ref_time.replace(tzinfo=timezone.utc)
        start_time = ref_time - timedelta(hours=48)

        db_reads = 0
        rows_fetched = 0

        # ۱. خواندن فهرست دنبال‌شونده‌ها
        follow_rows = list(self.session.execute(self.stmt_get_follows, (user_id,)))
        db_reads += 1
        rows_fetched += len(follow_rows)
        followed_authors = [r.author_id for r in follow_rows]

        if not followed_authors:
            t_total = (time.perf_counter() - t_start) * 1000
            return [], ReadMetrics(db_reads, rows_fetched, t_total, 0.0, t_total)

        day_buckets = self._get_day_buckets(start_time, ref_time)
        collected_posts: Dict[str, Post] = {}

        # ۲. خواندن ترتیبی پارتیشن‌های نویسندگان
        for author_id in followed_authors:
            for bucket in day_buckets:
                rows = list(self.session.execute(
                    self.stmt_get_posts,
                    (author_id, bucket, start_time, ref_time)
                ))
                db_reads += 1
                rows_fetched += len(rows)
                for r in rows:
                    if r.post_id not in collected_posts:
                        pub_at = r.published_at.replace(tzinfo=timezone.utc) if r.published_at.tzinfo is None else r.published_at
                        collected_posts[r.post_id] = Post(
                            post_id=r.post_id,
                            author_id=r.author_id,
                            published_at=pub_at,
                            content=r.content,
                            topic=r.topic
                        )

        t_after_db = time.perf_counter()

        # ۳. مرتب‌سازی در حافظه برنامه
        sorted_posts = sorted(
            collected_posts.values(),
            key=lambda p: (p.published_at, p.post_id),
            reverse=True
        )[:limit]

        t_end = time.perf_counter()
        metrics = ReadMetrics(
            db_reads=db_reads,
            rows_fetched=rows_fetched,
            db_time_ms=(t_after_db - t_start) * 1000,
            sort_time_ms=(t_end - t_after_db) * 1000,
            total_time_ms=(t_end - t_start) * 1000
        )
        return sorted_posts, metrics

    def get_feed_bounded_concurrent(
        self, user_id: str, ref_time: datetime, limit: int = 20, concurrency: int = 16
    ) -> Tuple[List[Post], ReadMetrics]:
        """
        مسیر خواندن تغییریافته (مرحله دوم):
        استفاده از هم‌زمانی محدود (Bounded Concurrency) برای کاهش زمان انتظار شبکه
        """
        t_start = time.perf_counter()
        if ref_time.tzinfo is None:
            ref_time = ref_time.replace(tzinfo=timezone.utc)
        start_time = ref_time - timedelta(hours=48)

        db_reads = 0
        rows_fetched = 0

        # ۱. خواندن فهرست دنبال‌شونده‌ها
        follow_rows = list(self.session.execute(self.stmt_get_follows, (user_id,)))
        db_reads += 1
        rows_fetched += len(follow_rows)
        followed_authors = [r.author_id for r in follow_rows]

        if not followed_authors:
            t_total = (time.perf_counter() - t_start) * 1000
            return [], ReadMetrics(db_reads, rows_fetched, t_total, 0.0, t_total)

        day_buckets = self._get_day_buckets(start_time, ref_time)

        # ۲. آماده‌سازی پارامترها و اجرای هم‌زمان با سقف مشخص (concurrency=16)
        query_params = [
            (author_id, bucket, start_time, ref_time)
            for author_id in followed_authors
            for bucket in day_buckets
        ]
        db_reads += len(query_params)

        results = execute_concurrent_with_args(
            self.session,
            self.stmt_get_posts,
            query_params,
            concurrency=concurrency
        )

        collected_posts: Dict[str, Post] = {}
        for success, rows in results:
            if not success:
                raise RuntimeError("Database read failed during concurrent execution")
            row_list = list(rows)
            rows_fetched += len(row_list)
            for r in row_list:
                if r.post_id not in collected_posts:
                    pub_at = r.published_at.replace(tzinfo=timezone.utc) if r.published_at.tzinfo is None else r.published_at
                    collected_posts[r.post_id] = Post(
                        post_id=r.post_id,
                        author_id=r.author_id,
                        published_at=pub_at,
                        content=r.content,
                        topic=r.topic
                    )

        t_after_db = time.perf_counter()

        # ۳. مرتب‌سازی در حافظه برنامه
        sorted_posts = sorted(
            collected_posts.values(),
            key=lambda p: (p.published_at, p.post_id),
            reverse=True
        )[:limit]

        t_end = time.perf_counter()
        metrics = ReadMetrics(
            db_reads=db_reads,
            rows_fetched=rows_fetched,
            db_time_ms=(t_after_db - t_start) * 1000,
            sort_time_ms=(t_end - t_after_db) * 1000,
            total_time_ms=(t_end - t_start) * 1000
        )
        return sorted_posts, metrics


# ==========================================
# ابزار تولید بار کنترل‌شده و بنچمارک مرحله دوم
# ==========================================
def seed_stage2_workload(session, ref_time: datetime):
    """
    ایجاد ۵۰۰ نویسنده با فعالیت کنترل‌شده و ۳ کاربر نمونه
    با ۱۰، ۱۰۰ و ۵۰۰ دنبال‌شونده (با Seed ثابت برای تکرارپذیری)
    """
    random.seed(42)
    session.execute("TRUNCATE ravand.user_follows")
    session.execute("TRUNCATE ravand.posts_by_author")

    authors = [f"author_{i:04d}" for i in range(1, 501)]

    # ایجاد سه گروه کاربر آزمایشی: ۱۰، ۱۰۰ و ۵۰۰ دنبال‌شونده
    groups = {
        "user_follow_10": authors[:10],
        "user_follow_100": authors[:100],
        "user_follow_500": authors[:500],
    }

    stmt_follow = session.prepare("INSERT INTO ravand.user_follows (follower_id, author_id) VALUES (?, ?)")
    for user_id, follow_list in groups.items():
        for author_id in follow_list:
            session.execute(stmt_follow, (user_id, author_id))

    # تولید پست‌های کنترل‌شده (۹ پست برای هر نویسنده در بازه ۴۸ ساعته، پخش در ۳ باکت روزانه)
    stmt_post = session.prepare("""
        INSERT INTO ravand.posts_by_author
        (author_id, day_bucket, published_at, post_id, content, topic)
        VALUES (?, ?, ?, ?, ?, ?)
    """)

    post_counter = 1
    for author_id in authors:
        for hour_offset in [4, 12, 20, 26, 32, 40, 44, 46, 47]:
            pub_at = ref_time - timedelta(hours=hour_offset, minutes=random.randint(0, 50))
            post_id = f"post_{post_counter:06d}"
            post_counter += 1
            session.execute(
                stmt_post,
                (author_id, pub_at.date(), pub_at, post_id, f"Content {post_id}", "tech")
            )


def run_stage2_benchmark(service: Stage2FeedService, ref_time: datetime, iterations: int = 15):
    test_users = ["user_follow_10", "user_follow_100", "user_follow_500"]

    print(f"{'User Group':<18} | {'Mode':<12} | {'DB Reads':<8} | {'Rows':<6} | {'p50 (ms)':<9} | {'p95 (ms)':<9} | {'Sort (ms)':<9}")
    print("-" * 86)

    for user_id in test_users:
        # Warm-up کوتاه طبق قرارداد اندازه‌گیری سند
        # service.get_feed_sequential(user_id, ref_time)
        # service.get_feed_bounded_concurrent(user_id, ref_time, concurrency=16)

        seq_times, conc_times = [], []
        seq_sort_times = []
        last_seq_feed, last_conc_feed = None, None
        last_metrics = None

        for _ in range(iterations):
            last_seq_feed, m_seq = service.get_feed_sequential(user_id, ref_time)
            seq_times.append(m_seq.total_time_ms)
            seq_sort_times.append(m_seq.sort_time_ms)
            last_metrics = m_seq

            last_conc_feed, m_conc = service.get_feed_bounded_concurrent(user_id, ref_time, concurrency=16)
            conc_times.append(m_conc.total_time_ms)

            # بررسی صحت: خروجی روش تغییریافته باید دقیقاً برابر با روش مرجع باشد
            assert [p.post_id for p in last_seq_feed] == [p.post_id for p in last_conc_feed], "Correctness check failed!"

        p50_seq = statistics.median(seq_times)
        p95_seq = sorted(seq_times)[int(0.95 * len(seq_times)) - 1]
        p50_conc = statistics.median(conc_times)
        p95_conc = sorted(conc_times)[int(0.95 * len(conc_times)) - 1]
        avg_sort = statistics.mean(seq_sort_times)

        print(f"{user_id:<18} | {'Sequential':<12} | {last_metrics.db_reads:<8} | {last_metrics.rows_fetched:<6} | {p50_seq:<9.2f} | {p95_seq:<9.2f} | {avg_sort:<9.3f}")
        print(f"{user_id:<18} | {'Conc (16)':<12} | {last_metrics.db_reads:<8} | {last_metrics.rows_fetched:<6} | {p50_conc:<9.2f} | {p95_conc:<9.2f} | {avg_sort:<9.3f}")
        print("-" * 86)


if __name__ == "__main__":
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    ref_time = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
    seed_stage2_workload(session, ref_time)

    service = Stage2FeedService(session)
    run_stage2_benchmark(service, ref_time, iterations=15)

    cluster.shutdown()