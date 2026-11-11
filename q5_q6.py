"""
کارگاه «رَوَند» - مراحل ۵ و ۶ (فید ترکیبی، شخصی‌سازی و تحلیل CTR در Cassandra)

پیش‌نیاز: Keyspace و جداول مراحل ۱ تا ۳ (q1.sql و q3.sql) از قبل ساخته شده باشند.
جداول جدید مرحله ۶ (feed_impressions_by_day و feed_clicks_by_impression) توسط همین فایل
با «CREATE TABLE IF NOT EXISTS» ساخته می‌شوند.

اجرا:  python q5_q6.py
"""
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from cassandra import ConsistencyLevel, DriverException
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.concurrent import execute_concurrent_with_args

# ---------------------------------------------------------------------------
# تنظیمات ثابت
# ---------------------------------------------------------------------------
WINDOW = timedelta(hours=48)
# خواندن و نوشتن با LOCAL_QUORUM (با RF=3: W + R > RF) تا خواندنِ بلافاصله پس از نوشتن، پست را از دست ندهد.
CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM
# خطاهایی که «خرابی یک منبع داده» محسوب می‌شوند (NoHostAvailable زیرکلاس DriverException نیست).
SOURCE_ERRORS = (DriverException, NoHostAvailable)

# مجموعه نویسندگان مشهور: در طول هر اجرا ثابت است (ورودی/تنظیمات سرویس).
CELEBRITY_AUTHORS = frozenset({"celeb_1", "celeb_bulk", "celeb_bench"})

REF_TIME = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class ModelConfig:
    topic_bonus: float
    recency_weight: float


# v1 دقیقاً فرمول صورت‌مسئله است. v2 فقط برای نمایش تفکیک CTR بر اساس نسخه مدل است (ضریب موضوع ۵.۰).
MODEL_CONFIGS: Dict[str, ModelConfig] = {
    "v1": ModelConfig(topic_bonus=10.0, recency_weight=0.1),
    "v2": ModelConfig(topic_bonus=5.0, recency_weight=0.1),
}


# ---------------------------------------------------------------------------
# مدل‌های داده
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Post:
    post_id: str
    author_id: str
    published_at: datetime
    content: str
    topic: str


@dataclass
class PublishMetrics:
    path: str
    followers_fanned_out: int
    logical_writes: int
    accept_ms: float
    fanout_ms: float


@dataclass
class CandidateResult:
    posts: List[Post]
    is_partial: bool
    failed_sources: List[str]
    db_reads: int
    candidate_gen_ms: float


@dataclass
class ScoredPost:
    post: Post
    score: float


@dataclass
class PersonalizedFeed:
    user_id: str
    model_version: str
    candidates: List[Post]
    ranked: List[ScoredPost]
    is_partial: bool
    failed_sources: List[str]
    candidate_gen_ms: float
    ranking_ms: float


@dataclass
class CtrRow:
    day: date
    model_version: str
    impressions: int
    clicked_impressions: int
    click_rows: int

    @property
    def ctr(self) -> Optional[float]:
        return self.clicked_impressions / self.impressions if self.impressions else None


# ---------------------------------------------------------------------------
# ابزارهای کمکی
# ---------------------------------------------------------------------------
def utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def elapsed_ms(t0: float, t1: float) -> float:
    return (t1 - t0) * 1000.0


def prepare(session, cql: str):
    stmt = session.prepare(cql)
    stmt.consistency_level = CONSISTENCY
    return stmt


def day_buckets_desc(start_time: datetime, end_time: datetime) -> List[date]:
    """روزهای تقویمی UTC پوشش‌دهنده بازه، از جدیدترین به قدیمی‌ترین."""
    buckets, current = [], end_time.date()
    while current >= start_time.date():
        buckets.append(current)
        current -= timedelta(days=1)
    return buckets


def sort_posts(posts: Iterable[Post]) -> List[Post]:
    return sorted(posts, key=lambda p: (p.published_at, p.post_id), reverse=True)


def score_post(post: Post, interests: set, ref_time: datetime, cfg: ModelConfig) -> float:
    age_hours = (ref_time - post.published_at).total_seconds() / 3600.0
    topic_score = cfg.topic_bonus if post.topic in interests else 0.0
    recency_score = max(0.0, (48.0 - age_hours) * cfg.recency_weight)
    return round(topic_score + recency_score, 6)


# ---------------------------------------------------------------------------
# شِمای جداول جدید و بازنشانی داده آزمون
# ---------------------------------------------------------------------------
NEW_TABLES_CQL = [
    """
    CREATE TABLE IF NOT EXISTS ravand.feed_impressions_by_day (
        day_bucket date,
        model_version text,
        impression_id text,
        user_id text,
        post_id text,
        shown_at timestamp,
        PRIMARY KEY ((day_bucket, model_version), impression_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ravand.feed_clicks_by_impression (
        impression_id text,
        click_id text,
        clicked_at timestamp,
        PRIMARY KEY ((impression_id), click_id)
    )
    """,
]

ALL_TABLES = [
    "users", "user_follows", "author_followers", "posts_by_author", "user_feed",
    "feed_impressions_by_day", "feed_clicks_by_impression",
]


def ensure_schema(session) -> None:
    for cql in NEW_TABLES_CQL:
        session.execute(cql, timeout=60.0)


def reset_tables(session) -> None:
    for table in ALL_TABLES:
        session.execute(f"TRUNCATE ravand.{table}", timeout=60.0)


# ---------------------------------------------------------------------------
# گام اول (مرحله ۵): فید ترکیبی
# ---------------------------------------------------------------------------
class HybridFeedService:
    def __init__(self, session, celebrity_authors: Iterable[str]):
        self.session = session
        self.celebrity_authors = frozenset(celebrity_authors)

        self.stmt_insert_author_post = prepare(session, """
            INSERT INTO ravand.posts_by_author
            (author_id, day_bucket, published_at, post_id, content, topic)
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        self.stmt_get_followers = prepare(session,
            "SELECT follower_id FROM ravand.author_followers WHERE author_id = ?")
        self.stmt_insert_user_feed = prepare(session, """
            INSERT INTO ravand.user_feed
            (user_id, day_bucket, published_at, post_id, author_id, content, topic)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """)
        self.stmt_get_follows = prepare(session,
            "SELECT author_id FROM ravand.user_follows WHERE follower_id = ?")
        self.stmt_read_user_feed = prepare(session, """
            SELECT post_id, author_id, published_at, content, topic
            FROM ravand.user_feed
            WHERE user_id = ? AND day_bucket = ?
              AND published_at >= ? AND published_at < ?
            LIMIT ?
        """)
        self.stmt_read_author_posts = prepare(session, """
            SELECT post_id, author_id, published_at, content, topic
            FROM ravand.posts_by_author
            WHERE author_id = ? AND day_bucket = ?
              AND published_at >= ? AND published_at < ?
            LIMIT ?
        """)

    # ---------------- مسیر انتشار ----------------
    def publish(self, post: Post, concurrency: int = 32) -> PublishMetrics:
        published_at = utc(post.published_at)
        day_bucket = published_at.date()

        t0 = time.perf_counter()
        self.session.execute(self.stmt_insert_author_post, (
            post.author_id, day_bucket, published_at, post.post_id, post.content, post.topic))
        t1 = time.perf_counter()

        if post.author_id in self.celebrity_authors:
            return PublishMetrics("celebrity_no_fanout", 0, 1, elapsed_ms(t0, t1), 0.0)

        followers = [r.follower_id for r in self.session.execute(self.stmt_get_followers, (post.author_id,))]
        if followers:
            params = [(f, day_bucket, published_at, post.post_id, post.author_id, post.content, post.topic)
                      for f in followers]
            # raise_on_first_error=True (پیش‌فرض): هر خطا بلافاصله استثنا می‌دهد؛ نوشتن‌ها Idempotent هستند.
            execute_concurrent_with_args(self.session, self.stmt_insert_user_feed, params,
                                         concurrency=concurrency)
        t2 = time.perf_counter()
        return PublishMetrics("normal_fanout_on_write", len(followers), 1 + len(followers),
                              elapsed_ms(t0, t1), elapsed_ms(t1, t2))

    # ---------------- مسیر خواندن ----------------
    @staticmethod
    def _row_to_post(r) -> Post:
        return Post(r.post_id, r.author_id, utc(r.published_at), r.content, r.topic)

    def _read_source(self, stmt, key: str, start: datetime, ref_time: datetime, k: int) -> Tuple[List[Post], int]:
        """حداکثر k ردیف از یک منبع، از جدیدترین باکت به قدیمی‌ترین؛ با توقف زودهنگام پس از پر شدن k."""
        posts: List[Post] = []
        reads = 0
        for bucket in day_buckets_desc(start, ref_time):
            remaining = k - len(posts)
            if remaining <= 0:
                break
            rows = list(self.session.execute(stmt, (key, bucket, start, ref_time, remaining)))
            reads += 1
            posts.extend(self._row_to_post(r) for r in rows)
        return posts, reads

    def get_followed_authors(self, user_id: str) -> List[str]:
        return [r.author_id for r in self.session.execute(self.stmt_get_follows, (user_id,))]

    def get_candidates(self, user_id: str, ref_time: datetime, limit: int = 20) -> CandidateResult:
        """فید ترکیبی: user_feed + پست زنده نویسندگان مشهور دنبال‌شده؛ ادغام، حذف تکرار، مرتب‌سازی قطعی."""
        t0 = time.perf_counter()
        ref_time = utc(ref_time)
        start = ref_time - WINDOW
        sources: List[List[Post]] = []
        failed: List[str] = []
        reads = 0

        # منبع ۱: فید آماده (از هر منبع حداکثر limit ردیف کافی است؛ پرسش ۱ دفترچه)
        try:
            posts, r = self._read_source(self.stmt_read_user_feed, user_id, start, ref_time, limit)
            sources.append(posts)
            reads += r
        except SOURCE_ERRORS:
            failed.append("user_feed")

        # منبع ۲: پست‌های زنده نویسندگان مشهور دنبال‌شده (برای هر نویسنده حداکثر limit ردیف)
        try:
            block_posts: List[Post] = []
            block_reads = 1
            followed = self.get_followed_authors(user_id)
            for author in sorted(a for a in followed if a in self.celebrity_authors):
                posts, r = self._read_source(self.stmt_read_author_posts, author, start, ref_time, limit)
                block_posts.extend(posts)
                block_reads += r
            sources.append(block_posts)
            reads += block_reads
        except SOURCE_ERRORS:
            failed.append("celebrity_posts")

        if len(failed) == 2:
            raise RuntimeError("All feed sources failed; cannot build even a partial feed")

        merged: Dict[str, Post] = {}
        for source in sources:
            for p in source:
                merged.setdefault(p.post_id, p)  # حذف تکرار بر اساس post_id
        final = sort_posts(merged.values())[:limit]
        return CandidateResult(final, bool(failed), failed, reads, elapsed_ms(t0, time.perf_counter()))

    def get_feed_pure_stage3(self, user_id: str, ref_time: datetime, limit: int = 20) -> Tuple[List[Post], int]:
        """خواندن مرحله ۳ خالص: فقط user_feed (برای مقایسه تعداد خواندن)."""
        ref_time = utc(ref_time)
        posts, reads = self._read_source(self.stmt_read_user_feed, user_id, ref_time - WINDOW, ref_time, limit)
        return sort_posts(posts)[:limit], reads

    def get_feed_reference(self, user_id: str, ref_time: datetime, limit: int = 20) -> List[Post]:
        """پاسخ مرجع: همه نویسندگان دنبال‌شده مستقیماً از posts_by_author (منبع حقیقت)."""
        ref_time = utc(ref_time)
        start = ref_time - WINDOW
        merged: Dict[str, Post] = {}
        for author in self.get_followed_authors(user_id):
            posts, _ = self._read_source(self.stmt_read_author_posts, author, start, ref_time, limit)
            for p in posts:
                merged.setdefault(p.post_id, p)
        return sort_posts(merged.values())[:limit]


# ---------------------------------------------------------------------------
# گام دوم (مرحله ۶): شخصی‌سازی
# ---------------------------------------------------------------------------
class PersonalizedFeedService:
    def __init__(self, session, hybrid: HybridFeedService):
        self.session = session
        self.hybrid = hybrid
        self.stmt_get_interests = prepare(session, "SELECT interests FROM ravand.users WHERE user_id = ?")

    def get_interests(self, user_id: str) -> set:
        row = self.session.execute(self.stmt_get_interests, (user_id,)).one()
        # مجموعه خالی در Cassandra به‌صورت null برمی‌گردد.
        return set(row.interests) if row is not None and row.interests else set()

    def build_feed(self, user_id: str, ref_time: datetime, model_version: str = "v1",
                   candidate_limit: int = 100, top_n: int = 20) -> PersonalizedFeed:
        ref_time = utc(ref_time)
        cfg = MODEL_CONFIGS[model_version]

        cand = self.hybrid.get_candidates(user_id, ref_time, limit=candidate_limit)  # زمان تولید کاندیدا

        t0 = time.perf_counter()  # زمان رتبه‌بندی (شامل خواندن علایق کاربر + امتیازدهی + مرتب‌سازی)
        interests = self.get_interests(user_id)
        scored = [ScoredPost(p, score_post(p, interests, ref_time, cfg)) for p in cand.posts]
        scored.sort(key=lambda s: (s.score, s.post.published_at, s.post.post_id), reverse=True)
        ranked = scored[:top_n]
        ranking_ms = elapsed_ms(t0, time.perf_counter())

        return PersonalizedFeed(user_id, model_version, cand.posts, ranked, cand.is_partial,
                                cand.failed_sources, cand.candidate_gen_ms, ranking_ms)


# ---------------------------------------------------------------------------
# ثبت نمایش/کلیک و گزارش CTR در Cassandra
# ---------------------------------------------------------------------------
class InteractionStore:
    def __init__(self, session):
        self.session = session
        self.stmt_insert_impression = prepare(session, """
            INSERT INTO ravand.feed_impressions_by_day
            (day_bucket, model_version, impression_id, user_id, post_id, shown_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        self.stmt_insert_click = prepare(session, """
            INSERT INTO ravand.feed_clicks_by_impression (impression_id, click_id, clicked_at)
            VALUES (?, ?, ?)
        """)
        self.stmt_list_impressions = prepare(session, """
            SELECT impression_id FROM ravand.feed_impressions_by_day
            WHERE day_bucket = ? AND model_version = ?
        """)
        self.stmt_list_clicks = prepare(session,
            "SELECT click_id FROM ravand.feed_clicks_by_impression WHERE impression_id = ?")

    @staticmethod
    def make_impression_id(user_id: str, post_id: str, model_version: str, shown_at: datetime) -> str:
        # شناسه قطعی: ثبت دوباره همان نمایش = همان کلید اصلی = Upsert (نه ردیف جدید)
        return f"{user_id}|{post_id}|{model_version}|{utc(shown_at).strftime('%Y%m%dT%H%M%SZ')}"

    def record_impressions(self, feed: PersonalizedFeed, shown_at: datetime) -> Dict[str, str]:
        shown_at = utc(shown_at)
        ids = {s.post.post_id: self.make_impression_id(feed.user_id, s.post.post_id, feed.model_version, shown_at)
               for s in feed.ranked}
        params = [(shown_at.date(), feed.model_version, imp_id, feed.user_id, post_id, shown_at)
                  for post_id, imp_id in ids.items()]
        if params:
            execute_concurrent_with_args(self.session, self.stmt_insert_impression, params, concurrency=16)
        return ids

    def record_click(self, impression_id: str, click_id: str, clicked_at: datetime) -> None:
        self.session.execute(self.stmt_insert_click, (impression_id, click_id, utc(clicked_at)))

    def ctr_report(self, combos: List[Tuple[date, str]]) -> List[CtrRow]:
        """CTR = (تعداد impression_id متمایز با حداقل یک کلیک) / (تعداد impression_id متمایز) به تفکیک (روز، نسخه مدل)."""
        rows: List[CtrRow] = []
        for day, model_version in combos:
            imp_ids = sorted({r.impression_id for r in
                              self.session.execute(self.stmt_list_impressions, (day, model_version))})
            clicked = click_rows = 0
            if imp_ids:
                results = execute_concurrent_with_args(
                    self.session, self.stmt_list_clicks, [(i,) for i in imp_ids], concurrency=32)
                for _success, rs in results:
                    n = len(list(rs))
                    click_rows += n
                    clicked += 1 if n > 0 else 0
            rows.append(CtrRow(day, model_version, len(imp_ids), clicked, click_rows))
        return rows


# ---------------------------------------------------------------------------
# داده آزمون
# ---------------------------------------------------------------------------
def T(day: int, hour: int, minute: int = 0, second: int = 0, month: int = 5) -> datetime:
    return datetime(2026, month, day, hour, minute, second, tzinfo=timezone.utc)


MAIN_USERS = {"sara": {"tech"}, "amin": {"travel"}}
MAIN_FOLLOWS = ["author_1", "author_2", "author_quiet", "author_empty", "celeb_1"]

# (post_id, author, published_at, topic) - زمان مرجع ۲۰۲۶-۰۵-۱۰ ۱۲:۰۰ UTC ؛ بازه مجاز [۰۵-۰۸ ۱۲:۰۰ ، ۰۵-۱۰ ۱۲:۰۰)
MAIN_POSTS = [
    ("post_200", "author_1",          T(8, 11, 59, 59), "tech"),    # ۱ ثانیه قبل از شروع بازه -> خارج
    ("post_201", "author_2",          T(8, 12, 0, 0),   "travel"),  # دقیقاً مرز شروع (شامل)
    ("post_202", "author_quiet",      T(8, 14, 30, 0),  "tech"),    # تنها پست نویسنده کم‌فعالیت -> نباید حذف شود
    ("post_203", "author_unfollowed", T(9, 10, 0, 0),   "tech"),    # نویسنده دنبال‌نشده -> نباید وارد شود
    ("post_204", "author_1",          T(9, 15, 0, 0),   "tech"),    # تساوی زمان (۱ از ۳)
    ("post_205", "author_2",          T(9, 15, 0, 0),   "travel"),  # تساوی زمان (۲ از ۳)
    ("post_206", "celeb_1",           T(9, 15, 0, 0),   "travel"),  # تساوی زمان (۳ از ۳؛ از مسیر مشهور)
    ("post_207", "celeb_1",           T(8, 20, 0, 0),   "tech"),    # پست مشترک در هر دو منبع (پیش از مشهور شدن منتشر شده)
    ("post_208", "celeb_1",           T(10, 9, 0, 0),   "travel"),
    ("post_209", "author_1",          T(10, 11, 30, 0), "tech"),
    ("post_210", "celeb_1",           T(10, 12, 0, 0),  "tech"),    # دقیقاً مرز پایان (غیرشامل)
    ("post_211", "author_1",          T(10, 12, 0, 0),  "tech"),    # دقیقاً مرز پایان (غیرشامل)
    ("post_212", "author_1",          T(7, 12, 0, 0),   "tech"),    # ۷۲ ساعت قبل؛ موضوع منطبق اما خارج از بازه
]

EXPECTED_FEED_IDS = ["post_209", "post_208", "post_206", "post_205", "post_204", "post_207", "post_202", "post_201"]
EXPECTED_RANK = {
    "sara": ["post_209", "post_204", "post_207", "post_202", "post_208", "post_206", "post_205", "post_201"],
    "amin": ["post_208", "post_206", "post_205", "post_201", "post_209", "post_204", "post_207", "post_202"],
}
FORBIDDEN_IDS = {"post_200", "post_203", "post_210", "post_211", "post_212"}


def seed_main_dataset(session) -> HybridFeedService:
    stmt_user = prepare(session, "INSERT INTO ravand.users (user_id, interests) VALUES (?, ?)")
    stmt_follow = prepare(session, "INSERT INTO ravand.user_follows (follower_id, author_id) VALUES (?, ?)")
    stmt_followers = prepare(session, "INSERT INTO ravand.author_followers (author_id, follower_id) VALUES (?, ?)")

    for user_id, interests in MAIN_USERS.items():
        session.execute(stmt_user, (user_id, interests))
        for author in MAIN_FOLLOWS:
            session.execute(stmt_follow, (user_id, author))
            session.execute(stmt_followers, (author, user_id))
    session.execute(stmt_follow, ("user_other", "author_unfollowed"))
    session.execute(stmt_followers, ("author_unfollowed", "user_other"))

    service = HybridFeedService(session, CELEBRITY_AUTHORS)
    before_fame = HybridFeedService(session, ())  # celeb_1 هنگام انتشار post_207 هنوز «عادی» بود
    for post_id, author, ts, topic in MAIN_POSTS:
        post = Post(post_id, author, ts, f"Content {post_id}", topic)
        (before_fame if post_id == "post_207" else service).publish(post)
    return service


def seed_bulk_dataset(service: HybridFeedService) -> None:
    """آزمون سقف ۱۰۰: ۱۲۰ پست نویسنده مشهور و ۶۰ پست نویسنده عادی در بازه ۴۸ ساعته."""
    s = service.session
    for stmt_cql, params in (
        ("INSERT INTO ravand.user_follows (follower_id, author_id) VALUES (?, ?)", ("bulk_user", "celeb_bulk")),
        ("INSERT INTO ravand.user_follows (follower_id, author_id) VALUES (?, ?)", ("bulk_user", "normal_bulk")),
        ("INSERT INTO ravand.author_followers (author_id, follower_id) VALUES (?, ?)", ("normal_bulk", "bulk_user")),
        ("INSERT INTO ravand.author_followers (author_id, follower_id) VALUES (?, ?)", ("celeb_bulk", "bulk_user")),
    ):
        s.execute(prepare(s, stmt_cql), params)
    for i in range(120):
        ts = REF_TIME - timedelta(minutes=(i + 1) * 10)
        service.publish(Post(f"bulk_c_{i:03d}", "celeb_bulk", ts, "c", "tech"))
    for i in range(60):
        ts = REF_TIME - timedelta(minutes=i * 15 + 5)
        service.publish(Post(f"bulk_n_{i:03d}", "normal_bulk", ts, "n", "tech"))


# ---------------------------------------------------------------------------
# چاپ
# ---------------------------------------------------------------------------
def banner(title: str) -> None:
    print("\n" + "=" * 100 + f"\n{title}\n" + "=" * 100)


def ids(posts: Iterable[Post]) -> List[str]:
    return [p.post_id for p in posts]


def print_posts(posts: List[Post]) -> None:
    for p in posts:
        print(f"  {p.post_id}  {p.author_id:<14} {p.published_at:%Y-%m-%d %H:%M:%S}  {p.topic}")


def print_scored(scored: List[ScoredPost]) -> None:
    for i, s in enumerate(scored, 1):
        p = s.post
        print(f"  #{i:<2} {p.post_id}  score={s.score:<8}  {p.author_id:<14} {p.published_at:%m-%d %H:%M:%S}  {p.topic}")


# ---------------------------------------------------------------------------
# اجرای تحویل‌دادنی‌ها
# ---------------------------------------------------------------------------
def run_deliverables_1_2(service: HybridFeedService) -> None:
    banner("Deliverable 1 & 2: hybrid read (merge + dedup), low-activity author, deterministic tie-break")
    start = REF_TIME - WINDOW

    src_feed, _ = service._read_source(service.stmt_read_user_feed, "sara", start, REF_TIME, 20)
    src_celeb, _ = service._read_source(service.stmt_read_author_posts, "celeb_1", start, REF_TIME, 20)
    print("Source A (user_feed of sara)      :", ids(sort_posts(src_feed)))
    print("Source B (posts_by_author celeb_1):", ids(sort_posts(src_celeb)))
    assert "post_207" in ids(src_feed) and "post_207" in ids(src_celeb), "shared post must exist in both sources"

    result = service.get_candidates("sara", REF_TIME, limit=20)
    print("\nHybrid feed for sara (limit=20):")
    print_posts(result.posts)
    reference = service.get_feed_reference("sara", REF_TIME, limit=20)

    assert ids(result.posts) == EXPECTED_FEED_IDS, f"unexpected hybrid feed: {ids(result.posts)}"
    assert ids(result.posts) == ids(reference), "hybrid feed != reference answer"
    assert len(ids(result.posts)) == len(set(ids(result.posts))), "duplicate post_id in feed"
    assert ids(result.posts).count("post_207") == 1, "shared post must appear once"
    assert "post_202" in ids(result.posts), "low-activity author post was dropped"
    tie = [i for i in ids(result.posts) if i in {"post_204", "post_205", "post_206"}]
    assert tie == ["post_206", "post_205", "post_204"], f"tie-break failed: {tie}"
    assert not (FORBIDDEN_IDS & set(ids(result.posts))), "out-of-window / unfollowed post leaked"
    print("\nOK: equals reference | shared post once | low-activity author kept | ties by post_id DESC | boundaries [start,end)")

    # آزمون سقف خواندن از هر منبع (پرسش ۱): کاربر bulk_user
    for limit in (20, 100):
        hybrid = service.get_candidates("bulk_user", REF_TIME, limit=limit)
        ref = service.get_feed_reference("bulk_user", REF_TIME, limit=limit)
        assert ids(hybrid.posts) == ids(ref), f"bulk mismatch at limit={limit}"
        assert len(hybrid.posts) == limit
        print(f"OK: bulk_user limit={limit}: {len(hybrid.posts)} posts == reference (DB reads={hybrid.db_reads})")

    # سیاست خطای منبع (پرسش ۶): قطع منبع پست‌های مشهور
    original = service._read_source

    def flaky(stmt, key, *args, **kwargs):
        if key == "celeb_1":
            raise DriverException("simulated outage of posts_by_author read")
        return original(stmt, key, *args, **kwargs)

    service._read_source = flaky
    try:
        partial = service.get_candidates("sara", REF_TIME, limit=20)
    finally:
        service._read_source = original
    assert partial.is_partial and partial.failed_sources == ["celebrity_posts"]
    assert ids(partial.posts) == ["post_209", "post_205", "post_204", "post_207", "post_202", "post_201"]
    print(f"OK: source failure -> is_partial={partial.is_partial}, failed_sources={partial.failed_sources}, "
          f"returned={ids(partial.posts)}")


def run_deliverables_5_6_7(personal: PersonalizedFeedService, hybrid: HybridFeedService) -> Dict[str, PersonalizedFeed]:
    banner("Deliverable 5, 6, 7: personalization (sara=tech vs amin=travel), safety asserts, timing, repeatability")
    feeds: Dict[str, PersonalizedFeed] = {}
    for user_id in ("sara", "amin"):
        feed = personal.build_feed(user_id, REF_TIME, "v1")
        feeds[user_id] = feed
        print(f"\n--- {user_id} | interests={sorted(personal.get_interests(user_id))} | model={feed.model_version} ---")
        print(f"Candidates ({len(feed.candidates)}):")
        print_posts(feed.candidates)
        print("Final feed (score DESC, published_at DESC, post_id DESC):")
        print_scored(feed.ranked)

        # تحویل ۶: عدم نفوذ پست نامجاز
        followed = set(hybrid.get_followed_authors(user_id))
        for p in feed.candidates:
            assert p.author_id in followed, f"candidate from unfollowed author: {p.post_id}"
            assert REF_TIME - WINDOW <= p.published_at < REF_TIME, f"candidate outside window: {p.post_id}"
        assert len(feed.candidates) <= 100 and len(feed.ranked) <= 20
        assert {s.post.post_id for s in feed.ranked} <= {p.post_id for p in feed.candidates}
        assert not (FORBIDDEN_IDS & {s.post.post_id for s in feed.ranked}), "forbidden post in final feed"
        assert ids(s.post for s in feed.ranked) == EXPECTED_RANK[user_id], f"unexpected rank for {user_id}"
        print("OK: all candidates are followed+in-window; forbidden posts absent; order as expected")

        # تحویل ۷: تکرارپذیری
        signature = [(s.post.post_id, s.score) for s in feed.ranked]
        for _ in range(3):
            again = personal.build_feed(user_id, REF_TIME, "v1")
            assert [(s.post.post_id, s.score) for s in again.ranked] == signature, "non-deterministic ranking"
        print(f"Timing: candidate_generation={feed.candidate_gen_ms:.2f} ms | ranking={feed.ranking_ms:.2f} ms")
        print("OK: 3 re-runs with same input+model_version gave identical order and scores")

    s_ids, a_ids = EXPECTED_RANK["sara"], EXPECTED_RANK["amin"]
    assert s_ids != a_ids and set(s_ids) == set(a_ids)
    print("\nSame candidate set, different order for sara and amin: OK")
    return feeds


def run_deliverable_8(personal: PersonalizedFeedService, store: InteractionStore) -> None:
    banner("Deliverable 8: impressions/clicks in Cassandra and CTR per (day, model_version)")
    day1, day2 = REF_TIME, REF_TIME + timedelta(days=1)
    clock = REF_TIME + timedelta(seconds=30)

    # اجرای A: v1 در روز ۱۰ مه
    feed_sara = personal.build_feed("sara", day1, "v1")
    feed_amin = personal.build_feed("amin", day1, "v1")
    sara_ids = store.record_impressions(feed_sara, day1)
    store.record_impressions(feed_sara, day1)  # نمایش تکراری (همان رویداد دوباره ثبت می‌شود)
    amin_ids = store.record_impressions(feed_amin, day1)
    store.record_click(sara_ids["post_209"], "click-1", clock)
    store.record_click(sara_ids["post_209"], "click-1", clock)                          # ثبت تکراری همان کلیک
    store.record_click(sara_ids["post_209"], "click-2", clock + timedelta(seconds=5))   # کلیک دوباره روی همان نمایش
    store.record_click(sara_ids["post_204"], "click-3", clock)
    store.record_click(amin_ids["post_208"], "click-4", clock)

    # اجرای B: v2 در همان روز
    feed_sara_v2 = personal.build_feed("sara", day1, "v2")
    feed_amin_v2 = personal.build_feed("amin", day1, "v2")
    sara_v2 = store.record_impressions(feed_sara_v2, day1)
    store.record_impressions(feed_amin_v2, day1)
    store.record_click(sara_v2["post_209"], "click-5", clock)

    # اجرای C: v1 در روز ۱۱ مه (پنجره ۴۸ ساعته متفاوت)
    feed_sara_d2 = personal.build_feed("sara", day2, "v1")
    sara_d2 = store.record_impressions(feed_sara_d2, day2)
    store.record_click(sara_d2["post_210"], "click-6", day2 + timedelta(seconds=30))

    combos = [(day1.date(), "v1"), (day1.date(), "v2"), (day2.date(), "v1")]
    rows = store.ctr_report(combos)
    print(f"{'day_bucket':<12} | {'model':<5} | {'distinct impressions':<20} | {'clicked impressions':<19} | {'raw click rows':<14} | CTR")
    print("-" * 95)
    for r in rows:
        print(f"{r.day.isoformat():<12} | {r.model_version:<5} | {r.impressions:<20} | {r.clicked_impressions:<19} | "
              f"{r.click_rows:<14} | {r.ctr:.6f}")
    by_key = {(r.day, r.model_version): r for r in rows}
    a = by_key[(day1.date(), "v1")]
    assert (a.impressions, a.clicked_impressions, a.click_rows) == (16, 3, 4)
    b = by_key[(day1.date(), "v2")]
    assert (b.impressions, b.clicked_impressions) == (16, 1)
    c = by_key[(day2.date(), "v1")]
    assert (c.impressions, c.clicked_impressions) == (7, 1)
    print(f"\nNaive CTR (raw click rows / impressions) for 2026-05-10 v1 = {a.click_rows}/{a.impressions} = "
          f"{a.click_rows / a.impressions:.4f}  (WRONG)  vs  correct = {a.clicked_impressions}/{a.impressions} = {a.ctr:.4f}")
    print("OK: duplicate impressions and repeated clicks do not inflate CTR")


def run_deliverable_3(session) -> None:
    banner("Deliverable 3: logical writes at publish + DB reads at feed open (hybrid vs pure stage 3)")
    followers = 10_000
    stmt_followers = prepare(session, "INSERT INTO ravand.author_followers (author_id, follower_id) VALUES (?, ?)")
    execute_concurrent_with_args(
        session, stmt_followers, [("celeb_bench", f"bench_user_{i:05d}") for i in range(1, followers + 1)],
        concurrency=64)
    session.execute(prepare(session, "INSERT INTO ravand.user_follows (follower_id, author_id) VALUES (?, ?)"),
                    ("bench_user_00001", "celeb_bench"))

    hybrid = HybridFeedService(session, CELEBRITY_AUTHORS)
    pure = HybridFeedService(session, ())  # مجموعه مشهور خالی = دقیقاً مرحله ۳ خالص
    m_h = hybrid.publish(Post("bench_post_hybrid", "celeb_bench", T(10, 11, 0), "bench", "news"))
    m_p = pure.publish(Post("bench_post_pure", "celeb_bench", T(10, 11, 10), "bench", "news"))

    feed_h = hybrid.get_candidates("bench_user_00001", REF_TIME, limit=20)
    _, reads_pure = pure.get_feed_pure_stage3("bench_user_00001", REF_TIME, limit=20)

    print(f"{'Method':<18} | {'Followers':<9} | {'Logical writes/publish':<22} | {'Accept(ms)':<10} | {'Fan-out(ms)':<11} | DB reads/feed open")
    print("-" * 105)
    print(f"{'Pure stage 3':<18} | {followers:<9} | {m_p.logical_writes:<22} | {m_p.accept_ms:<10.2f} | {m_p.fanout_ms:<11.2f} | {reads_pure}")
    print(f"{'Hybrid':<18} | {followers:<9} | {m_h.logical_writes:<22} | {m_h.accept_ms:<10.2f} | {m_h.fanout_ms:<11.2f} | {feed_h.db_reads}")
    assert m_p.logical_writes == 1 + followers and m_h.logical_writes == 1
    assert feed_h.db_reads == 1 + 3 + 3 and reads_pure == 3
    assert {"bench_post_hybrid", "bench_post_pure"} <= set(ids(feed_h.posts))
    print("Hybrid feed contains both posts (bench_post_hybrid exists only in posts_by_author): OK")


def main() -> None:
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    try:
        ensure_schema(session)
        reset_tables(session)

        service = seed_main_dataset(session)
        seed_bulk_dataset(service)
        personal = PersonalizedFeedService(session, service)
        store = InteractionStore(session)

        run_deliverables_1_2(service)
        run_deliverables_5_6_7(personal, service)
        run_deliverable_8(personal, store)
        run_deliverable_3(session)
        banner("ALL CHECKS PASSED")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
