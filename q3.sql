USE ravand;

-- ۱. جدول کمکی دنبال‌کنندگان هر نویسنده (فهرست گیرندگان آماده هنگام انتشار)
CREATE TABLE IF NOT EXISTS author_followers (
    author_id text,
    follower_id text,
    PRIMARY KEY ((author_id), follower_id)
);

-- ۲. جدول فید ازپیش‌آماده‌شده برای هر کاربر (Fan-out on Write)
-- استفاده از day_bucket برای جلوگیری از رشد بی‌حد پارتیشن فید کاربر در طول زمان
CREATE TABLE IF NOT EXISTS user_feed (
    user_id text,
    day_bucket date,
    published_at timestamp,
    post_id text,
    author_id text,
    content text,
    topic text,
    PRIMARY KEY ((user_id, day_bucket), published_at, post_id)
) WITH CLUSTERING ORDER BY (published_at DESC, post_id DESC);