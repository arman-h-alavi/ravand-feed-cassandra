CREATE KEYSPACE IF NOT EXISTS ravand
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
};

USE ravand;

-- ۱. جدول اطلاعات کاربران و علایق (آماده برای مراحل بعد)
CREATE TABLE IF NOT EXISTS users (
    user_id text PRIMARY KEY,
    interests set<text>
);

-- ۲. جدول رابطه دنبال‌کردن (مسیر خواندن مرحله اول: کاربر چه کسانی را دنبال می‌کند؟)
CREATE TABLE IF NOT EXISTS user_follows (
    follower_id text,
    author_id text,
    PRIMARY KEY ((follower_id), author_id)
);

-- ۳. جدول پست‌ها به تفکیک نویسنده و روز
CREATE TABLE IF NOT EXISTS posts_by_author (
    author_id text,
    day_bucket date,
    published_at timestamp,
    post_id text,
    content text,
    topic text,
    PRIMARY KEY ((author_id, day_bucket), published_at, post_id)
) WITH CLUSTERING ORDER BY (published_at DESC, post_id DESC);