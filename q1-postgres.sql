-- مدل و ایندکس منصفانه در PostgreSQL برای مرحله اول
CREATE TABLE user_follows (
    follower_id text NOT NULL,
    author_id text NOT NULL,
    PRIMARY KEY (follower_id, author_id)
);

CREATE TABLE posts (
    post_id text PRIMARY KEY,
    author_id text NOT NULL,
    published_at timestamptz NOT NULL,
    content text NOT NULL,
    topic text NOT NULL
);

-- ایندکس ترکیبی منطبق با الگوی دسترسی و مرتب‌سازی
CREATE INDEX idx_posts_author_time_id 
ON posts (author_id, published_at DESC, post_id DESC);

-- پرس‌وجوی مرحله اول در PostgreSQL
SELECT p.post_id, p.author_id, p.published_at, p.content, p.topic
FROM user_follows f
JOIN posts p ON f.author_id = p.author_id
WHERE f.follower_id = 'sara'
  AND p.published_at >= '2026-05-08 12:00:00+00'
  AND p.published_at <  '2026-05-10 12:00:00+00'
ORDER BY p.published_at DESC, p.post_id DESC
LIMIT 20;