CREATE TABLE tweet (
	created_at VARCHAR,
	id_str VARCHAR PRIMARY KEY,
	text TEXT,
	source VARCHAR,
	truncated BOOLEAN,
	in_reply_to_status_id_str VARCHAR,
	in_reply_to_user_id_str VARCHAR,
	in_reply_to_screen_name	VARCHAR,
	user user,
	coordinates coordinates,
	place place,
	quoted_status_id_str VARCHAR,
	is_quote_status	BOOLEAN,
	quoted_status tweet,
	retweeted_status tweet,
	quote_count INT,
	reply_count INT,
	retweet_count INT,	
	favorite_count INT,
	entities entities,
	favorited BOOLEAN,
	retweeted BOOLEAN,
	possibly_sensitive BOOLEAN,
	filter_level VARCHAR,
	lang VARCHAR,
	matching_rules Array of Rule Objects
);

CREATE TABLE user (
	id_sttr,
	name,
	screen_name,
	location,
	derived,
	urk,
	description,
	protected,
	verified,
	followers_count,
	friends_count,
	listed_count,
	favourites_count,
	statuses_count,
	created_at,
	profile_banner_url,
	profile_image_url_https,
	default_profile,
	default_profile_image,
	withheld_in_countries,
	withheld_scope
);

CREATE TABLE place (
	id,
	url,
	place_type,
	name,
	full_name,
	country_code,
	country,
	bounding_box,
	attributes
);

CREATE TABLE bounding_box (
	coordinates,
	type
);

CREATE TABLE coordinates (
	coordinates,
	type	
);

CREATE TABLE entities (
	hashtags,
	media,
	urls,
	user_mentions,
	symbols,
	polls
);

CREATE TABLE hashtag (
	indices,
	text
);

CREATE TABLE media (
	display_url,
	expanded_url,
	id_str,
	indices,
	media_url,
	media_url_https,
	sizes,
	source_status_id_str
	type,
	url
);

CREATE TABLE sizes (
	thumb,
	large,
	medium,
	small
);

CREATE TABLE size (
	w,
	h,
	resize
);

CREATE TABLE url (
	display_url,
	expanded_url,
	induces,
	url
);

CREATE TABLE user_mentions (
	id_str,
	indices,
	name,
	screen_name	
);

CREATE TABLE symbols (
	indices,
	text
);

CREATE TABLE poll (
	options,
	end_datetime,
	duration_minutes
);