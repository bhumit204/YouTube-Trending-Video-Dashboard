SELECT 
	category, 
	sum(view_count) AS view_count, 
	sum(likes) AS likes, 
	sum(dislikes) AS dislikes, 
	sum(comment_count) AS comment_count,
FROM {{ ref('YT_data') }}
group by category