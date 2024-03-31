{{ config(
    materialized="table",
    partition_by={
      "field": "trending_date",
      "data_type": "date"
    },
    cluster_by = ["category"]
) }}

SELECT 
	video_id, 
	max(title) AS title, 
	max(view_count) AS view_count, 
	max(likes) AS likes, 
	max(dislikes) AS dislikes, 
	max(comment_count) AS comment_count, 
	max(category) AS category, 
	max(trending_date) AS trending_date
FROM `YT_Trending_data_all.YT_external_table_data`
group by video_id