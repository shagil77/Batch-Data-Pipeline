# Batch-Data-Pipeline

<img width="881" alt="Screenshot 2024-08-19 at 11 49 55 PM" src="https://github.com/user-attachments/assets/ad5544fb-128d-48d1-a446-e634ffd7cdb0">

This batch data pipeline collects user data and creates a user profile. We are building a data pipeline to populate the user_behavior_metric table. The user_behavior_metric table is an OLAP table used by analysts, dashboard software, etc. It is built from:

1. user_purchase: OLTP table with user purchase information.
2. movie_review.csv: Data is sent every day by an external data vendor.
