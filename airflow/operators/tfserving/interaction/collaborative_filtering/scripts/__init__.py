from airflow.operators.tfserving.interaction.collaborative_filtering.scripts.collab_filtering_module import CollabFilteringRecommendationModule

run_recommendation = CollabFilteringRecommendationModule().run_recommendation
