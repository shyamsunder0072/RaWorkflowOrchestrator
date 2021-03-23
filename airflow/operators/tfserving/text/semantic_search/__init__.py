from airflow.operators.tfserving.text.semantic_search.scripts import index_building, similarity_matching

index_util = index_building.IndexUtil()
run_index_builder = index_util.build_index

match_util = similarity_matching.MatchingUtil()
load_annoy_index = match_util.load_annoy_index
run_similarity_matching = match_util.find_similar_items