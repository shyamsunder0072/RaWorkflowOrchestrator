import sys

from importlib import import_module


def map_tfserving_model(model_name):
    root_package = "airflow.operators.tfserving."
    module_imports = {
        "ACTION_RECOGNITION": [root_package + "video.action_recognition", "run_action_recognition"],
        "AUDIO_CLASSIFICATION": [root_package + "audio.audio_classification", "run_audio_classification"],
        "AUDIO_EMBEDDINGS_EXTRACTION": [root_package + "audio.audio_embeddings_extraction",
                                        "run_audio_embeddings_extraction"],
        'FACE_EMBEDDINGS_EXTRACTION': [root_package + "image.face_embeddings_extraction",
                                       "run_face_embeddings_extraction"],
        'FACE_KEY_POINTS_DETECTION': [root_package + "image.face_key_points_detection",
                                      "run_face_key_points_detection"],
        "IMAGE_CLASSIFICATION": [root_package + "image.image_classification", "run_image_classification"],
        "IMAGE_FEATURE_EXTRACTION": [root_package + "image.feature_extraction", "run_feature_extraction"],
        "OBJECT_DETECTION": [root_package + "image.object_detection", "run_object_detection"],
        "OPTICAL_CHARACTER_RECOGNITION": [root_package + "image.optical_character_recognition",
                                          "run_optical_character_recognition"],
        "POSE_DETECTION": [root_package + "image.pose_detection", "run_pose_detection"],
        "SEMANTIC_SEGMENTATION": [root_package + "image.semantic_segmentation", "run_semantic_segmentation"],
        "SOURCE_SEPARATION": [root_package + "audio.source_separation", "run_source_separation"],
        "SPEECH_TO_TEXT": [root_package + "audio.speech_to_text", "run_speech_to_text"],
        "TEXT_BOXES_DETECTION": [root_package + "image.text_boxes_detection", "run_text_boxes_detection"],
        "TEXT_EMBEDDINGS_EXTRACTION": [root_package + "text.text_embeddings_extraction",
                                       "run_text_embeddings_extraction"],
        "COLLAB_FILTERING_RECOMMENDATION": [root_package + "interaction.collaborative_filtering", "run_recommendation"]
    }
    models_list = list(module_imports.keys())
    if model_name in models_list:
        module_imports = module_imports.get(model_name)
    else:
        print("ModelNotFoundError: \'{}\' No such model found. \nList of models available : \n{}".format(
            model_name, models_list
        ))
        sys.exit(1)

    """get the method object."""
    return getattr(import_module(module_imports[0]), module_imports[1])


def map_compute_fn(fn_name):
    root_package = "airflow.operators.tfserving."
    module_imports = {
        "VIDEO_SCENE_DETECTION": [root_package + "video.video_scene_detection", "run_video_scenes_detection"],
        "RGB_STREAMS_COMPUTATION": [root_package + "video.rgb_streams_computation", "run_rgb_streams_computation"],
        "VIDEO_SYNOPSIS_GENERATOR": [root_package + "video.video_synopsis_generation", "run_video_synopsis_generation"],
        "KEY_FRAMES_EXTRACTION_HIST": [root_package + "video.key_frames_extraction", "run_frames_extraction_using_hist"],
        "KEY_FRAMES_EXTRACTION_FPS": [root_package + "video.key_frames_extraction", "run_frames_extraction_using_fps"],
        "AUDIO_SCENE_DETECTION": [root_package + "audio.audio_scene_detection", "run_audio_scene_detection"],
        "FRAME_TEXT_REDUCTION": [root_package + "image.frames_text_reduction", "run_frame_text_reduction"],
        "COLOR_HISTOGRAM_COMPUTATION": [root_package + "image.color_histogram", "run_color_histogram_feature_extraction"]
    }
    modules_list = list(module_imports.keys())
    if fn_name in modules_list:
        module_imports = module_imports.get(fn_name)
    else:
        print("ModuleNotFoundError: \'{}\' No such module found. \nList of modules available : \n{}".format(
            fn_name, modules_list
        ))
        sys.exit(1)

    """get the method object."""
    return getattr(import_module(module_imports[0]), module_imports[1])