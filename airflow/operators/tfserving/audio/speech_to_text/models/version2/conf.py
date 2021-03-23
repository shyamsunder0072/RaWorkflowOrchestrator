# from pkg_resources import resource_filename, Requirement

# language models files
# alphabet_path = resource_filename(Requirement.parse("commons==0.0.1"), "commons/audio/speech_to_text/models/version2/alphabet.txt")
stt_dir = '/usr/local/couture/trained-models/other-models/language_models/'
alphabet_path = stt_dir + 'alphabet.txt'
lm_binary_path = stt_dir + 'lm.binary'
lm_trie_path = stt_dir + 'trie'

# speech to text model parameters
lm_alpha = 0.75  # Language model weight
lm_beta = 1.85  # Word insertion bonus

beam_width = 1024 # Beam width for the CTC decoder
cutoff_prob = 1.0
cutoff_top_n = 300
n_context = 9
n_input = 26
n_cell_dim = 2048
audio_sample_rate = 16000
feature_win_len = 32
feature_win_step = 20
audio_window_samples = audio_sample_rate * (feature_win_len / 1000)
audio_step_samples = audio_sample_rate * (feature_win_step / 1000)

# tf serving model information
MODEL_NAME = "speech_to_text"
MODEL_VERSION = 1

# enable config for gpu options
# session_config = tfv1.ConfigProto(allow_soft_placement=True, log_device_placement=False,
#                                   inter_op_parallelism_threads=0,
#                                   intra_op_parallelism_threads=0,
#                                   gpu_options=tfv1.GPUOptions(allow_growth=False))
