import librosa.display
import os
import numpy as np
from glob import glob

genres = ["blues", "classical", "country", "disco", "hiphop", "jazz", "metal", "pop", "reggae", "rock"]


class AudioFeatures(object):
    def __init__(self):
        return

    @staticmethod
    def extract_waveform_features(sample_rate, sample_duration):
        spectrogram_pairs = dict()
        for genre in genres:
            print("genre: ", genre)
            for file in glob('/Users/dimplebansal/Downloads/genres/' + genre + '/*.wav'):
                # print(file)
                for i in np.arange(5):
                    # print(sample_duration * i + 1)
                    x, sr = librosa.load(file, sr=sample_rate, offset=sample_duration * i + 1, duration=sample_duration)
                    spectrogram_pairs[os.path.basename(file) + "_" + str(i)] = x
        print(len(spectrogram_pairs))
        return spectrogram_pairs

    @staticmethod
    def extract_spectrogram_images(sample_rate, sample_duration, n_mels, hop_size):
        spectrogram_pairs = dict()
        for genre in genres:
            print("genre: ", genre)
            for file in glob('/Users/dimplebansal/Downloads/genres/' + genre + '/*.wav'):
                # print(file)
                for i in np.arange(5):
                    # print(sample_duration * i + 1)
                    x, sr = librosa.load(file, sr=sample_rate, offset=sample_duration * i + 1, duration=sample_duration)
                    # print(x.shape)
                    S = librosa.feature.melspectrogram(y=x, sr=sr, n_mels=n_mels, hop_length=hop_size, fmax=8000)
                    S_dB = librosa.power_to_db(S, ref=np.max)
                    # print(S_dB.shape)
                    spectrogram_pairs[os.path.basename(file) + "_" + str(i)] = S_dB
        print(len(spectrogram_pairs))
        return spectrogram_pairs
