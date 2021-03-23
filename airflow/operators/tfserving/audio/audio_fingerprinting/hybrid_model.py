import librosa.display
import os
import numpy as np
from glob import glob
import tensorflow as tf

genres = ["blues", "classical", "country", "disco", "hiphop", "jazz", "metal", "pop", "reggae", "rock"]


def get_one_hot(label_num, num_classes):
    one_hot = np.zeros((1, num_classes))
    one_hot[0, int(label_num)] = 1
    return one_hot


def extract_spectrogram_features(sample_rate, sample_duration, num_classes, down_sampling_ratio):
    training_spectrogram_pairs = dict()
    test_spectrogram_pairs = dict()
    training_genre_label = dict()
    test_genre_label = dict()
    for index, genre in enumerate(genres):
        # print("genre: ", genre)
        for file in glob('/Users/dimplebansal/Downloads/genres/' + genre + '/*.wav'):
            # print(file)
            for i in np.arange(5):
                x, sr = librosa.load(file, sr=sample_rate, offset=sample_duration * i + 1, duration=sample_duration)
                x = x.reshape(int(len(x) / down_sampling_ratio), down_sampling_ratio)
                x = np.mean(x, axis=1)
                if i < 4:
                    training_spectrogram_pairs[os.path.basename(file) + "_" + str(i)] = x
                    training_genre_label[os.path.basename(file) + "_" + str(i)] = get_one_hot(index, num_classes)
                else:
                    test_spectrogram_pairs[os.path.basename(file) + "_" + str(i)] = x
                    test_genre_label[os.path.basename(file) + "_" + str(i)] = get_one_hot(index, num_classes)

    print('len(training_spectrogram_pairs):', len(training_spectrogram_pairs))
    print('len(training_genre_label):', len(training_genre_label))
    print('len(test_spectrogram_pairs):', len(test_spectrogram_pairs))
    print('len(test_genre_label):', len(test_genre_label))
    return training_spectrogram_pairs, training_genre_label, test_spectrogram_pairs, test_genre_label


sample_rate = 22050  # 8000
sample_duration = 5
batch_size = 128
epochs = 1000
sample_outputs = 10
down_sampling_ratio = 42
gama = 0.9
inp_dim = int(sample_duration * sample_rate / down_sampling_ratio)
num_classes = len(genres)

training_spectrogram_pairs, training_genre_label, test_spectrogram_pairs, test_genre_label = \
    extract_spectrogram_features(sample_rate=sample_rate,
                                 sample_duration=sample_duration,
                                 num_classes=num_classes,
                                 down_sampling_ratio=down_sampling_ratio)

training_features = np.asarray([*training_spectrogram_pairs.values()])
one_hot_labels = np.asarray([*training_genre_label.values()])
one_hot_labels = one_hot_labels.reshape((one_hot_labels.shape[0], one_hot_labels.shape[2]))
testing_features = np.asarray([*test_spectrogram_pairs.values()])
test_one_hot_labels = np.asarray([*test_genre_label.values()])
test_one_hot_labels = test_one_hot_labels.reshape((test_one_hot_labels.shape[0], test_one_hot_labels.shape[2]))

# Encoder
inp = tf.keras.Input(shape=(inp_dim,))
e1 = tf.keras.layers.Dense(1024, activation="relu")(inp)
dr1 = tf.keras.layers.Dropout(0.4)(e1)
e2 = tf.keras.layers.Dense(512, activation="relu")(dr1)
dr2 = tf.keras.layers.Dropout(0.4)(e2)
e3 = tf.keras.layers.Dense(256, activation="tanh")(dr2)
dr3 = tf.keras.layers.Dropout(0.4)(e3)
encoded = tf.keras.layers.Dense(128, activation="tanh")(dr3)

# Classification
c1 = tf.keras.layers.Dense(64, activation='relu')(encoded)
dr4 = tf.keras.layers.Dropout(0.4)(c1)
c2 = tf.keras.layers.Dense(32, activation='relu')(dr4)
dr5 = tf.keras.layers.Dropout(0.4)(c2)
softmax = tf.keras.layers.Dense(num_classes, activation="softmax", name='classification')(dr5)

# Decoder
d1 = tf.keras.layers.Dense(256, activation="relu")(encoded)
dr6 = tf.keras.layers.Dropout(0.4)(d1)
d2 = tf.keras.layers.Dense(512, activation="relu")(dr6)
dr7 = tf.keras.layers.Dropout(0.4)(d2)
d3 = tf.keras.layers.Dense(1024, activation="tanh")(dr7)
dr8 = tf.keras.layers.Dropout(0.4)(d3)
decoded = tf.keras.layers.Dense(inp_dim, activation="tanh", name='autoencoder')(dr8)

hybrid = tf.keras.Model(inputs=inp, outputs=[softmax, decoded])

hybrid.compile(loss={'classification': 'categorical_crossentropy', 'autoencoder': 'mse'},
               loss_weights={'classification': 1 - gama, 'autoencoder': gama},
               optimizer='adam',
               metrics={'classification': [tf.keras.metrics.Precision(), tf.keras.metrics.Recall(), tf.keras.metrics.CategoricalAccuracy()],
                        'autoencoder': ['mse']})

hybrid.fit(training_features,
           {'classification': one_hot_labels, 'autoencoder': training_features},
           batch_size=batch_size,
           epochs=epochs,
           validation_data=(testing_features, {'classification': test_one_hot_labels, 'autoencoder': testing_features}),
           shuffle=True,
           verbose=1)

hybrid.evaluate(training_features,
                {'classification': one_hot_labels, 'autoencoder': training_features})
