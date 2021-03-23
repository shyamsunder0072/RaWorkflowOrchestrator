import tensorflow as tf
import matplotlib.pyplot as plt
import io
import numpy as np

from airflow.operators.tfserving.audio.audio_fingerprinting.audio_features import AudioFeatures
from airflow.operators.tfserving.audio.audio_fingerprinting.waveform_model_architecture import Autoencoder, loss, train

sample_rate = 8000  # 22050
sample_duration = 5
batch_size = 128
epochs = 100
sample_outputs = 10


def plot_to_image(figure):
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close(figure)
    buf.seek(0)
    image = tf.image.decode_png(buf.getvalue(), channels=4)
    image = tf.expand_dims(image, 0)
    return image


spectrogram_pairs = AudioFeatures.extract_waveform_features(sample_rate=sample_rate, sample_duration=sample_duration)

autoencoder = Autoencoder(n_dims=[4048, 2048, 1024, 512, 256], input_dim=sample_duration * sample_rate)

training_features = np.asarray([*spectrogram_pairs.values()])
# training_features = training_features.astype('float32')
training_dataset = tf.data.Dataset.from_tensor_slices(training_features)
training_dataset = training_dataset.batch(batch_size)
training_dataset = training_dataset.shuffle(training_features.shape[0])
training_dataset = training_dataset.prefetch(batch_size * 4)

writer = tf.summary.create_file_writer('log')

with writer.as_default():
    with tf.summary.record_if(True):
        for epoch in range(epochs):
            learning_rate = 0.2
            if epoch > 10:
                learning_rate = 0.02
            if epoch > 20:
                learning_rate = 0.01
            if epoch > 50:
                learning_rate = 0.005
            opt = tf.optimizers.Adam(learning_rate=learning_rate)
            print(epoch)
            for step, batch_features in enumerate(training_dataset):
                # print(step)
                train(loss, autoencoder, opt, batch_features)
                loss_values = loss(autoencoder, batch_features)
                original_audio = tf.reshape(batch_features, (batch_features.shape[0], batch_features.shape[1], 1))
                reconstructed_audio = tf.reshape(autoencoder(tf.constant(batch_features)),
                                                 (batch_features.shape[0], batch_features.shape[1], 1))
                cumm_step = (step + 1) + epoch * np.ceil(len(training_features) / batch_size)
                tf.summary.scalar('loss', loss_values, step=cumm_step)
                tf.summary.audio('original', original_audio, sample_rate=sample_rate, step=cumm_step, max_outputs=sample_outputs,
                                 encoding=None,
                                 description=None)
                tf.summary.audio('reconstructed', reconstructed_audio, sample_rate=sample_rate, step=cumm_step, max_outputs=sample_outputs,
                                 encoding=None,
                                 description=None)

                original_waveform = tf.reshape(batch_features, (batch_features.shape[0], batch_features.shape[1]))
                reconstructed_waveform = tf.reshape(autoencoder(tf.constant(batch_features)),
                                                    (batch_features.shape[0], batch_features.shape[1]))

                fig, axes = plt.subplots(nrows=min(len(batch_features), sample_outputs), ncols=2, sharex='all', sharey='row')
                plt.suptitle("Original vs Reconstructed Audio Waveforms", size=10)
                for i in np.arange(axes.shape[0]):
                    ax1 = axes[i][0]
                    ax2 = axes[i][1]
                    ax1.plot(original_waveform[i], 'y')
                    ax1.tick_params(axis='both', which='major', labelsize=5)
                    ax1.tick_params(axis='both', which='minor', labelsize=5)
                    ax2.plot(reconstructed_waveform[i], 'r')
                    ax2.tick_params(axis='both', which='major', labelsize=5)
                    ax2.tick_params(axis='both', which='minor', labelsize=5)

                image = plot_to_image(fig)
                tf.summary.image("waveforms", image, step=cumm_step, max_outputs=10)

                # fig, axes = plt.subplots(nrows=min(len(batch_features), sample_outputs), ncols=2, sharex='all', sharey='row')
                # plt.colorbar(format='%+2.0f dB')
                # plt.suptitle("Original vs Reconstructed Audio Spectrograms", size=10)
                # for i in np.arange(axes.shape[0]):
                #     S_dB = librosa.power_to_db(librosa.feature.melspectrogram(y=original_waveform[i].numpy(), sr=sample_rate, n_mels=128), ref=np.max)
                #     axes[i][0].plot(librosa.display.specshow(S_dB, x_axis='time', y_axis='mel', sr=sample_rate))

                template = 'Epoch {}, Step {}, Loss: {}'
                print(template.format(epoch, step + 1, loss_values))
