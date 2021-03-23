import tensorflow as tf
import tensorflow.keras.layers as layers
tf.config.experimental_run_functions_eagerly(True)


class Encoder(tf.keras.layers.Layer):

    def __init__(self,
                 n_dims,
                 name='encoder',
                 **kwargs):
        super(Encoder, self).__init__(name=name, **kwargs)
        self.n_dims = n_dims
        # self.n_layers = 1

    # @tf.function
    def call(self, inputs):
        x = inputs
        for i in self.n_dims:
            encode_layer = layers.Dense(i, activation='relu')
            x = encode_layer(x)
        return x


class Decoder(tf.keras.layers.Layer):

    def __init__(self,
                 n_dims,
                 input_dim,
                 name='decoder',
                 **kwargs):
        super(Decoder, self).__init__(name=name, **kwargs)
        self.n_dims = n_dims
        self.input_dim = input_dim

    # @tf.function
    def call(self, inputs):
        x = inputs
        for i in self.n_dims:
            decode_layer = layers.Dense(i, activation='relu')
            x = decode_layer(x)
        reconstruction_layer = layers.Dense(self.input_dim, activation='tanh')
        x = reconstruction_layer(x)
        return x


class Autoencoder(tf.keras.Model):

    def __init__(self,
                 n_dims,
                 input_dim,
                 name='autoencoder',
                 **kwargs):
        super(Autoencoder, self).__init__(name=name, **kwargs)
        self.n_dims = n_dims
        self.input_dim = input_dim
        self.encoder = Encoder(n_dims)
        n_dims.reverse()
        self.decoder = Decoder(n_dims, input_dim)

    # @tf.function
    def call(self, inputs):
        x = self.encoder(inputs)
        return self.decoder(x)


def loss(model, original):
    reconstruction_error = tf.reduce_mean(tf.square(tf.subtract(model(original), original)))
    return reconstruction_error


def train(loss, model, opt, original):
    with tf.GradientTape() as tape:
        gradients = tape.gradient(loss(model, original), model.trainable_variables)
        gradient_variables = zip(gradients, model.trainable_variables)
        opt.apply_gradients(gradient_variables)
