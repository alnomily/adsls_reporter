from keras.utils import register_keras_serializable
from keras.models import load_model
from tensorflow.python.keras.backend import ctc_batch_cost
import tensorflow as tf


# --- Custom Objects ---
@register_keras_serializable(package="Custom", name="ctc_loss")
def ctc_loss(y_true, y_pred):
    batch_size = tf.shape(y_true)[0]
    input_length = tf.fill((batch_size, 1), tf.shape(y_pred)[1])
    label_length = tf.fill((batch_size, 1), 4)
    return ctc_batch_cost(y_true, y_pred, input_length, label_length)

@register_keras_serializable(package="Custom", name="Tran")
class TransposeLayer(tf.keras.layers.Layer):
    def call(self, inputs):
        return tf.transpose(inputs, perm=[0, 2, 1, 3])
    def compute_output_shape(self, input_shape):
        return (input_shape[0], input_shape[2], input_shape[1], input_shape[3])


class PredictImageAPI:
    _image_height, _image_width = 60, 200

    def __init__(self, model_path):
        self._int_to_char = {i: char for i,
                             char in enumerate(sorted(list("0123456789X")))}
        self.model = load_model(str(model_path))

    def __decode_predictions(self, pred):
        import numpy as np
        input_len = tf.fill(
            (1,), 25)
        decoded, _ = tf.keras.backend.ctc_decode(
            pred, input_length=input_len, greedy=True)
        # decoded may contain -1 or values outside our mapping (CTC blank or unknown)
        try:
            seq = decoded[0][0, :4].numpy()
        except Exception:
            # fallback: try to decode whatever is available
            try:
                seq = decoded[0].numpy().ravel()
            except Exception:
                return ""

        decoded_labels = []
        for x in seq:
            try:
                xi = int(x)
            except Exception:
                continue
            # ignore CTC blank (-1) or any negative values
            if xi < 0:
                continue
            ch = self._int_to_char.get(xi)
            if ch is None:
                # unknown index — skip
                continue
            decoded_labels.append(ch)

        predicted_text = ''.join(decoded_labels)
        return predicted_text

    def __preprocess_image(self, image_path):
        image = tf.io.read_file(str(image_path))
        image = tf.image.decode_png(image, channels=3)
        image = tf.image.resize(
            image, (self._image_height, self._image_width))
        image = tf.cast(image, tf.float32)

        r, g, b = image[..., 0], image[..., 1], image[..., 2]

        yellow_enhanced = (r + g) - b

        yellow_enhanced = tf.image.adjust_contrast(
            tf.expand_dims(yellow_enhanced, -1), 1.1)
        yellow_enhanced = tf.clip_by_value(
            yellow_enhanced, 0, 255)

        return yellow_enhanced

    def predict_image(self, image_path):
        processed_image = self.__preprocess_image(image_path)
        if processed_image is None: 
            return None
        processed_image = tf.expand_dims(processed_image, axis=0)
        prediction = self.model.predict(processed_image)
        decoded_text = self.__decode_predictions(prediction)
        return decoded_text
