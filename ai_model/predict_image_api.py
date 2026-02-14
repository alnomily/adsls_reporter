import tensorflow as tf
from keras.saving import load_model
from keras.utils import register_keras_serializable


@register_keras_serializable(package="Custom", name="Tran")
class TransposeLayer(tf.keras.layers.Layer):
    def call(self, inputs):
        return tf.transpose(inputs, perm=[0, 2, 1, 3])

    def compute_output_shape(self, input_shape):
        return (input_shape[0], input_shape[2], input_shape[1], input_shape[3])


class PredictImageAPI:
    _image_height, _image_width = 60, 200

    def __init__(self, model_path: str):
        self._int_to_char = {i: char for i, char in enumerate(sorted(list("0123456789X")))}
        # The shipped model is a `.keras` artifact saved with Keras 3.
        # Load with `compile=False` since we only need inference.
        self.model = load_model(
            str(model_path),
            compile=False,
            custom_objects={"TransposeLayer": TransposeLayer},
        )

    def __decode_predictions(self, pred) -> str:
        input_len = tf.fill((1,), 25)
        decoded, _ = tf.keras.backend.ctc_decode(pred, input_length=input_len, greedy=True)

        try:
            seq = decoded[0][0, :4].numpy()
        except Exception:
            try:
                seq = decoded[0].numpy().ravel()
            except Exception:
                return ""

        decoded_labels: list[str] = []
        for x in seq:
            try:
                xi = int(x)
            except Exception:
                continue
            if xi < 0:
                continue
            ch = self._int_to_char.get(xi)
            if ch is None:
                continue
            decoded_labels.append(ch)

        return "".join(decoded_labels)

    def __preprocess_image_bytes(self, image_bytes: bytes):
        # Accept common formats (png/jpg). decode_image returns uint8.
        image = tf.io.decode_image(image_bytes, channels=3, expand_animations=False)
        image = tf.image.resize(image, (self._image_height, self._image_width))
        image = tf.cast(image, tf.float32)

        r, g, b = image[..., 0], image[..., 1], image[..., 2]
        yellow_enhanced = (r + g) - b

        yellow_enhanced = tf.image.adjust_contrast(tf.expand_dims(yellow_enhanced, -1), 1.1)
        yellow_enhanced = tf.clip_by_value(yellow_enhanced, 0, 255)
        return yellow_enhanced

    def predict_image_bytes(self, image_bytes: bytes) -> str:
        processed_image = self.__preprocess_image_bytes(image_bytes)
        processed_image = tf.expand_dims(processed_image, axis=0)
        prediction = self.model.predict(processed_image, verbose=0)
        return self.__decode_predictions(prediction)
