from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator


class EuroLeaguePredictor:
    """
    Parameters:
    - spark: SparkSession
    - features: [str], list of columns
    - label: str, label column
    """

    def __init__(self, spark_session, features, label):
        self.__spark = spark_session
        self.__model = None

        self.__features_columns = features
        self.__label_column = label
        self.__max_iters = 10
        self.__reg_param = 0.01

        self.__test_data = None
        self.__test_predictions = None
        self.__model_acc = None

    def load_data(self, data):
        # Load your dataset (assuming it has features and labels columns)
        self.__data = data

    def preprocess_data(self):
        # Assuming you have 'features' and 'label' columns in your dataset
        assembler = VectorAssembler(
            inputCols=self.__features_columns, outputCol="features"
        )
        self.__data = assembler.transform(self.__data).select(
            "features", self.__label_column
        )

    def train_model(self):
        # Split the data into training and testing sets
        train_data, self.__test_data = self.__data.randomSplit([0.8, 0.2], seed=42)

        # Create a Logistic Regression model
        lr = LogisticRegression(
            featuresCol="features",
            labelCol=self.__label_column,
            maxIter=self.__max_iters,
            regParam=self.__reg_param,
        )

        # Create a pipeline with the assembler and the logistic regression model
        pipeline = Pipeline(stages=[lr])

        # Train the model
        self.__model = pipeline.fit(train_data)

    def evaluate_model(self):
        # Make predictions on the test data
        predictions = self.__model.transform(self.__test_data)

        # Evaluate the model using BinaryClassificationEvaluator
        evaluator = BinaryClassificationEvaluator(
            rawPredictionCol="rawPrediction", labelCol=self.__label_column
        )
        accuracy = evaluator.evaluate(predictions)
        self.__model_acc = accuracy 
        self.__test_predictions = (predictions.select(self.__label_column).collect(), predictions.select("probability").collect())

        print(f"Model Accuracy: {accuracy}")

    def predict(self, input_data):
        # Assuming input_data is a DataFrame with the same structure as the training data
        input_data = VectorAssembler(
            inputCols=self.__features_columns, outputCol="features"
        ).transform(input_data)
        result = self.__model.transform(input_data)
        return result.select("prediction")

    def get_model(self):
        return self.__model
    
    def get_model_accuracy(self):
        return self.__model_acc
    
    def get_test_predictions(self):
        return self.__test_predictions