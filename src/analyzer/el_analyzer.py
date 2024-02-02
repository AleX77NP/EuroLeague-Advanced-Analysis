from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, col


class ElAnalyzer:
    def __init__(self, spark, df):
        """
        Initialize the ElAnalyzer class with a Spark session.

        Parameters:
        - spark: SparkSession
        - df: intiial DataFrame
        """
        self.spark = spark
        self.df = df

    def calculate_averages(self, order_col="AVG_PTS_DIFF"):
        """
        Calculate the points scored and conceived per team
        Parameters:
        - order_col: Scolumns used for order by sorting (default is 'AVG_PTS_DIFF')

        Returns:
        - result_df: Spark DataFrame with columns 'team', 'AVG_PTS', 'AVG_OP_PTS' and 'AVG_PTS_DIFF'
        """
        avg_columns = [
            round(avg(col), 1).alias(f"AVG_{col}") for col in ["PTS", "OP_PTS"]
        ]
        result_df = (
            self.df.groupBy("team")
            .agg(*avg_columns)
            .withColumn("AVG_PTS_DIFF", round(col("AVG_PTS") - col("AVG_OP_PTS"), 1))
            .orderBy(order_col, ascending=False)
        )
        return result_df
