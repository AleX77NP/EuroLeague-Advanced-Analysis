from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, col


class EuroLeagueAnalyzer:
    def __init__(self, spark, df):
        """
        Initialize the EuroLeagueAnalyzer class with a Spark session and DataFrame.

        Parameters:
        - spark: SparkSession
        - df: intiial DataFrame
        """
        self.__spark = spark
        self.__df = df
        self.__query_path = "data/queries"

    def calculate_team_averages(self, order_col="AVG_PTS_DIFF"):
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
            self.__df.groupBy("team")
            .agg(*avg_columns)
            .withColumn("AVG_PTS_DIFF", round(col("AVG_PTS") - col("AVG_OP_PTS"), 1))
            .orderBy(order_col, ascending=False)
        )
        return result_df

    def calculate_team_win_streaks(self):
        """
        Calculate longest consecutive win streaks

        Returns:
        - result_df: Spark DataFrame with columns 'team', 'WIN_STREAK'
        """
        self.__df.createOrReplaceTempView("euroleague_games")

        with open(f"{self.__query_path}/win_streak.sql", "r") as file:
            sql_query = file.read()

        result_df = self.__spark.sql(sql_query)
        return result_df
