from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (floor, col, current_timestamp, avg, max, sum, 
                                   count, dense_rank, length, broadcast)
from pyspark.sql import Window
import logging
import datetime

def get_season():
    """
    Function that derives the current season string that is compatiable with the API URL
    Args: None
    """
    try:
        # This grabs the current year and month
        now = datetime.datetime.now()
        year = now.year
        month = now.month - 4
        season = f"{year}-{month:02d}"
        logging.info(f"Clash of Clans League Ranking Stats from year: {year}, and month: {month} will be pulled from the API")

    except Exception as e:
        logging.error(f"There was an issue getting the current season... Error: {e}")

    return season

def create_spark_session(app_name: str = "clashofspark") -> SparkSession:
    """Create a Spark Session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    
    return spark 

def read_bronze_df(spark: SparkSession, season: str ) -> DataFrame:
    """

    Load bronze parquet from S3 with schema and load_ts.
    
    """

    try:

        raw_path = f"s3a://warehouse/bronze/coc_data_{season}.parquet"

        bronze_df = spark.read \
            .parquet(raw_path) \
            .withColumn("load_ts", current_timestamp()) \
            
        logging.info("Bronze DF loaded successfully from S3.")

        # drop bad records
        bronze_df = bronze_df.filter(
            col("expLevel").isNotNull() &
            (col("expLevel") == floor(col("expLevel")))
        )

        # Cast to long
        bronze_df = bronze_df.withColumn("expLevel", col("expLevel").cast("long"))

        return bronze_df

    except Exception as e:
        logging.error(f"Failed to read Bronze DF from s3: {e}")
        raise

        
def build_silver_df(bronze_df: DataFrame) -> DataFrame:
    """

    Clean, select relevant columns, and filter for quality.

    """
    # Clean column names
    for column in bronze_df.columns:
        bronze_df = bronze_df.withColumnRenamed(column, column.replace(".", "_"))
    

    # Select usable fields
    silver_df = bronze_df.select(
        col("tag").alias("player_tag"),
        col("name").alias("player_name"),
        col("expLevel").alias("player_xp_level"),
        col("rank").alias("player_rank"),
        col("trophies").alias("player_trophies"),
        col("attackWins").alias("attack_wins"),
        col("defenseWins").alias("defense_wins"),
        col("clan_tag"),
        col("clan_name"),
        col("leagueTier_name").alias("league_name"),
        col("load_ts"),
    )


    # Quality filters
    silver_df = silver_df.filter(
        (length(col("player_tag")) >= 3) &
        col("player_tag").isNotNull() &
        (col("player_trophies") > 0) &
        col("player_tag").like("#%")
    )

    silver_df.cache()
    silver_df.count()   # materialize cache

    return silver_df


def write_silver_df(silver_df: DataFrame, season: str):
    """

    Writing  to silver s3.
    
    """
    try:
        
        silver_df.coalesce(2) \
        .write \
        .mode("overwrite") \
        .parquet(f"s3a://warehouse/silver/players_summary_{season}")

        
        logging.info("Silver DF written to s3 ")

    except Exception as e:
        logging.error(f"Failed to write Silver DF to s3: {e}")
        raise


def build_gold_players_df(silver_df: DataFrame) -> DataFrame:
    """

    Calculate total combat wins and sort.

    """
    gold_players_df = silver_df.withColumn(
        "total_combat_wins",
        col("attack_wins") + col("defense_wins")
    )

    return gold_players_df


def write_gold_players_df(gold_players_df: DataFrame, season: str):

    """
    
    Write to s3.
    
    """
    try:
        gold_players_df.coalesce(2) \
        .write \
        .mode("overwrite") \
        .parquet(f"s3a://warehouse/gold/players_df_{season}")

        
        logging.info(" Gold players DF written s3.")

    except Exception as e:
        logging.error(f" Failed to write gold players DF to s3 : {e}")
        raise


def build_gold_clans_df(silver_df: DataFrame, gold_players_df: DataFrame) -> DataFrame:
    """

    Clan-level aggregations + ranking, additonally enrich gold players DF with more clan stats.

    """
    clans_df = silver_df.groupBy("clan_tag")\
        .agg(
            avg(col("player_xp_level")).alias("average_player_xp"),
            max(col("player_xp_level")).alias("highest_level_player"),
            avg(col("player_trophies")).alias("average_player_trophies"),
            sum(col("attack_wins")).alias("total_attack_wins"),
            sum(col("defense_wins")).alias("total_defense_wins"),
            count(col("player_tag")).alias("number_of_players")
        )\
        .filter(col("clan_tag").isNotNull())
    
    

    # Ranking window
    window_spec = Window.orderBy(col("average_player_trophies").desc())
    clans_df = clans_df.withColumn("clan_ranking", dense_rank().over(window_spec))

    # Enrich players
    final_players_df = gold_players_df.join(
        broadcast(clans_df.select("clan_tag", "clan_ranking")),
        on="clan_tag",
        how="left"
    )

    return clans_df, final_players_df


def write_gold_clans_df(clans_df: DataFrame, season: str):    
    """
    
    Write to S3.
    
    """
    try:

        clans_df.coalesce(2) \
        .write \
        .mode("overwrite") \
        .parquet(f"s3a://warehouse/gold/clans_summary_{season}")

        
        logging.info("Gold clans DF written to s3.")
    except Exception as e:
        logging.error(f"Failed to write gold clans DF to s3: {e}")
        raise


def build_leaderboard_df(final_players_df: DataFrame) -> DataFrame:
    """

    Rank players by trophies within each clan.
    
    """
    window_spec = Window.partitionBy("clan_tag").orderBy(col("player_trophies").desc())

    gold_lb_df = final_players_df.withColumn("rank_in_clan", dense_rank().over(window_spec))\

    
    return gold_lb_df
    


def write_leaderboard_df(gold_lb_df: DataFrame, season : str):
    """
    
    Write to S3.
    
    """
    try:

        gold_lb_df.coalesce(2) \
        .write \
        .mode("overwrite") \
        .parquet(f"s3a://warehouse/gold/player_leaderboard_{season}")


    except Exception as e:
        logging.error(f"Failed to write leaderboard DF to s3: {e}")
        raise


def main():
    """
    
    Spark ETL Pipeline

    """
    

    try:

        spark = create_spark_session()
        
        # Get season 
        season = get_season()

        # Bronze ingest
        bronze_df = read_bronze_df(spark, season)

        # Silver layer
        silver_df = build_silver_df(bronze_df)

        write_silver_df(silver_df,season)

        # Gold (players)
        gold_players_df = build_gold_players_df(silver_df)

        write_gold_players_df(gold_players_df,season)

        # Gold (clans + enriched players)
        clans_df, final_players_df = build_gold_clans_df(silver_df, gold_players_df)

        # uncache silver df
        silver_df.unpersist()

        write_gold_clans_df(clans_df, season)

        # Gold (leaderboard)
        leaderboard_df = build_leaderboard_df(final_players_df)

        write_leaderboard_df(leaderboard_df, season)

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
