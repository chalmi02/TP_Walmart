from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark import SparkContext
from pyspark.sql.types import IntegerType


if __name__=="__main__":

    # --- on ouvre une session Spark --- 
    spark = SparkSession.builder.master('local').appName("Walmart_Stock").getOrCreate()
    
    # on telecharge le fichier CSV et on convertis les données en int 
    df=spark.read.option("header","true").csv("walmart_stock.csv")
    df=df.withColumn("Volume", col("Volume").cast(IntegerType()))
    df=df.withColumn("Close", col("Close").cast(IntegerType()))
    df=df.withColumn("High", col("High").cast(IntegerType()))
    #df.show()

    # --- question 3-4 le nom des colonnes et schema --- ? 

    
    #df.columns
    #df.printSchema()

    # --- question 5 on creer un nouveau dataframe avec la colonne HV ratio ---

    #df2 = df.withColumn("HV_RATIO",col("High")/col("Volume"))

    # --- question 6 ---

    #df.orderBy(df['High'].desc()).select(['Date']).['Date']

    #sql
    df.createOrReplaceTempView("tab") #SQL
    spark.sql("select Date from tab where High=(select max(High) from tab) ").show()

    # --- question 7 ---

    #spark.sql(" select avg(Close) from tab").show() #sql
    df.agg({'Close': 'mean'}).show() 

    # --- question 8 ---

    #spark.sql("select min(Volume),max(Volume)  from tab").show() #sql
    df.select(max('Volume'),min('Volume')).show() 

    # --- question 9 ---

    #spark.sql("select count(Close) from tab where Close < 60    ").show()
    df.filter(df['Close'] <60).count()


    # --- question 10 ---

    spark.sql("""select round (
    (select count(High) from tab where High >80 ) /count(Date)*100,2) as Pourcentage from tab""").show()
    
    df.filter('High > 80').count() * 100/df.count()

    


    # --- question 11 ---

    #spark.sql(" select Year(date) from tab where High=(select max(High) from tab) group by Year").show()
    df.groupBy(year("Date").alias("Annee")).agg({"High" : "max" }).orderBy("Annee").show()

    # --- question 12 ---

    df.groupBy(month("Date").alias("Mois")).agg({"Close" : "mean" }).orderBy("Mois").show()

    