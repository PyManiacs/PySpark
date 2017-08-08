import os
from pyspark.mllib.recommendation import ALS

from pyspark.sql import SQLContext
from pyspark.sql.types import *


import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:
    """A Details Fetching Engine"""







    def __get_customers(self, customer_id):
    #get customers
    #create a SQLContext from SparkContext
	sqlContext = SQLContext(self.sc)
    #create our customSchema for the DataFrame
	customSchema = StructType([ \
		    StructField("CUSTOMER_ID", StringType(), True), \
		    StructField("CUSTOMER_NAME", StringType(), True), \
		    StructField("CUSTOMER_ADDRESS1", StringType(), True), \
		    StructField("CUSTOMER_ADDRESS2", StringType(), True), \
		    StructField("CUSTOMER_CITY", StringType(), True), \
		    StructField("CUSTOMER_STATE", StringType(), True), \
 		    StructField("CUSTOMER_COUNTRY", StringType(), True), \
		    StructField("CUSTOMER_ZIPCODE", StringType(), True)])
	df = sqlContext.createDataFrame(self.customer_RDD, customSchema)
	#Infer the scehma and register the DataFrame as a table
	df.registerTempTable("tpo_cutomer")
	#getting the required data by querying the table
	testquery="""SELECT CUSTOMER_ID,CUSTOMER_NAME,CUSTOMER_ADDRESS1,CUSTOMER_ADDRESS2,CUSTOMER_CITY,CUSTOMER_STATE,CUSTOMER_COUNTRY,CUSTOMER_ZIPCODE FROM tpo_cutomer where CUSTOMER_ID='%s' """%(customer_id)
	logger.info(testquery)
	tcp_interactions = sqlContext.sql(testquery)
	logger.info("Loading Customer...")
	logger.info(customer_id)
	logger.info(tcp_interactions.show())
        return tcp_interactions

    def __get_sites(self, site_id):
    #get sitedetails
	sqlContext = SQLContext(self.sc)
	customSchema = StructType([ \
		    StructField("SITE_ID", StringType(), True), \
		    StructField("SITE_NAME", StringType(), True), \
		    StructField("SITE_ADDRESS1", StringType(), True), \
		    StructField("SITE_ADDRESS2", StringType(), True), \
		    StructField("SITE_CITY", StringType(), True), \
 		    StructField("SITE_STATE", StringType(), True), \
		    StructField("SITE_COUNTRY", StringType(), True), \
		     StructField("SITE_ZIPCODE", StringType(), True), \
		    StructField("CUSTOMER_ID", StringType(), True)])
	df = sqlContext.createDataFrame(self.site_RDD, customSchema)
	#Infer the scehma and register the DataFrame as a table
	df.registerTempTable("tpo_site")
	testquery="""SELECT * FROM tpo_site where SITE_ID='%s' """%(site_id)
	logger.info(testquery)
	tcp_interactions = sqlContext.sql(testquery)
	logger.info("Loading Site...")
        return tcp_interactions

    def __get_turbines(self):
    #get turbinedetails
	sqlContext = SQLContext(self.sc)
	customSchema = StructType([ \
		    StructField("TURBINE_ID", StringType(), True), \
		    StructField("TURBINE_SN", StringType(), True), \
		    StructField("EQUIPMENT_TYPE", StringType(), True), \
		    StructField("EQUIPMENT_MODEL", StringType(), True), \
		    StructField("EQUIPMENT_NAME", StringType(), True), \
 		    StructField("SITE_ID", StringType(), True), \
		    StructField("SITE_NAME", StringType(), True), \
		    StructField("CUSTOMER_ID", StringType(), True)])
	df = sqlContext.createDataFrame(self.turbine_RDD, customSchema)
	#Infer the scehma and register the DataFrame as a table
	df.registerTempTable("tpo_unit_config")
	testquery="""SELECT * FROM tpo_unit_config"""
	logger.info(testquery)
	tcp_interactions = sqlContext.sql(testquery)
	logger.info("Loading Site...")
        return tcp_interactions


    def get_customer_info(self, customer_id):
        #Get customer_info
        customers = self.__get_customers(customer_id).collect()
        return customers

    def get_site_info(self, site_id):
        # Get site_info
        sites = self.__get_sites(site_id).collect()
        return sites

    def get_turbine_info(self):
        #Get turbine_info
        turbines = self.__get_turbines().collect()

        return turbines


    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Turbine Deails Fetch Engine: ")
        self.sc = sc

	#Load cusomer data for later use
        logger.info("Loading Customer data...")
        customer_file_path = os.path.join(dataset_path, 'tpo_customer.csv')
        #Reading the csv file
        customer_raw_RDD = self.sc.textFile(customer_file_path)
        #Eliminating the first line from the list of data
        customer_raw_data_header = customer_raw_RDD.take(1)[0]
        #Split the rows into columns and form a rdd
        self.customer_RDD = customer_raw_RDD.filter(lambda line: line!=customer_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: ((tokens[0]),(tokens[1]),(tokens[2]),(tokens[3]),(tokens[4]),(tokens[5]),(tokens[6]),(tokens[7]))).cache()
        logger.info("Loading Customer data success...")

	#Load turbine data for later use
        logger.info("Loading Turbine data...")
        turbine_file_path = os.path.join(dataset_path, 'test_tpo_unit_config.csv')
        turbine_raw_RDD = self.sc.textFile(turbine_file_path)
        #Eliminating the first line from the list of data
        turbine_raw_data_header = turbine_raw_RDD.take(1)[0]
        #Split the rows into columns and form a rdd
        self.turbine_RDD = turbine_raw_RDD.filter(lambda line: line!=turbine_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: ((tokens[0]),(tokens[1]),(tokens[2]),(tokens[3]),(tokens[5]),(tokens[34]),(tokens[51]),(tokens[35]))).cache()
        logger.info("Loading Turbine data success...")


	#Load site data for later use
        logger.info("Loading Site data...")
        site_file_path = os.path.join(dataset_path, 'tpo_site.csv')
        site_raw_RDD = self.sc.textFile(site_file_path)
        #Eliminating the first line from the list of data
        site_raw_data_header = site_raw_RDD.take(1)[0]
        #Split the rows into columns and form a rdd
        self.site_RDD = site_raw_RDD.filter(lambda line: line!=site_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: ((tokens[0]),(tokens[1]),(tokens[2]),(tokens[3]),(tokens[4]),(tokens[5]),(tokens[6]),(tokens[7]),(tokens[16]))).cache()
        logger.info("Loading Site data success...")
