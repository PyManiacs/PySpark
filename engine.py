gt import os
from pyspark.mllib.recommendation import ALS

from pyspark.sql import SQLContext
from pyspark.sql.types import *


import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (movieID, ratings_iterable) 
    returns (movieID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


class RecommendationEngine:
    """A movie recommendation engine
    """

    def __count_and_average_ratings(self):
        """Updates the movies ratings counts from 
        the current data self.ratings_RDD
        """
        logger.info("Counting movie ratings...")
        movie_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
        self.movies_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))


    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                               iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("ALS model built!")


    def __predict_ratings(self, user_and_movie_RDD):
        """Gets predictions for a given (userID, movieID) formatted RDD
        Returns: an RDD with format (movieTitle, movieRating, numRatings)
        """
        predicted_RDD = self.model.predictAll(user_and_movie_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.movies_titles_RDD).join(self.movies_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
        
        return predicted_rating_title_and_count_RDD



    def __get_customers(self, customer_id):
        """get customers
        """	    
	
	sqlContext = SQLContext(self.sc)
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
	#dist_df1=sqlContext.createDataFrame(self.customer_RDD)
	df.registerTempTable("tpo_cutomer")
	#dist_df1.show()
	testquery="""SELECT CUSTOMER_ID,CUSTOMER_NAME,CUSTOMER_ADDRESS1,CUSTOMER_ADDRESS2,CUSTOMER_CITY,CUSTOMER_STATE,CUSTOMER_COUNTRY,CUSTOMER_ZIPCODE FROM tpo_cutomer where CUSTOMER_ID='%s' """%(customer_id)
	logger.info(testquery)
	tcp_interactions = sqlContext.sql(testquery)
	logger.info("Loading Customer...")
	logger.info(customer_id)	
	logger.info(tcp_interactions.show())
        return tcp_interactions

    def __get_sites(self, site_id):
        """get sitedetails
        """	    
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
	#dist_df1=sqlContext.createDataFrame(self.site_RDD)
	df.registerTempTable("tpo_site")
	#dist_df1.show()
	testquery="""SELECT * FROM tpo_site where SITE_ID='%s' """%(site_id)
	logger.info(testquery)
	tcp_interactions = sqlContext.sql(testquery)
	logger.info("Loading Site...")
	#logger.info(customer_id)	
	#tcp_interactions.show()
        return tcp_interactions
    
    def __get_turbines(self):
        """get sitedetails
        """	

    
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
	#dist_df1=sqlContext.createDataFrame(self.turbine_RDD)
	df.registerTempTable("tpo_unit_config")
	#dist_df1.show()
	testquery="""SELECT * FROM tpo_unit_config"""
	logger.info(testquery)
	tcp_interactions = sqlContext.sql(testquery)
	logger.info("Loading Site...")
	#logger.info(customer_id)	
	#tcp_interactions.show()
        return tcp_interactions
    
    def add_ratings(self, ratings):
        """Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings_RDD = self.sc.parallelize(ratings)
        # Add new ratings to the existing ones
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # Re-compute movie ratings count
        self.__count_and_average_ratings()
        # Re-train the ALS model with the new ratings
        self.__train_model()
        
        return ratings

    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """
        requested_movies_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        ratings = self.__predict_ratings(requested_movies_RDD).collect()

        return ratings
    def get_customer_info(self, customer_id):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """
        #requested_customer_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        customers = self.__get_customers(customer_id).collect()

        return customers
    
    def get_site_info(self, site_id):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """
        #requested_customer_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        sites = self.__get_sites(site_id).collect()

        return sites

    def get_turbine_info(self):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """
        turbines = self.__get_turbines().collect()

        return turbines
    
    def get_top_ratings(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        # Get pairs of (userID, movieID) for user_id unrated movies
        user_unrated_movies_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id)\
                                                 .map(lambda x: (user_id, x[1])).distinct()
        # Get predicted ratings
        ratings = self.__predict_ratings(user_unrated_movies_RDD).filter(lambda r: r[2]>=25).takeOrdered(movies_count, key=lambda x: -x[1])

        return ratings

    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

	#Load cusomer data for later use
	
        logger.info("Loading Customer data...")
        customer_file_path = os.path.join(dataset_path, 'tpo_customer.csv')
        customer_raw_RDD = self.sc.textFile(customer_file_path)
        customer_raw_data_header = customer_raw_RDD.take(1)[0]
        self.customer_RDD = customer_raw_RDD.filter(lambda line: line!=customer_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: ((tokens[0]),(tokens[1]),(tokens[2]),(tokens[3]),(tokens[4]),(tokens[5]),(tokens[6]),(tokens[7]))).cache()
	logger.info("Loading Customer data success...")
	#CUSTOMCUSTOMER_NAME,CUSTOMER_ADDRESS1,CUSTOMER_ADDRESS2,CUSTOMER_CITY,CUSTOMER_STATE,CUSTOMER_COUNTRY,CUSTOMER_ZIPCODE,CREATED_BY,CREATION_DATE,LAST_UPDATED_BY,LAST_UPDATE_DATE
        


	
	#Load turbine data for later use	
        logger.info("Loading Turbine data...")
        turbine_file_path = os.path.join(dataset_path, 'test_tpo_unit_config.csv')
        turbine_raw_RDD = self.sc.textFile(turbine_file_path)
        turbine_raw_data_header = turbine_raw_RDD.take(1)[0]
        self.turbine_RDD = turbine_raw_RDD.filter(lambda line: line!=turbine_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: ((tokens[0]),(tokens[1]),(tokens[2]),(tokens[3]),(tokens[5]),(tokens[34]),(tokens[51]),(tokens[35]))).cache()
	logger.info("Loading Turbine data success...")
 
	
	
	
	#Load site data for later use	
        logger.info("Loading Site data...")
        site_file_path = os.path.join(dataset_path, 'tpo_site.csv')
        site_raw_RDD = self.sc.textFile(site_file_path)
        site_raw_data_header = site_raw_RDD.take(1)[0]
        self.site_RDD = site_raw_RDD.filter(lambda line: line!=site_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: ((tokens[0]),(tokens[1]),(tokens[2]),(tokens[3]),(tokens[4]),(tokens[5]),(tokens[6]),(tokens[7]),(tokens[16]))).cache()
	logger.info("Loading Site data success...")
	



	# Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
        # Load movies data for later use
        logger.info("Loading Movies data...")
        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        movies_raw_RDD = self.sc.textFile(movies_file_path)
        movies_raw_data_header = movies_raw_RDD.take(1)[0]
        self.movies_RDD = movies_raw_RDD.filter(lambda line: line!=movies_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()
        self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]),x[1])).cache()
        # Pre-calculate movies ratings counts
        self.__count_and_average_ratings()

        # Train the model
        self.rank = 8
        self.seed = 5L
        self.iterations = 10
        self.regularization_parameter = 0.1
        self.__train_model() 
