from flask import Blueprint
main = Blueprint('main', __name__)
import json
from engine import RecommendationEngine

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

@main.route("/turbines", methods=["GET"])
def get_turbines():
    turbines = recommendation_engine.get_turbine_info()
    logger.info(turbines)
    strjson="""[ """
    for turbine in turbines:
    	 logger.info(turbine)
    	 strjson=strjson+"""{"TURBINE_ID":"%s","TURBINE_SN":"%s","EQUIPMENT_TYPE":"%s","EQUIPMENT_MODEL":"%s","EQUIPMENT_NAME":"%s","SITE_ID":"%s","SITE_NAME":"%s","CUSTOMER_ID":"%s"},"""%(turbine[0],turbine[1],turbine[2],turbine[3],turbine[4],turbine[5],turbine[6],turbine[7])
    strjson=strjson[:-1]
    logger.info(turbine)
    strjson=strjson+"""]"""
    return strjson



@main.route("/customer/<customer_id>/<site_id>", methods=["GET"])
def movie_ratings(customer_id,site_id):
    logger.debug("Customer Info %s", customer_id)
    logger.info("Site_id %s", site_id)
    customers = recommendation_engine.get_customer_info(customer_id)
    strjson="""{"CUSTOMER_ID":"%s","CUSTOMER_NAME":"%s","CUSTOMER_ADDRESS1":"%s","CUSTOMER_ADDRESS2":"%s","CUSTOMER_CITY":"%s","CUSTOMER_STATE":"%s","CUSTOMER_COUNTRY":"%s","CUSTOMER_ZIPCODE":"%s","sites":[ """%(customers[0][0],customers[0][1],customers[0][2],customers[0][3],customers[0][4],customers[0][5],customers[0][6],customers[0][7])
    sites = recommendation_engine.get_site_info(site_id)
    logger.info(sites)
    for site in sites:
    	 logger.info(site)
    	 strjson=strjson+"""{"SITE_ID":"%s","SITE_NAME":"%s","SITE_ADDRESS1":"%s","SITE_ADDRESS2":"%s","SITE_CITY":"%s","SITE_STATE":"%s","SITE_COUNTRY":"%s","SITE_ZIPCODE":"%s","CUSTOMER_ID":"%s" }"""%(site[0],site[1],site[2],site[3],site[4],site[5],site[6],site[7],site[8])
    	 logger.info(site)
    strjson=strjson+"""]}"""
    return strjson



def create_app(spark_context, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
