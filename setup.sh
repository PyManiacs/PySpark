#!/usr/bin bash
apt-get update
apt-get install -q -y python-setuptools
easy_install cherrypy flask paste pastedeploy
apt-get install python-pip
pip install numpy
spark-submit $1
