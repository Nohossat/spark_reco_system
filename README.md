# Spark Recommendation System

A simple recommendation system with ALS algorithm implemented in Apache Spark.

For this project, the movielens dataset has been imported into a MongoDB Atlas instance.

## How does it work ?

From a userID value, we first print out the user preferences then infer some recommendations based on the user ratings.

An application_example.conf file is provided to set the database login values. Rename the application_example.conf to application.conf to use it in the application.

The final model will be updated after a grid search optimization. 

## Install

```shell
git clone https://github.com/Nohossat/spark_reco_system.git
cd spark_reco_system
sbt
compile
run
```

## Improvements

- [ ] Allow the user to choose the userId from the CLI. 
- [ ] Create an UI