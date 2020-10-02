#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 15:42:24 2020

@author: maverick

working with rdds
"""
#%%
from spark101 import SpContext
dataset_folder = "./../../datasets/"

#%%
# read a file in spark
tweet_file = SpContext.textFile(dataset_folder+"movietweets.csv")
#%%
# count number of positive reviews vs number of positive reviews
total_reviews = tweet_file.count() #total reviews

#%%

def categorize_tweets(tweet):
    status = tweet.split(',')
    return (status[0], (status[1], 1))

# def get_feedback_count(tweet_data):
#     return 1

data = (tweet_file.map(categorize_tweets)
                  .mapValues(lambda tweet_data: tweet_data[1])
                .reduceByKey(lambda x, y: int(x) + int(y))
                .collect()
                )

#%%
print(total_reviews)