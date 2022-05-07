import time
import pickle
import pandas as pd
import bz2file as bz2
import numpy as np
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
import sklearn.metrics as metrics
import itertools
import seaborn as sns
import re
import nltk
nltk.download('words')
from nltk.stem import SnowballStemmer
from sklearn.preprocessing import StandardScaler
from spacy.lang.en.stop_words import STOP_WORDS
import warnings
warnings.filterwarnings("ignore")
from name_filter import product_name_filter

def decompress_pickle(file):
	data = bz2.BZ2File(file, 'rb')
	data = pickle.load(data)
	return data

loaded_model = pickle.load(open('MODEL.pkl','rb'))
loaded_tfidf = decompress_pickle('TFIDF_1.pbz2')
#loaded_tfidf = pickle.load(open('TFIDF.pkl','rb'))
loaded_kmeans = pickle.load(open('KMEANS.pkl','rb'))


def predictor(news):
    input_data = [news]
    vectorized_input_data = loaded_tfidf.transform(input_data)
    prediction = loaded_model.predict(vectorized_input_data)
    return prediction

def testingKmeans(df,query):
    df = df.replace("",np.nan)
    df = df.dropna()
    df = df.reset_index()
    df = df.drop(['index'],axis=1)
    link=df.Link
    link=link.to_dict()
    name=df.Product_Name
    name=name.to_dict()
    reviews=df.Review
    discount=((df.Actual_Product_Price - df.Product_Price)/df.Actual_Product_Price) * 100
    df['Discount']=discount
    df = df.drop(['Product_Name','Link','Review','Actual_Product_Price'], axis = 1)
    x="The media could not be loaded"
    new_reviews = []
    for i in reviews:
        i = re.sub(x, '.', i)
        new_reviews.append(i)
    words = set(nltk.corpus.words.words())
    k = []
    for i in new_reviews:
        k.append(" ".join(w for w in nltk.wordpunct_tokenize(i)                   if w.lower() in words or not w.isalpha()))
    ps=SnowballStemmer('english'); #stemming object creation

    corpus=[];

    for i in range(0,len(k)):
      review=re.sub('[^a-zA-Z]',' ',k[i])#substitute non-alphabetical characters with space
      review=review.lower()#lower the sentence
      review=review.split()#convert sentence into list of words
      review=[ps.stem(word) for word in review if not word in set(STOP_WORDS)]#stemming and removing stopwords
      review=' '.join(review)
      corpus.append(review)
    arr_prediction = []

    for i in range(0,len(corpus)):
        s=corpus[i]
        arr_prediction.append(predictor(s))

    df['Sentiment'] = pd.DataFrame(arr_prediction)
    
    df=df.astype({'Product_Price': int, 'No_of_ratings': int, 'Discount': int})
    print("After Sentiment Classification:")
    print(df)
    scaler = StandardScaler()
    df=scaler.fit_transform(df)
    df=pd.DataFrame(df,columns=['Product_Price','Product_Rating','No_of_Rating','Discount','Sentiment'])
    print("After Standard Scaling:")
    print(df)
    df.dropna(inplace=True)
    labels=loaded_kmeans.predict(df)

    df=scaler.inverse_transform(df,copy=bool)

    df=pd.DataFrame(df,columns=['Product_Price','Product_Rating','No_of_Rating','Discount','Sentiment'])
    df=df.astype({'Product_Price': int, 'No_of_Rating': int, 'Discount': int, 'Sentiment':int})
    df["Cluster_Labels"]=labels
    print("After Product Analysis:")
    print(df)
    df_best = df[df['Cluster_Labels'] <= 1]
    if(df_best.shape[0] == 0):
        df_best = df[df['Cluster_Labels'] == 3 and df['Sentiment'] == 1]
    best_products_name=[]
    best_products_link=[]
    print("Intermidiate Recommendations:")
    print(df_best)
    index=df_best.index.to_list()
    i=0
    for key,value in name.items():
        for i in range(0,len(index)):
            if key==index[i]:
                best_products_name.append(value)
    i=0
    for key,value in link.items():
        for i in range(0,len(index)):
            if key==index[i]:
                best_products_link.append(value)
    df_best['Name']=best_products_name
    df_best['Link']=best_products_link
    df_best=df_best.drop(['No_of_Rating','Sentiment','Cluster_Labels'],axis=1)
    
    df_best = product_name_filter(df_best,query)
    print("Final Recommendations:")
    print(df_best)
    return df_best
