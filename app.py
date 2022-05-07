from urllib import response
from flask import Flask, render_template, request,jsonify,session,redirect,url_for
from numpy import flip
import pandas as pd
from amazon_scrapper import amazon_extractor_script
from flipkart_scrapper import flipkart_extractor_script
from KMeansTest_Final import testingKmeans
from nltk.stem import WordNetLemmatizer
import secrets
import requests
import os
import nltk
nltk.download('omw-1.4')


API_URL = "https://api-inference.huggingface.co/models/facebook/blenderbot-400M-distill"
headers = {"Authorization": "Bearer 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"}

def query(payload):
	response = requests.post(API_URL, headers=headers, json=payload)
	return response.json()

app = Flask(__name__)

app.secret_key = secrets.token_hex(16)

@app.get("/")
def index_get():
    return render_template("base.html")

@app.get("/devpage")
def index_dev():
    return render_template("devspage.html")

@app.post("/predict")
def predict():
    text = request.get_json().get("message")
    text = text.lower()
    text_array = text.split(" ")
    value_set_lower = session.get("set_lower_price")
    value_set_upper = session.get("set_upper_price")
    next_10 = session.get("show_next_10")
    if text.startswith("show the best"):
        try:
            if os.path.exists(session['query']+".csv"):
                os.remove(session['query']+".csv")
        except:
            pass
        session['query'] = " ".join(text_array[5:])
        message = {"answer":"Enter lower price limit"}
        session['set_lower_price'] = 1
    elif value_set_lower==1:
        session['lower_price'] = float("".join(text_array))
        session['set_lower_price'] = 0
        message = {"answer":"Enter upper price limit"}
        session['set_upper_price'] = 1
    elif value_set_upper==1:
        message1 = "The recommended products are : <br>"
        session['upper_price'] = float("".join(text_array))
        session['set_upper_price'] = 0
        try:
            df_amazon = amazon_extractor_script(session['query'],session['lower_price'],session['upper_price'])
        except:
            df_amazon = pd.DataFrame()
        try:
            df_flipkart = flipkart_extractor_script(session['query'],session['lower_price'],session['upper_price'])
        except:
            df_flipkart = pd.DataFrame()
        frames = [df_amazon,df_flipkart]
        df = pd.concat(frames)
        #df.reset_index(inplace=True).drop(['index'])
        print(df)
        print("Total No of Products Extracted",df.shape)
        df.drop_duplicates(subset=['Product_Name','Product_Price','Actual_Product_Price','Product_Rating(5)','No_of_ratings','Review','Link'],keep='first',inplace=True)
        df.sort_values(by='Product_Price',inplace=True)
        df = df.reset_index()
        df = df.drop(columns = ['index'], axis = 1)
        df.drop_duplicates(subset=['Product_Name'],keep='first',inplace=True)
        df.drop_duplicates(subset=['Product_Price','Actual_Product_Price','Product_Rating(5)'],keep='first',inplace=True)
        print("Total No of unique Products",df.shape)
        try:
            df = testingKmeans(df,session['query'])
        except:
            return jsonify({"answer":"No recommended products. Please try a different price range for this product."})
        print("Total No of recommended products",df.shape)
        df.to_csv(session['query']+".csv")
        if(df.shape[0]>10):
            for i in range(10):
               message1 = message1 + "<br>" + "<a " + "target=" + "\"" + "_blank" + "\"" + " href=" + "\"" + df['Link'].iloc[i] + "\"" + ">" + df['Name'].iloc[i] +"</a>" + "<br>"
            message = {"answer":message1}
            session['show_next_10'] = 10
            #session['query'] = ""
            session['lower_price'] = ""
            session['upper_price'] = ""
        else:
            for i in range(df.shape[0]):
               message1 = message1 + "<br>" + "<a " + "target=" + "\"" + "_blank" + "\"" + " href=" + "\"" + df['Link'].iloc[i] + "\"" + ">" + df['Name'].iloc[i] +"</a>" + "<br>"
            message1 = message1 + "<br><b>There are no more products</b></br>"
            message = {"answer":message1}
            session['show_next_10'] = 0
            session['query'] = ""
            session['lower_price'] = ""
            session['upper_price'] = ""
    elif text.split(" ")[0]=="show" and text.split(" ")[1]=="more" and next_10!=0:
        df = pd.read_csv(session['query']+".csv")
        message1 = "The recommended products are : <br>"
        if(df.shape[0]<next_10+10):
            for i in range(10,df.shape[0]):
               message1 = message1 + "<br>" + "<a " + "target=" + "\"" + "_blank" + "\"" + " href=" + "\"" + df['Link'].iloc[i] + "\"" + ">" + df['Name'].iloc[i] +"</a>" + "<br>"
            message1 = message1 + "<br><b>There are no more products</b></br>"
            message = {"answer":message1}
            session['show_next_10'] = 0
            os.remove(session['query']+".csv")
            session['query'] = ""
        else:
            for i in range(10,next_10+10):
               message1 = message1 + "<br>" + "<a " + "target=" + "\"" + "_blank" + "\"" + " href=" + "\"" + df['Link'].iloc[i] + "\"" + ">" + df['Name'].iloc[i] +"</a>" + "<br>"
            message = {"answer":message1}
            session['show_next_10'] = next_10+10
    elif text.split(" ")[0]=="bye":
        message = {"answer":"Bye, Have a Nice Day!!!"}
        try:
            directory = os.getcwd()
            print(directory)
            files = os.listdir(directory)
            files_filter = [file for file in files if file.endswith(".csv")]
            for file in files_filter:
                path_to_file = os.path.join(directory,file)
                os.remove(path_to_file)
            session['show_next_10'] = 0
            session['query'] = ""
            session['lower_price'] = ""
            session['upper_price'] = ""
        except:
            pass
    elif "name" in text:
        message = {"answer":"My name is Perry...What name do you have?"}
    elif "hi" in text or "hello" in text or "hey" in text:
        message = {"answer":"Hi, I am Perry...Chat with me to know more... :)"}
    else:
        output = query({
            "inputs": {
                "text": text
            },
        })
        message = {"answer":output['generated_text']}
    return jsonify(message)

if __name__=="__main__":
    app.run(debug=True)
