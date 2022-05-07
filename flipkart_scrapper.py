import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import numpy as np
from multiprocessing import cpu_count,Pool
from concurrent.futures import ThreadPoolExecutor

def download_and_extract_product_data(url):
    proxylist = ['46.4.96.137:1080', '198.199.86.148:47638', '31.7.232.178:1080']
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 OPR/78.0.4093.153","Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
    for proxy in proxylist:
        try:
            page = requests.get(url,headers=headers,proxies={'http://':proxy,'https://':proxy},timeout=5)
            if page.status_code==200:
                break
            else:
                continue
        except:
            download_and_extract_product_data(url)
    soup1 = BeautifulSoup(page.content,"html.parser")
    soup2 = BeautifulSoup(soup1.prettify(), "html.parser")
    product_data_list = []
    if soup2.find('div',{'class':'_2kHMtA'}) is not None:
        product_data = soup2.find_all('div',{'class':'_1AtVbE col-12-12'})
        product_data_dict = {}
        for item in product_data:
            flag=0
            try:
                name = item.find(class_="_4rR01T").get_text().strip()
            except:
                name = np.nan
            try:
                price = float(item.find(class_="_30jeq3 _1_WHN1").get_text().strip().replace(',','').replace('₹',''))
            except:
                price = np.nan
            try:
                actual_price = float((item.find(class_="_3I9_wc _27UcVY")).get_text().strip().replace(' ','').replace('\n','').replace(',','').replace('₹',''))
            except:
                actual_price = np.nan
            try: 
                rating_pos = float(str(item.find(class_="_3LWZlK")).split()[2])
            except:
                rating_pos = np.nan
            try:
                no_of_ratings = str(item.find(class_="_2_R_DZ")).split()
                no_of_ratings_pos = float(no_of_ratings[4].replace(',',''))
            except:
                no_of_ratings_pos = np.nan
            try:           
                no_of_reviews_pos = float(no_of_ratings[12].replace(',',''))
            except:
                no_of_reviews_pos = np.nan
            try:
                link = item.find(class_="_1fQZEK")['href'].strip()
            except:
                link  = np.nan
            product_data_dict = {
                'Product_Name': name,
                'Product_Price': price,
                'Actual_Product_Price': actual_price,
                'Product_Rating(5)':rating_pos,
                'No_of_ratings':no_of_ratings_pos,
                'No_of_reviews':no_of_reviews_pos,
                'Link': link,
                'Position':"Vertical"
                }                
            product_data_list.append(product_data_dict)
        return product_data_list
    else:
        product_data = soup2.find_all('div',{'class':'_4ddWXP'})
        pos = "Horizontal"
        if product_data==[]:
            product_data = soup2.find_all('div',{'class':'_1xHGtK _373qXS'})
            pos = "diff horizontal"
        #print(product_data)
        for item in product_data:
            try:
                name = item.find(class_="s1Q9rs")['title'].strip()
            except:
                try:
                    name = item.find(class_="IRpwTa")['title'].strip()
                except:
                    name = np.nan
            try:
                price = float(item.find(class_="_30jeq3").get_text().strip().replace(",","").replace("₹",""))
            except:
                price = np.nan
            try:
                actual_price = float(item.find(class_="_3I9_wc").get_text().strip().replace("\n","").replace(" ","").replace("₹","").replace(",",""))
            except:
                actual_price = np.nan
            try:
                link = item.find(class_="s1Q9rs")['href'].strip()
            except:
                try:
                    link = item.find(class_="IRpwTa")['href'].strip()
                except:
                    link = np.nan
            try:
                rating = float(item.find(class_="_3LWZlK").get_text().strip())
            except:
                rating = np.nan
                if pos=="diff horizontal":
                    rating = 0
            try:
                no_rating = float(item.find(class_="_2_R_DZ").get_text().replace("(","").replace(")","").replace(",","").strip())
            except:
                no_rating = np.nan
                if pos=="diff horizontal":
                    no_rating = 0
            product_data_dict = {
                'Product_Name':name,
                'Product_Price': price,
                'Actual_Product_Price': actual_price,
                'Product_Rating(5)':rating,
                'No_of_ratings':no_rating,
                'Link': link,
                'Position':pos
                }
            product_data_list.append(product_data_dict)
        print(".",end="",flush=True)
        return product_data_list

def download_and_extract_product_review_data(url):
    reviewlist = []
    review_dict = {
        'Product_Name':"",
        'Product_Review': "",
    }
    proxylist = ['46.4.96.137:1080', '198.199.86.148:47638', '31.7.232.178:1080']
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 OPR/78.0.4093.153","Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
    for proxy in proxylist:
        try:
            page = requests.get(url,headers=headers,proxies={'http://':proxy,'https://':proxy}, timeout=5)
            if page.status_code==200:
                break
            else:
                download_and_extract_product_review_data(url)
        except:
            download_and_extract_product_review_data(url)
    soup1 = BeautifulSoup(page.content,"html.parser")
    soup2 = BeautifulSoup(soup1.prettify(), "html.parser")
    #print("In review")
    reviews = soup2.find_all('div',{'class':'_1AtVbE col-12-12'})
    if reviews==[]:
        reviews = soup2.find_all('div',{'class':'t-ZTKy _1QgsS5'})
    try:
        product_review = ""
        for item in reviews:
            try:
                product_name = soup2.find(class_="s1Q9rs")['title']
            except:
                product_name = ""
            try:
                try:
                    review = str(item.find(class_="_6K-7Co").get_text().strip())
                except:
                    review = str(item.find(class_="t-ZTKy").get_text().strip())
            except:
                review = ""
            product_review = product_review + " " + review
        review_dict = {
            'Product_Name':url.split('/')[3],
            'Product_Review': product_review,
            'Links_Changed':url,
            }
        reviewlist.append(review_dict)
    except:
        pass
    print(".",end="",flush=True)
    return reviewlist

def individual_product_data(link):
    proxylist = ['51.81.83.181:59427', '5.182.39.88:9988', '13.250.64.147:48540']
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 OPR/78.0.4093.153","Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
    try:
        for proxy in proxylist:
            page = requests.get(link,headers=headers,proxies={'http://':proxy,'https://':proxy}, timeout=5)
            if page.status_code==200:
                break
            else:
                continue
    except:
        return individual_product_data(link)
    soup1 = BeautifulSoup(page.content,"html.parser")
    soup = BeautifulSoup(soup1.prettify(), "html.parser")
    try:
        string = soup.find('span',{'class':'_2_R_DZ'}).get_text().strip()
        no_rating = float(string.split(" ")[0].replace(",",""))
        rating = soup.find(class_="_3LWZlK _3uSWvT").get_text().strip()
    except:
        no_rating = np.nan
        rating = np.nan
    rating_dict = {
        'Product_Rating(5)':rating,
        'No_of_ratings':no_rating,
        'Link':link
    }
    print(".",end="",flush=True)
    return rating_dict


def flipkart_extractor_script(keyword,lower_limit,upper_limit):
    #starting time of execution
    time_start = time.time()
    #initializing empty final product list
    product_data_list_final = []
    url_array = []
    
    keyword_array = keyword.split(" ")
    query = ""
    i=0
    for string in keyword_array:
        if i==0:
            query = query + string
            i+=1
        else:
            query = query + "+" + string
    
    x=1
    while x<3:
        url = 'https://www.flipkart.com/search?q='+query+'&page='+str(x)
        url_array.append(url)
        x=x+1
        url = ''

    print("Extracting products for",keyword)
    
    with ThreadPoolExecutor(max_workers=10) as p:
        soup_var = p.map(download_and_extract_product_data,url_array)
    
    for i in list(soup_var):
        product_data_list_final.extend(i)
    df = pd.DataFrame(product_data_list_final)
    pos = df['Position'].to_list()[0]
    df = df[(df['Product_Price']>=lower_limit) & (df['Product_Price']<=upper_limit)]
        
    links = df['Link'].to_list()
    link_array = []
    
    for link in links:
        url_arr = link.split("/")
        url_arr1 = url_arr[3].split('?')
        try:
            url_arr2 = url_arr1[1].split('&')
        except:
            print(link)
            exit()
        url_form = "https://www.flipkart.com/"+url_arr[1]+"/product-reviews/"+url_arr1[0]+"?"+url_arr2[0]+"&"+url_arr2[1]+"&"+url_arr2[2]
        link_array.append(url_form)
 
    df['Links_Changed'] = link_array
    
    df = df.replace('',np.nan)
    df.dropna(inplace=True)
    link_array = df['Links_Changed'].to_list()
    
    with ThreadPoolExecutor(max_workers=10) as s:
        soup_var = s.map(download_and_extract_product_review_data,link_array)
     
    list_final = []
    review_list_final = []
    for i in list(soup_var):
        list_final.extend(i)
    
    df_review = pd.DataFrame(list_final)
    
    df = pd.merge(df, df_review, on='Links_Changed')
    df['Link'] = "https://www.flipkart.com"+df['Link']
    if pos=="diff horizontal":
        urls = df['Link'].to_list()
        df.drop(['Product_Rating(5)','No_of_ratings'],axis=1,inplace=True)
        with ThreadPoolExecutor(max_workers=10) as p:
            soup_var = p.map(individual_product_data,urls)
        list_final1 = []
        for i in list(soup_var):
            list_final1.append(i)
        df_rating = pd.DataFrame(list_final1)
        df = pd.merge(df, df_rating, on='Link')
        
    #print(df)
    
    df.drop(['Links_Changed'],axis=1,inplace=True)
  
    df = df[['Product_Name_x','Product_Price','Actual_Product_Price','Product_Rating(5)','No_of_ratings','Product_Review','Link']]
    df.columns = ['Product_Name','Product_Price','Actual_Product_Price','Product_Rating(5)','No_of_ratings','Review','Link']
    time_end = time.time()
    time_diff = time_end - time_start
    print()
    print("Structure of Flipkart Pages : ",pos)
    print("The Extracted Flipkart DataFrame size : ",df.shape)
    print("Time of Execution : ",time_diff)
    return df