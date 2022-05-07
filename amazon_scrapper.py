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
            #page = s.get(url,headers=headers,proxies={'http://':proxy,'https://':proxy},timeout=2)
            if page.status_code==200:
                break
            else:
                continue
        except:
            download_and_extract_product_data(url)
    soup1 = BeautifulSoup(page.content,"html.parser")
    soup2 = BeautifulSoup(soup1.prettify(), "html.parser")
    product_data_list = []
    product_data = soup2.find_all('div',{'data-component-type':'s-search-result'})
    product_data_dict = {}
    for item in product_data:
        try:
            product_name = item.find(class_="a-size-medium a-color-base a-text-normal").get_text().strip()
        except:
            product_name = ""
        try:
            product_asin = item.find(class_="a-link-normal s-no-outline")['href'].strip().split("/")[3]
        except:
            product_asin = ""
        try:
            product_price = float(item.find(class_="a-price-whole").get_text().strip().replace(',','').replace('₹',''))
        except:
            product_price = np.nan
        try:
            actual_product_price = float(((item.find(class_="a-price a-text-price")).find(class_="a-offscreen")).get_text().strip().replace(' ','').replace('\n','').replace(',','').replace('₹',''))
        except:
            actual_product_price = np.nan
        try:
            product_rating = float(item.find(class_="a-icon-alt").get_text().strip().strip(" out of 5 stars"))
        except:
            product_rating = np.nan
        try:
            no_of_ratings = int((item.find(class_="a-row a-size-small")).find(class_="a-size-base").get_text().strip().replace(',',''))
        except:
            no_of_ratings = np.nan
        try:
            link = 'https://www.amazon.in'+item.find(class_="a-link-normal s-no-outline")['href'].strip()
        except:
            link = ""
        if product_name!="":
            grid = "Vertical"
            product_data_dict = {
            'Product_Name':product_name,
            'Product_ASIN':product_asin,
            'Product_Price': product_price,
            'Actual_Product_Price': actual_product_price,
            'Product_Rating(5)': product_rating,
            'No_of_ratings': no_of_ratings,
            'Link': link,
            }
        else:
            grid = "Horizontal"
            product_name = item.find(class_="a-size-base-plus a-color-base a-text-normal").get_text().strip()
            product_data_dict = {
            'Product_Name':product_name,
            'Product_ASIN':product_asin,
            'Product_Price': product_price,
            'Actual_Product_Price': actual_product_price,
            'Product_Rating(5)': product_rating,
            'No_of_ratings': no_of_ratings,
            'Link': link,
            }
        product_data_list.append(product_data_dict)
        if len(product_data_list)==0:
            download_and_extract_product_data(url)
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
            #page = s.get(url,headers=headers,proxies={'http://':proxy,'https://':proxy},timeout=2)
            if page.status_code==200:
                break
            else:
                download_and_extract_product_review_data(url)
        except:
            download_and_extract_product_review_data(url)
    soup1 = BeautifulSoup(page.content,"html.parser")
    soup2 = BeautifulSoup(soup1.prettify(), "html.parser")
    #print(soup2)
    reviews = soup2.find_all('div',{'data-hook':'review'})
    try:
        product_review = ""
        for item in reviews:
            try:
                product_name = soup2.title.text.replace('Amazon.in:Customer reviews: ','').strip()
            except:
                product_name = ""
            try:
                review = item.find('span',{'data-hook':'review-body'}).get_text().strip()
            except:
                review = ""
            product_review = product_review + " " + review
        review_dict = {
        'Product_Name':product_name,
        'Product_ASIN':url.split("/")[5],
        'Product_Review': product_review,
        }
        reviewlist.append(review_dict)
    except:
        pass
    print(".",end="",flush=True)
    return reviewlist

def amazon_extractor_script(keyword,lower_limit,upper_limit):
    #keyword=input("Enter a keyword : ")
    # = requests.Session()
    time_start = time.time()
    product_data_list_final = []
    url_array = []
    
    x=1
    while x<=3:
        url = 'https://www.amazon.in/s?k='+keyword+'&page='+str(x)+'&ref=sr_pg_'+str(x)
        url_array.append(url)
        x=x+1

    print("Extracting products for",keyword)
    
    with ThreadPoolExecutor(max_workers=10) as p:
        soup_var = p.map(download_and_extract_product_data,url_array)
    
    for i in list(soup_var):
        product_data_list_final.extend(i)
    df = pd.DataFrame(product_data_list_final)
    
    df = df[(df['Product_Price']>=lower_limit) & (df['Product_Price']<=upper_limit)]
    
    links = df['Link'].to_list()
    link_array = []
    for link in links:
        url_array = link.split("/")
        if url_array[3]!="gp":
            url_1 = 'https://www.amazon.in/'+url_array[3]+'/product-reviews/'+url_array[5]+'/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
            link_array.append(url_1)
        else:
            link_array.append("")
    df['Links_Changed'] = link_array
    df = df.replace('',np.nan)
    df.dropna(inplace=True)
    link_array = df['Links_Changed'].to_list()
    with ThreadPoolExecutor(max_workers=10) as p:
        soup_var = p.map(download_and_extract_product_review_data,link_array)
    
    list_final = []
    review_list_final = []
    for i in list(soup_var):
        list_final.extend(i)
    df_review = pd.DataFrame(list_final)
    
    df = pd.merge(df, df_review, on='Product_ASIN')
    
    df.drop(['Links_Changed'],axis=1,inplace=True)
    time_end = time.time()
    time_diff = time_end - time_start
    print()
    df = df[['Product_Name_y','Product_Price','Actual_Product_Price','Product_Rating(5)','No_of_ratings','Product_Review','Link']]
    df.columns = ['Product_Name','Product_Price','Actual_Product_Price','Product_Rating(5)','No_of_ratings','Review','Link']
    print("The Extracted Flipkart DataFrame size : ",df.shape)
    print("Time of Execution : ",time_diff)
    return df