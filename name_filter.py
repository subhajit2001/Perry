from nltk.stem import WordNetLemmatizer
 
def product_name_filter(df,query):
    lemmatizer = WordNetLemmatizer()
    names_list = df['Name'].to_list()
    query_tokens = query.lower().split()
    query_tokens = [lemmatizer.lemmatize(i) for i in query_tokens]
    prob_list = []
    prob_query = len(query_tokens)
    for i in names_list:
        name_tokens = i.lower().split()
        name_tokens = [lemmatizer.lemmatize(i) for i in name_tokens]
        name_tokens = sorted(set(name_tokens)) 
        count=0
        prob = 0
        for j in name_tokens:
            count += query_tokens.count(j)
        prob = count/prob_query
        prob_list.append(prob)
    df['Name_probability'] = prob_list
    
    df.sort_values(by=['Name_probability','Product_Price'],ascending=[False,True],inplace=True)
    print("After Probabilistic Keyword Analysis:")
    print(df)
    df.drop(['Name_probability'],axis=1,inplace=True)
    return df
    
    
                    
            