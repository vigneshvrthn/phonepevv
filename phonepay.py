#importing packages 
import pandas as pd
import psycopg2 as sql
import streamlit as st
import plotly.express as px
import os
import json
import requests
conection=sql.connect(host="localhost",user="postgres",port=5432,password="vignesh",database="phonepepulse")
cursor=conection.cursor()

# Clone the Phonepe Pulse repository
#in terminal use git clone (the usrl link) https://github.com/PhonePe/pulse.git

#extracting data from aggregated transaction and converting into dataframe and cleaning and preprocessing the data and insert the datas into sql table format 
path1="C:/Users/krish/OneDrive/Desktop/project/phonepe/pulse/data/aggregated/transaction/country/india/state"
state_list = os.listdir(path1)
clm={"states":[],"years":[],"quater":[],"name_of_transactions":[],"transaction_counts":[],"transaction_amts":[]}
for state in state_list:
    cur_state = path1 + "/"+state + "/"
    agg_year_list = os.listdir(cur_state)
    for year in agg_year_list:
        cur_year=cur_state+year +"/"
        file=os.listdir(cur_year)
        for files in file:
            cur_file=cur_year+files
            data=open(cur_file,'r')
            f_open=json.load(data)
            for z in f_open["data"]["transactionData"]:
                transaction_name1=z["name"]
                transaction_count1=z["paymentInstruments"][0]["count"]
                transaction_amt1=z["paymentInstruments"][0]["amount"]
                clm["states"].append(state)
                clm["years"].append(year)
                clm["quater"].append(files.strip(".json"))
                clm["name_of_transactions"].append(transaction_name1)
                clm["transaction_counts"].append(transaction_count1)
                clm["transaction_amts"].append(transaction_amt1)
agg_tran_df=pd.DataFrame(clm)
agg_tran_df.index=range(1,len(agg_tran_df)+1)
agg_tran_df=agg_tran_df.replace({"states":{"andaman-&-nicobar-islands":"Andaman & Nicobar",'andhra-pradesh':'Andhra Pradesh','arunachal-pradesh':'Arunachal Pradesh',
                                'assam':'Assam','bihar':'Bihar','chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
                                'delhi':'Delhi', 'goa':'Goa','gujarat':'Gujarat','haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh',
                                'jammu-&-kashmir':'Jammu & Kashmir','jharkhand':'Jharkhand','karnataka':'Karnataka', 'kerala':'Kerala',
                                'ladakh':'Ladakh','lakshadweep':'Lakshadweep','madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra',
                                'manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram','nagaland':'Nagaland','odisha':'Odisha',
                                'puducherry':'Puducherry','punjab':'Punjab','rajasthan':'Rajasthan','telangana':'Telangana','sikkim':'Sikkim',
                                'tamil-nadu':'Tamil Nadu','tripura':"Tripura",'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand','west-bengal':'West Bengal'}})
dr_querry="""drop table if exists Aggregation_Transaction"""
cursor.execute(dr_querry)
conection.commit()
query="""create table Aggregation_Transaction (states varchar(100),year int,quater int,transaction_type varchar(50),transaction_count bigint, amt bigint)"""
cursor.execute(query)
conection.commit()
for _, j in agg_tran_df.iterrows():
    in_query = '''INSERT INTO Aggregation_Transaction (states, year, quater, transaction_type,transaction_count, amt) VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (j["states"], j["years"], j["quater"], j["name_of_transactions"],j["transaction_counts"], j["transaction_amts"]) 
    cursor.execute(in_query, values)
    conection.commit()
#extracting data from aggregated user and converting into dataframe and cleaning and preprocessing the data and insert the datas into sql table format 
conection=sql.connect(host="localhost",user="postgres",port=5432,password="vignesh",database="phonepepulse")
cursor=conection.cursor()
clm2={"state":[],"year":[],"quater":[],"brand":[],"no_of_users":[],"precentage":[]}
path2="C:/Users/krish/OneDrive/Desktop/project/phonepe/pulse/data/aggregated/user/country/india/state"
agg_user_list=os.listdir(path2)
for state in agg_user_list:
    path2_state=path2+"/"+state+"/"
    agg_year=os.listdir(path2_state)
    for year in agg_year:
        path2_state_year=path2_state+year+"/"
        agg_file=os.listdir(path2_state_year)
        for files in agg_file:
            path2_state_year_file=path2_state_year+files
            d=open(path2_state_year_file,"r")
            f1_open=json.load(d)          
            try:
                for i in f1_open["data"]["usersByDevice"]:
                    brand_name = i["brand"]
                    counts = i["count"]
                    percents = i["percentage"]
                    clm2["state"].append(state)
                    clm2["year"].append(year)
                    clm2["quater"].append(files.strip(".json"))
                    clm2["brand"].append(brand_name)
                    clm2["no_of_users"].append(counts)
                    clm2["precentage"].append(percents)
            except:
                pass
agg_user_df=pd.DataFrame(clm2)
agg_user_df=agg_user_df.replace({"state":{"andaman-&-nicobar-islands":"Andaman & Nicobar",'andhra-pradesh':'Andhra Pradesh','arunachal-pradesh':'Arunachal Pradesh',
                                'assam':'Assam','bihar':'Bihar','chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
                                'delhi':'Delhi', 'goa':'Goa','gujarat':'Gujarat','haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh',
                                'jammu-&-kashmir':'Jammu & Kashmir','jharkhand':'Jharkhand','karnataka':'Karnataka', 'kerala':'Kerala',
                                'ladakh':'Ladakh','lakshadweep':'Lakshadweep','madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra',
                                'manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram','nagaland':'Nagaland','odisha':'Odisha',
                                'puducherry':'Puducherry','punjab':'Punjab','rajasthan':'Rajasthan','telangana':'Telangana','sikkim':'Sikkim',
                                'tamil-nadu':'Tamil Nadu','tripura':"Tripura",'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand','west-bengal':'West Bengal'}})
dr_querry1="""drop table if exists Aggregation_User"""
cursor.execute(dr_querry1)
conection.commit()
querr="""create table Aggregation_User (state varchar(100),year int,quater int,brand varchar(25),no_of_user bigint,precentage float)"""
cursor.execute(querr)
conection.commit()
for i,j in agg_user_df.iterrows():
    inquerr="""insert into Aggregation_User (state,year,quater,brand,no_of_user,precentage) values (%s ,%s ,%s ,%s ,%s ,%s)"""
    values=(j["state"],j["year"],j["quater"],j["brand"],j["no_of_users"],j["precentage"])
    cursor.execute(inquerr,values)
    conection.commit()



#extracting data from map transaction and converting into dataframe and cleaning and preprocessing the data and insert the datas into sql table format 
clm3={"state":[],"year":[],"quater":[],"dristict":[],"transaction_count":[],"transaction_amt":[]}
path3="C:/Users/krish/OneDrive/Desktop/project/phonepe/pulse/data/map/transaction/hover/country/india/state"
st_list=os.listdir(path3)
for state in st_list:
    path_st=path3+"/"+state+"/"
    agg_path_st=os.listdir(path_st)
    for year in agg_path_st:
        path_st_ye=path_st+year+"/"
        agg_path_st_ye=os.listdir(path_st_ye)
        for file in agg_path_st_ye:
            file_path=path_st_ye+file
            data=open(file_path,"r")
            d=json.load(data)
            for z in (d["data"]["hoverDataList"]):
                dristict=z["name"]
                tran_count=z["metric"][0]["count"]
                tran_amt=z["metric"][0]["amount"]
                clm3["state"].append(state)
                clm3["year"].append(year)
                clm3["quater"].append(file.strip(".json"))
                clm3["dristict"].append(dristict)
                clm3["transaction_count"].append(tran_count)
                clm3["transaction_amt"].append(tran_amt)
map_tran_df=pd.DataFrame(clm3)
map_tran_df=map_tran_df.replace({"state":{"andaman-&-nicobar-islands":"Andaman & Nicobar",'andhra-pradesh':'Andhra Pradesh','arunachal-pradesh':'Arunachal Pradesh',
                                'assam':'Assam','bihar':'Bihar','chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
                                'delhi':'Delhi', 'goa':'Goa','gujarat':'Gujarat','haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh',
                                'jammu-&-kashmir':'Jammu & Kashmir','jharkhand':'Jharkhand','karnataka':'Karnataka', 'kerala':'Kerala',
                                'ladakh':'Ladakh','lakshadweep':'Lakshadweep','madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra',
                                'manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram','nagaland':'Nagaland','odisha':'Odisha',
                                'puducherry':'Puducherry','punjab':'Punjab','rajasthan':'Rajasthan','telangana':'Telangana','sikkim':'Sikkim',
                                'tamil-nadu':'Tamil Nadu','tripura':"Tripura",'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand','west-bengal':'West Bengal'}})
dr_querry="""drop table if exists Map_Transaction"""
cursor.execute(dr_querry)
cr_query="""create table Map_Transaction (states varchar(100), year int ,quater int, dristict varchar(100),transaction_count bigint, amt bigint)"""
cursor.execute(cr_query)
for i,j in map_tran_df.iterrows():
    in_query="""insert into Map_Transaction(states,year,quater,dristict,transaction_count,amt) values(%s,%s,%s,%s,%s,%s)"""
    values=(j["state"],j["year"],j["quater"],j["dristict"],j["transaction_count"],j["transaction_amt"])
    cursor.execute(in_query,values)
    conection.commit()

#extracting data from map user and converting into dataframe and cleaning and preprocessing the data and insert the datas into sql table format 
clm4={"state":[],"year":[],"quater":[],"dristict":[],"no_of_user":[]}
path4="C:/Users/krish/OneDrive/Desktop/project/phonepe/pulse/data/map/user/hover/country/india/state"
map_st_list=os.listdir(path4)
for state in map_st_list:
    path4_st=path4+"/"+state+"/"
    map_year=os.listdir(path4_st)
    for year in map_year:
        path_st_ye=path4_st+year+"/"
        map_file=os.listdir(path_st_ye)
        for file in map_file:
            path_st_ye_f=path_st_ye+file
            d=open(path_st_ye_f,"r")
            data=json.load(d)
            for i in data["data"]["hoverData"].items():
                dristict=i[0]
                no_user=i[1]["registeredUsers"]
                clm4["state"].append(state)
                clm4["year"].append(year)
                clm4["quater"].append(file.strip(".json"))
                clm4["dristict"].append(dristict)
                clm4["no_of_user"].append(no_user)
map_user_df=pd.DataFrame(clm4)
map_user_df=map_user_df.replace({"state":{"andaman-&-nicobar-islands":"Andaman & Nicobar",'andhra-pradesh':'Andhra Pradesh','arunachal-pradesh':'Arunachal Pradesh',
                                'assam':'Assam','bihar':'Bihar','chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
                                'delhi':'Delhi', 'goa':'Goa','gujarat':'Gujarat','haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh',
                                'jammu-&-kashmir':'Jammu & Kashmir','jharkhand':'Jharkhand','karnataka':'Karnataka', 'kerala':'Kerala',
                                'ladakh':'Ladakh','lakshadweep':'Lakshadweep','madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra',
                                'manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram','nagaland':'Nagaland','odisha':'Odisha',
                                'puducherry':'Puducherry','punjab':'Punjab','rajasthan':'Rajasthan','telangana':'Telangana','sikkim':'Sikkim',
                                'tamil-nadu':'Tamil Nadu','tripura':"Tripura",'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand','west-bengal':'West Bengal'}})

dr_querry="""drop table if exists Map_User"""
cursor.execute(dr_querry)
cr_query="""create table Map_User (state varchar(50),year int,quater int, dristict varchar,no_of_user bigint)"""
cursor.execute(cr_query)
for i,j in map_user_df.iterrows():
    in_query="""insert into Map_User(state,year,quater,dristict,no_of_user) values(%s,%s,%s,%s,%s)"""
    values=(j["state"],j["year"],j["quater"],j["dristict"],j["no_of_user"])
    cursor.execute(in_query,values)
    conection.commit()




#extracting data from top transaction and converting into dataframe and cleaning and preprocessing the data and insert the datas into sql table format 
clm5={"state":[],"year":[],"quater":[],"dristict":[],"transaction_counts":[],"transaction_amts":[]}
path5="C:/Users/krish/OneDrive/Desktop/project/phonepe/pulse/data/top/transaction/country/india/state/"
top_tr_st=os.listdir(path5)
for state in top_tr_st:
    path5_st=path5+state+"/"
    top_year=os.listdir(path5_st)
    for year in top_year:
        path5_st_y=path5_st+year+"/"
        top_year_file=os.listdir(path5_st_y)
        for file in top_year_file:
            path5_st_y_f=path5_st_y+file
            d=open(path5_st_y_f,"r")
            data=json.load(d)
            for z in data["data"]["districts"]:
                dristict=z["entityName"]
                count=z["metric"]["count"]
                amt=z["metric"]["amount"]
                clm5["state"].append(state)
                clm5["year"].append(year)
                clm5["quater"].append(file.strip(".json"))
                clm5["dristict"].append(dristict)
                clm5["transaction_counts"].append(count)
                clm5['transaction_amts'].append(amt)
top_tr_df=pd.DataFrame(clm5)

top_tr_df=top_tr_df.replace({"state":{"andaman-&-nicobar-islands":"Andaman & Nicobar",'andhra-pradesh':'Andhra Pradesh','arunachal-pradesh':'Arunachal Pradesh',
                                'assam':'Assam','bihar':'Bihar','chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
                                'delhi':'Delhi', 'goa':'Goa','gujarat':'Gujarat','haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh',
                                'jammu-&-kashmir':'Jammu & Kashmir','jharkhand':'Jharkhand','karnataka':'Karnataka', 'kerala':'Kerala',
                                'ladakh':'Ladakh','lakshadweep':'Lakshadweep','madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra',
                                'manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram','nagaland':'Nagaland','odisha':'Odisha',
                                'puducherry':'Puducherry','punjab':'Punjab','rajasthan':'Rajasthan','telangana':'Telangana','sikkim':'Sikkim',
                                'tamil-nadu':'Tamil Nadu','tripura':"Tripura",'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand','west-bengal':'West Bengal'}})
dr_querry='''drop table if exists Top_Transaction'''
cursor.execute(dr_querry)
cr_query='''create table  Top_Transaction(states varchar(100),year int,quater int,dristict varchar,transaction_count bigint,amt bigint)'''
cursor.execute(cr_query)
for i, j in top_tr_df.iterrows():
    in_query="""insert into Top_Transaction(states,year,quater,dristict,transaction_count,amt) values(%s,%s,%s,%s,%s,%s)"""
    values=(j["state"],j["year"],j["quater"],j["dristict"],j["transaction_counts"],j["transaction_amts"])
    cursor.execute(in_query,values) 
    conection.commit()




#extracting data from top user and converting into dataframe and cleaning and preprocessing the data and insert the datas into sql table format 
clm6={"state":[],"year":[],"quater":[],"dristict":[],"no_of_user":[]}
path6="C:/Users/krish/OneDrive/Desktop/project/phonepe/pulse/data/top/user/country/india/state/"
tu_st_list=os.listdir(path6)
for state in tu_st_list:
    path6_st=path6+state+"/"
    ye_list=os.listdir(path6_st)
    for year in ye_list:
        path6_st_ye=path6_st+year+"/"
        file_list=os.listdir(path6_st_ye)
        for file in file_list:
            path6_st_ye_f=path6_st_ye+file
            d=open(path6_st_ye_f,"r")
            data=json.load(d)
            for i in data["data"]["districts"]:
                dristict=i["name"]
                users=i["registeredUsers"]
                clm6["state"].append(state)
                clm6["year"].append(year)
                clm6["quater"].append(file.strip(".json"))
                clm6["dristict"].append(dristict)
                clm6["no_of_user"].append(users)
top_user_df=pd.DataFrame(clm6)
top_user_df=top_user_df.replace({"state":{"andaman-&-nicobar-islands":"Andaman & Nicobar",'andhra-pradesh':'Andhra Pradesh','arunachal-pradesh':'Arunachal Pradesh',
                                'assam':'Assam','bihar':'Bihar','chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
                                'delhi':'Delhi', 'goa':'Goa','gujarat':'Gujarat','haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh',
                                'jammu-&-kashmir':'Jammu & Kashmir','jharkhand':'Jharkhand','karnataka':'Karnataka', 'kerala':'Kerala',
                                'ladakh':'Ladakh','lakshadweep':'Lakshadweep','madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra',
                                'manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram','nagaland':'Nagaland','odisha':'Odisha',
                                'puducherry':'Puducherry','punjab':'Punjab','rajasthan':'Rajasthan','telangana':'Telangana','sikkim':'Sikkim',
                                'tamil-nadu':'Tamil Nadu','tripura':"Tripura",'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand','west-bengal':'West Bengal'}})
dr_querry="""drop table if exists Top_User"""
cursor.execute(dr_querry)
cr_query='''create table Top_User (state varchar (100),year int,quater int,dristict varchar(100),no_of_user bigint)'''
cursor.execute(cr_query)
for i,j in top_user_df.iterrows():
    in_query="""insert into Top_User (state,year,quater,dristict,no_of_user) values (%s,%s,%s,%s,%s)"""
    values=(j["state"],j["year"],j["quater"],j["dristict"],j["no_of_user"])
    cursor.execute(in_query,values)
    conection.commit()

#extracting data from aggregation  user  appopens and converting into dataframe and cleaning and preprocessing the data and insert the datas into sql table format 
clm7={"state":[],"year":[],"quater":[],"no_of_users":[],"appOpens":[]}
path2="C:/Users/krish/OneDrive/Desktop/project/phonepe/pulse/data/aggregated/user/country/india/state"
agg_user_list=os.listdir(path2)
for state in agg_user_list:
    path2_state=path2+"/"+state+"/"
    agg_year=os.listdir(path2_state)
    for year in agg_year:
        path2_state_year=path2_state+year+"/"
        agg_file=os.listdir(path2_state_year)
        for files in agg_file:
            path2_state_year_file=path2_state_year+files
            d=open(path2_state_year_file,"r")
            f1_open=json.load(d)
            app= f1_open["data"]["aggregated"]["appOpens"]
            user=f1_open["data"]["aggregated"]["registeredUsers"]
            clm7["state"].append(state)
            clm7["year"].append(year)
            clm7["quater"].append(files.strip(".json"))
            clm7["appOpens"].append(app)
            clm7["no_of_users"].append(user)
agg_userd_df=pd.DataFrame(clm7)
agg_userd_df
agg_userd_df=agg_userd_df.replace({"state":{"andaman-&-nicobar-islands":"Andaman & Nicobar",'andhra-pradesh':'Andhra Pradesh','arunachal-pradesh':'Arunachal Pradesh',
                                'assam':'Assam','bihar':'Bihar','chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
                                'delhi':'Delhi', 'goa':'Goa','gujarat':'Gujarat','haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh',
                                'jammu-&-kashmir':'Jammu & Kashmir','jharkhand':'Jharkhand','karnataka':'Karnataka', 'kerala':'Kerala',
                                'ladakh':'Ladakh','lakshadweep':'Lakshadweep','madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra',
                                'manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram','nagaland':'Nagaland','odisha':'Odisha',
                                'puducherry':'Puducherry','punjab':'Punjab','rajasthan':'Rajasthan','telangana':'Telangana','sikkim':'Sikkim',
                                'tamil-nadu':'Tamil Nadu','tripura':"Tripura",'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand','west-bengal':'West Bengal'}})
agg_userd_df
dr_querry1="""drop table if exists Aggregation_User_detail"""
cursor.execute(dr_querry1)
conection.commit()
cr_query1 = """CREATE TABLE  Aggregation_User_detail (
                state VARCHAR(100),
                year INT,
                quarter INT,
                app_opens BIGINT,
                no_of_users BIGINT
            )"""
cursor.execute(cr_query1)
conection.commit()

for i,j in agg_userd_df.iterrows():
    in_query="""insert into Aggregation_User_detail(state,year,quarter,app_opens,no_of_users) values(%s,%s,%s,%s,%s)"""
    values=(j["state"],j["year"],j["quater"],j["appOpens"],j["no_of_users"])
    cursor.execute(in_query,values)
conection.commit()   


#extracting data from top  user  pincode  and converting into dataframe and cleaning and preprocessing the data and insert the datas into sql table format 
clm8={"state":[],"year":[],"quater":[],"pincode":[],"no_of_user":[]}
path6="C:/Users/krish/OneDrive/Desktop/project/phonepe/pulse/data/top/user/country/india/state/"
tu_st_list=os.listdir(path6)
for state in tu_st_list:
    path6_st=path6+state+"/"
    ye_list=os.listdir(path6_st)
    for year in ye_list:
        path6_st_ye=path6_st+year+"/"
        file_list=os.listdir(path6_st_ye)
        for file in file_list:
            path6_st_ye_f=path6_st_ye+file
            d=open(path6_st_ye_f,"r")
            data=json.load(d)
            for i in data["data"]["pincodes"]:
                pincode=i["name"]
                user=i["registeredUsers"]
                clm8["state"].append(state)
                clm8["year"].append(year)
                clm8["quater"].append(file.strip(".json"))
                clm8["no_of_user"].append(user)
                clm8["pincode"].append(pincode)
df=pd.DataFrame(clm8)
df=df.replace({"state":{"andaman-&-nicobar-islands":"Andaman & Nicobar",'andhra-pradesh':'Andhra Pradesh','arunachal-pradesh':'Arunachal Pradesh',
                                'assam':'Assam','bihar':'Bihar','chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
                                'delhi':'Delhi', 'goa':'Goa','gujarat':'Gujarat','haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh',
                                'jammu-&-kashmir':'Jammu & Kashmir','jharkhand':'Jharkhand','karnataka':'Karnataka', 'kerala':'Kerala',
                                'ladakh':'Ladakh','lakshadweep':'Lakshadweep','madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra',
                                'manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram','nagaland':'Nagaland','odisha':'Odisha',
                                'puducherry':'Puducherry','punjab':'Punjab','rajasthan':'Rajasthan','telangana':'Telangana','sikkim':'Sikkim',
                                'tamil-nadu':'Tamil Nadu','tripura':"Tripura",'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand','west-bengal':'West Bengal'}})

dr_querry="""drop table if exists pincode_user"""
cursor.execute(dr_querry)
cr_query="""create table pincode_user (state varchar(100), year int, quater int, pincode bigint, no_of_user bigint)"""
cursor.execute(cr_query)
for i,j in df.iterrows():
    in_query="""insert into pincode_user (state,year,quater,pincode,no_of_user) values(%s,%s,%s,%s,%s)"""
    values=(j["state"],j["year"],j["quater"],j["pincode"],j["no_of_user"])
    cursor.execute(in_query,values)
conection.commit()



#extracting data from top  transaction  pincode  and converting into dataframe and cleaning and preprocessing the data and insert the datas into sql table format 
clm8={"state":[],"year":[],"quater":[],"pincode":[],"no_of_user":[]}
clm9={"state":[],"year":[],"quater":[],"pincode":[],"no_of_user":[],"amt":[]}
path6="C:/Users/krish/OneDrive/Desktop/project/phonepe/pulse/data/top/transaction/country/india/state/"
tu_st_list=os.listdir(path6)
for state in tu_st_list:
    path6_st=path6+state+"/"
    ye_list=os.listdir(path6_st)
    for year in ye_list:
        path6_st_ye=path6_st+year+"/"
        file_list=os.listdir(path6_st_ye)
        for file in file_list:
            path6_st_ye_f=path6_st_ye+file
            d=open(path6_st_ye_f,"r")
            data=json.load(d)
            for i in data["data"]["pincodes"]:
                clm9["pincode"].append(i["entityName"])
                clm9["no_of_user"].append(i["metric"]["count"])
                clm9["amt"].append(i["metric"]["count"])
                clm9["state"].append(state)
                clm9["year"].append(year)
                clm9["quater"].append(file.strip(".json"))
df=pd.DataFrame(clm9)
df=df.replace({"state":{"andaman-&-nicobar-islands":"Andaman & Nicobar",'andhra-pradesh':'Andhra Pradesh','arunachal-pradesh':'Arunachal Pradesh',
                                'assam':'Assam','bihar':'Bihar','chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
                                'delhi':'Delhi', 'goa':'Goa','gujarat':'Gujarat','haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh',
                                'jammu-&-kashmir':'Jammu & Kashmir','jharkhand':'Jharkhand','karnataka':'Karnataka', 'kerala':'Kerala',
                                'ladakh':'Ladakh','lakshadweep':'Lakshadweep','madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra',
                                'manipur':'Manipur','meghalaya':'Meghalaya','mizoram':'Mizoram','nagaland':'Nagaland','odisha':'Odisha',
                                'puducherry':'Puducherry','punjab':'Punjab','rajasthan':'Rajasthan','telangana':'Telangana','sikkim':'Sikkim',
                                'tamil-nadu':'Tamil Nadu','tripura':"Tripura",'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand','west-bengal':'West Bengal'}})
dr_querry="""drop table if exists pincode_transaction"""
cursor.execute(dr_querry)
cr_query="""create table pincode_transaction (state varchar(100),year int,quater int,pincode int,no_of_user bigint,amt bigint)"""
cursor.execute(cr_query)
for i,j in df.iterrows():
    in_query="""insert into pincode_transaction (state,year,quater,pincode,no_of_user,amt) values(%s,%s,%s,%s,%s,%s)"""
    values=(j["state"],j["year"],j["quater"],j["pincode"],j["no_of_user"],j["amt"])
    cursor.execute(in_query,values)
conection.commit() 