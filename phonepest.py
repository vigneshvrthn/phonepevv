import pandas as pd
import streamlit as st
import psycopg2 as sql
from streamlit_option_menu import option_menu
import requests
import json
import plotly.express as px
from PIL import Image
conection=sql.connect(host="localhost",user="postgres",port=5432,password="vignesh",database="phonepepulse")
cursor=conection.cursor()
st.set_page_config(page_title="PhonePE Data Exploration",page_icon=r"C:\Users\krish\OneDrive\Desktop\project\phonepe\th.jpeg",layout="wide")

url='https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson'
response=requests.get(url)
data=json.loads(response.content)
india_map=[]
for i in data["features"]:
    india_map.append(i["properties"]["ST_NM"])
india_map.sort()
def transaction():
    df2=df.groupby("states")[["transaction_count","amt"]].sum()
    df2.reset_index(inplace=True)
    fig1=px.choropleth(df2,geojson=data ,locations="states",featureidkey="properties.ST_NM",color="amt",color_continuous_scale="Rainbow",
                    range_color=(df2["amt"].min(),df2["amt"].max()),title=f"{select} Transaction for Year: {year}, Quarter :{quarter.strip("Quarter")}",
                    hover_name="states",hover_data={"transaction_count": True, "amt": True},fitbounds="locations",height=500,width=700)
    fig1.update_geos(visible=False)
    st.plotly_chart(fig1)
    fig2 = px.bar(df2, x='states', y='transaction_count', color='transaction_count',
                labels={'transaction_count': 'Transaction Count', 'states': 'States'},
                title=f'{select} Transaction Count by State',
                color_continuous_scale='Viridis',height=500,width=650)  
    fig2.update_xaxes(title_text='States')
    fig2.update_yaxes(title_text='Transaction Count')
    
    fig3 = px.bar(df2, x='states', y='amt', color='amt',
                labels={'amt': 'Transaction Amt', 'states': 'States'},
                title=f'{select} Transaction Amt by State',
                color_continuous_scale='Viridis',height=500,width=650)  
    fig3.update_xaxes(title_text='States')
    fig3.update_yaxes(title_text='Transaction Count')
    
    col1,col2=st.columns(2)
    with col1:
        st.plotly_chart(fig2) 
    with col2:
        st.plotly_chart(fig3)


def user():
    df2=df.groupby("state")[["no_of_user"]].sum()
    if len(df2)>1:
        df2.reset_index(inplace=True)     
        
        fig1=px.choropleth(df2,geojson=data ,locations="state",featureidkey="properties.ST_NM",color="no_of_user",color_continuous_scale="Rainbow",
                        range_color=(df2["no_of_user"].min(),df2["no_of_user"].max()),title=f"Aggregation User for Year: {year}, Quarter :{quarter.strip("Quarter")}",
                        hover_name="state",hover_data={"no_of_user": True},fitbounds="locations",height=500,width=700)
        fig1.update_geos(visible=False)
        st.plotly_chart(fig1)
        fig = px.bar(df2, x='state', y='no_of_user', color='no_of_user',
        labels={'no_of_user': 'User Count ', 'state': 'States'},
        title='Transaction Count by State',
        color_continuous_scale='Viridis',height=500,width=600)  
        fig.update_xaxes(title_text='States')
        fig.update_yaxes(title_text='User Count')
        st.plotly_chart(fig)
    else:
        st.write("In this year or in this quarter the data was not in the pulse so please change the year or quarter to explore")

def  agg_transaction_pie(df,state):
    df2=df[df["states"]==state]
    df3=df2.groupby("transaction_type")[["transaction_count","amt"]].sum()
    df3.reset_index(inplace=True)
    fig_pie=px.pie(data_frame=df3,names="transaction_type",values="transaction_count",width=500,title=f"Aggregation Transaction count for the state {state}",color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.5)
    fig_pie2=px.pie(data_frame=df3,names="transaction_type",values="amt",width=500,title=f"Aggregation Transaction Amt for the state {state}",color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.5)
    col1,col2=st.columns(2)
    with col1:
        st.plotly_chart(fig_pie)
    with col2:
        st.plotly_chart(fig_pie2)
def transaction_pie(df,state):
    df2=df[df["states"]==state]
    df3=df2.groupby("dristict")[["transaction_count","amt"]].sum()
    df3.reset_index(inplace=True)
    fig_pie=px.pie(data_frame=df3,names="dristict",values="transaction_count",width=500,title=f"{select} Transaction count for the state {state}<br>and details for the districts ",color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.5)
    fig_pie2=px.pie(data_frame=df3,names="dristict",values="amt",width=500,title=f"{select} Transaction Amount for the state {state}<br>and details for the districts ",color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.5)
    col1,col2=st.columns(2)
    with col1:
        st.plotly_chart(fig_pie)
    with col2:
        st.plotly_chart(fig_pie2)



# Your Streamlit app content goes here

st.title('Phonepe Pulse Data Visualization and Exploration')
with st.title("Title of your page"):
    cols = st.columns([1, 1])
    cols1 = st.columns([1, 1])
    with cols1[0]:
        st.image(Image.open(r"C:\Users\krish\OneDrive\Desktop\project\phonepe\PhonePe-Logo.png"))  # Provide relative path to the image
    with cols1[1]:
        st.image(r"C:\Users\krish\OneDrive\Desktop\project\phonepe\pulse-a7eb70ef6c6c2c5a9d1b3c3fd66752f5.gif",width=300)  # Provide relative path to the image

with st.sidebar:    
    select_fun=option_menu("Menu",["Home","Data Exploration","Insigth"])


if select_fun=="Home":
    page_bg_img = '''
    <style>
    [data-testid=stAppViewContainer] {
        background-image: url("https://fthmb.tqn.com/vMHG2Hi44XBqddh93WTo3nkWESU=/5000x3000/filters:fill(auto,1)/low-poly-background-672623312-5a5a8563e258f800370a105a.jpg");
        background-size: 100% 100%; /* Cover the entire container */
        background-repeat: no-repeat; /* Ensure background image doesn't repeat */
    }
    </style>
    '''
    
    st.markdown(page_bg_img, unsafe_allow_html=True)
    video="https://youtu.be/aXnNA4mv1dU"
    
    

    
    st.markdown("<span style='color:purple; font-weight:bold;'>" +"PhonePe is a digital payments platform which is 100% faster and secure", unsafe_allow_html=True)
    st.markdown("<span style='color:purple; font-weight:bold;'>" +"UPI Payments: Facilitating payments using Unified Payments Interface (UPI).", unsafe_allow_html=True)
    st.markdown("<span style='color:purple; font-weight:bold;'>" +"Mobile Recharges: Allowing users to recharge their mobile phones Easily.", unsafe_allow_html=True)
    st.markdown("<span style='color:purple; font-weight:bold;'>"+"Bill Payments: Enabling users to pay utility bills such as electricity, water, gas, etc.", unsafe_allow_html=True)
    st.markdown("<span style='color:purple; font-weight:bold;'>" +"Money Transfers: Allowing users to transfer money to other individuals with in a second and avoiding going to bank.", unsafe_allow_html=True)
    st.markdown("<span style='color:purple; font-weight:bold;'>"+"24x7 send money and receive money it will also work on bank leave",unsafe_allow_html=True)
     
        
    st.video(video, start_time=0)   
    st.subheader("By clicking the Data Exploration Menu")
    st.markdown("<span style='color:purple; font-weight:bold;'>" +"we can see explore the PhonePulse Data by visual", unsafe_allow_html=True)
    video1="https://www.youtube.com/watch?v=c_1H6vivsiA"
    st.video(video1, start_time=0)  
    st.subheader("By clicking Insight we can see the different type of chart by top or low")  
    st.video("https://www.youtube.com/watch?v=Vn-LjmSu9SY")
      

elif select_fun=="Data Exploration":
    page_bg_img = '''
    <style>
    [data-testid=stAppViewContainer] {
        background-image: url("https://techstory.in/wp-content/uploads/2021/08/PhonePe-receives-insurance-broking-license.jpg");
        background-size: 100% 100%; /* Cover the entire container */
        background-repeat: no-repeat; /* Ensure background image doesn't repeat */
    }
    </style>
    '''
    
    st.markdown(page_bg_img, unsafe_allow_html=True)
    select=option_menu(menu_title=None,options=["Aggregation","Map","Top"],icons=["collection-fill","map","graph-up-arrow"],orientation="horizontal")
    if select == "Aggregation":
        cols = st.columns([1, 1,1])
        with cols[0]:
            transaction_or_user = st.selectbox("Select Data Type", ("Transaction", "User"))
        with cols[1]:
            quarter = st.selectbox("Select Quarter", ["Quarter 1", "Quarter 2", "Quarter 3", "Quarter 4"])
        with cols[2]:
            year=st.selectbox("Select Year",[2018,2019,2020,2021,2022,2023])
        df=pd.read_sql_query(f'SELECT * FROM {select}_{transaction_or_user} WHERE quater = {quarter.strip('Quarter ')} AND year = {year}',conection)
    if select=="Aggregation" and transaction_or_user=="Transaction":
        transaction()
        state = st.selectbox("Select State", df["states"].unique())
        (agg_transaction_pie(df,state))
    if select=="Aggregation" and transaction_or_user=="User":
        df=pd.read_sql_query(f'SELECT state , sum(app_opens) as app_opens,sum(no_of_users) as users from {select}_{transaction_or_user}_detail WHERE quarter = {quarter.strip('Quarter ')} AND year = {year} group by state',conection)
        fig1=px.choropleth(df,geojson=data ,locations="state",featureidkey="properties.ST_NM",color="users",color_continuous_scale="Rainbow",
                        range_color=(df["users"].min(),df["users"].max()),title=f"Aggregation User for Year: {year}, Quarter :{quarter.strip("Quarter")}",
                        hover_name="state",hover_data={"users": True,"app_opens":True},fitbounds="locations",height=500,width=700)
        fig1.update_geos(visible=False)
        st.plotly_chart(fig1)
        
        fig2 = px.bar(df, x='state', y='app_opens', color='app_opens',
                        labels={'app_opens': 'app_opens', 'state': 'States'},
                        title=f'{select} user Appopens Count by State',
                        color_continuous_scale='Viridis',height=500,width=650)  
        fig2.update_xaxes(title_text='States')
        fig2.update_yaxes(title_text='App Opens')
        
        fig3 = px.bar(df, x='state', y='users', color='users',
                    labels={'users': 'users Amt', 'state': 'States'},
                    title=f'{select} user by State',
                    color_continuous_scale='Viridis',height=500,width=650)  
        fig3.update_xaxes(title_text='States')
        fig3.update_yaxes(title_text='Users')
        
        col1,col2=st.columns(2)
        with col1:
            st.plotly_chart(fig2) 
        with col2:
            st.plotly_chart(fig3)
        state = st.selectbox("Select State", df["state"].unique())
        df2=df[df["state"]==state]
        
        df=pd.read_sql_query(f"SELECT brand,sum(no_of_user) as no_of_user FROM {select}_{transaction_or_user} WHERE state='{state}' and quater = {quarter.strip('Quarter ')} AND year = {year}  group by brand",conection)
        fig_pie=px.pie(data_frame=df,names="brand",values="no_of_user",width=600,title="User count brand",color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.5)
        st.plotly_chart(fig_pie) 
   
        


            

            
                
    if select == "Map":

        cols = st.columns([1, 1,1])
        with cols[0]:
            transaction_or_user = st.selectbox("Select Data Type", ("Transaction", "User"))
        with cols[1]:
            quarter = st.selectbox("Select Quarter", ["Quarter 1", "Quarter 2", "Quarter 3", "Quarter 4"])
        with cols[2]:
            year=st.selectbox("Select Year",[2018,2019,2020,2021,2022,2023])
        df=pd.read_sql_query(f'SELECT * FROM {select}_{transaction_or_user} WHERE quater = {quarter.strip('Quarter ')} AND year = {year}',conection)
    if select=="Map" and transaction_or_user=="Transaction":
        transaction()
        state = st.selectbox("Select State", df["states"].unique())
        transaction_pie(df,state)
    if select=="Map" and transaction_or_user=="User":
        user()
        state = st.selectbox("Select State", df["state"].unique())
        df2=df[df["state"]==state]
        fig_pie=px.pie(data_frame=df2,names="dristict",values="no_of_user",width=600,title="User count dristict",color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.5)
        st.plotly_chart(fig_pie)

        
        

            
    if select == "Top":

        cols = st.columns([1, 1,1])
        with cols[0]:
            transaction_or_user = st.selectbox("Select Data Type", ("Transaction", "User"))
        with cols[1]:
            quarter = st.selectbox("Select Quarter", ["Quarter 1", "Quarter 2", "Quarter 3", "Quarter 4"])
        with cols[2]:
            year=st.selectbox("Select Year",[2018,2019,2020,2021,2022,2023])
        df=pd.read_sql_query(f'SELECT * FROM {select}_{transaction_or_user} WHERE quater = {quarter.strip('Quarter ')} AND year = {year}',conection)
      
    if select=="Top" and transaction_or_user=="Transaction":
        transaction()
        state = st.selectbox("Select State", df["states"].unique())
        transaction_pie(df,state)
    if select=="Top" and transaction_or_user=="User":
        user()
        state = st.selectbox("Select State", df["state"].unique())
        df2=df[df["state"]==state]
        fig_pie=px.pie(data_frame=df2,names="dristict",values="no_of_user",width=600,title="User count dristict",color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.5)
        st.plotly_chart(fig_pie)
elif select_fun=="Insigth":
    page_bg_img = '''
    <style>
    [data-testid=stAppViewContainer] {
    background-image: url("https://fthmb.tqn.com/vMHG2Hi44XBqddh93WTo3nkWESU=/5000x3000/filters:fill(auto,1)/low-poly-background-672623312-5a5a8563e258f800370a105a.jpg");
    background-size: 100% 100%; /* Cover the entire container */
    background-repeat: no-repeat; /* Ensure background image doesn't repeat */
    }
    </style>
    '''
    select=st.selectbox("Select any one of the question.",["1.	What are the brands of mobiles used and give the top three mobile?",
                         "2.	Which states have the lowest transaction amounts?",
                         "3.	What are the top 10 districts with the highest transaction amounts?",
                         "4.	Can you provide 5 districts with the lowest transaction amounts?",
                         "5.	Which transaction type is highly used, and also give by statewise?",
                         "6.	Which states have the lowest transaction count?",
                         "7.	Which states have the highest transaction count?",
                         "8.	The Top 10 district which have the highest transaction amounts?",
                         "9.	The Top 10 states which is having the number of  user is high?",
                         "10.	Give some state  the number of user is  low?"])
    if select=="1.	What are the brands of mobiles used and give the top three mobile?":
        year=st.number_input("enter the year from 2018 to 2023",step=1,max_value=2023,min_value=2018)
        df=pd.read_sql_query(f"select brand,no_of_user from aggregation_user where year={year}",conection)
        df_gr=df.groupby("brand")[["no_of_user"]].sum()
        df_gr.reset_index(inplace=True)
        df_gr_sorted = df_gr.sort_values("no_of_user", ascending=False)
        df_gr_sorted.index=range(1,len(df_gr_sorted)+1)
        
        fig_bar = px.bar(data_frame=df_gr_sorted, x="brand",title=f"The Mobile Phone Brands where using PhonePe in the year{year}", y="no_of_user",width=600)
        st.plotly_chart(fig_bar)
        
        st.write("<span style='color:blue'>" +f"The Top three Brands in the year {year} is ",df_gr_sorted["brand"].head(3) , unsafe_allow_html=True)
    if select=="2.	Which states have the lowest transaction amounts?":
        df = pd.read_sql_query(f"SELECT states,amt FROM aggregation_transaction", conection)
        dfgr=df.groupby("states")[["amt"]].sum()
        dfgr.reset_index(inplace=True)
        dfgr=dfgr.sort_values("amt")
        dfgr.index=range(1,len(dfgr)+1)
        fig = px.bar(dfgr, x='amt', y='states', orientation='h', title='Transaction Amounts by State',height=800,width=1000,color="amt",color_continuous_scale="rainbow")
        st.plotly_chart(fig)
        st.write("<span style='color:Darkred'>" +f"These are the states that having Lowest Transaction amt  is ",  dfgr["states"].head(5), unsafe_allow_html=True)

    if select=="3.	What are the top 10 districts with the highest transaction amounts?":
        df = pd.read_sql_query(f"SELECT states,amt FROM aggregation_transaction", conection)
        dfgr=df.groupby("states")[["amt"]].sum()
        dfgr.reset_index(inplace=True)
        dfgr1=dfgr.sort_values("amt",ascending=False)
        dfgr1.index=range(1,len(dfgr1)+1)
        fig = px.bar(dfgr1, x='amt', y='states', title='Transaction Amounts by State',height=800,color="amt",color_continuous_scale="rainbow")
        st.plotly_chart(fig)
        st.write("<span style='color:Darkred'>" +f"These are the states that having Highest Transaction amt  is ", dfgr1["states"].head(10), unsafe_allow_html=True)
    if select=="4.	Can you provide 5 districts with the lowest transaction amounts?":
        df=pd.read_sql_query("select dristict,sum(amt) as transaction_amt from map_transaction group by dristict",conection)
        df2=df.sort_values("transaction_amt",ascending=True).head()
        df2.index=range(1,len(df2)+1)
        st.write("five dristict which have the lowest transaction amt is ",df2)
        fig = px.scatter(df, x='dristict', y='transaction_amt', color='dristict',hover_name="dristict", 
                  title='Scatter Plot of Transaction Amount for District',
                 size='transaction_amt', size_max=40,height=700,width=1100)
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig)
    if select=="5.	Which transaction type is highly used, and also give by statewise?":
        df=pd.read_sql_query("select transaction_type,sum(transaction_count) as transaction_count from aggregation_transaction group by transaction_type",conection)
        fig_pie=px.pie(data_frame=df,names="transaction_type",values="transaction_count",width=600,title="Transaction count",color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.5)
        st.plotly_chart(fig_pie)
        dr=df.sort_values(by="transaction_count", ascending=False)["transaction_type"].head(1)
        dr.index=range(1,len(dr)+1)
        st.write("the highest transaction type highly used is ",dr)
        df1=pd.read_sql_query("select * from aggregation_transaction",conection)
        state = st.selectbox("Select State", df1["states"].unique())
        query = f"SELECT transaction_type, SUM(transaction_count) AS transaction_count FROM aggregation_transaction WHERE states='{state}' GROUP BY transaction_type;"
        df_st = pd.read_sql_query(query, conection)
        fig_pie=px.pie(data_frame=df_st,names="transaction_type",values="transaction_count",width=600,title="Transaction count",color_discrete_sequence=px.colors.qualitative.Pastel,hole=0.5)
        st.plotly_chart(fig_pie)
        dr=df_st.sort_values(by="transaction_count", ascending=False)["transaction_type"].head(1)
        dr.index=range(1,len(dr)+1)
        st.write(f"the highest transaction type highly used in the {state}is ",dr)
    if select== "6.	Which states have the lowest transaction count?":
        df=pd.read_sql_query("SELECT states,sum(transaction_count) as transaction_count from aggregation_transaction group by  states order by transaction_count limit 10",conection)
        df.index=range(1,len(df)+1)
        fig=px.bar(df,x="states",y="transaction_count",title="Transaction_count by states",hover_name="states",height=500,width=800,color="transaction_count",color_continuous_scale="rainbow")
        st.markdown("""<style>.lemon-yellow-text {color: #fe5a1d  ;}</style>""",unsafe_allow_html=True)
        st.write("<span class='lemon-yellow-text'>The last 10 states which have the lowest Transaction count</span>",df, unsafe_allow_html=True)

        st.plotly_chart(fig)
    if select== "7.	Which states have the highest transaction count?":
        df=pd.read_sql_query("SELECT states,sum(transaction_count) as transaction_count from aggregation_transaction group by  states order by transaction_count desc limit 10",conection)
        df.index=range(1,len(df)+1)
        fig=px.bar(df,x="states",y="transaction_count",title="Transaction_count by states",hover_name="states",height=500,width=800,color="transaction_count",color_continuous_scale="rainbow")
        st.markdown("""<style>.lemon-yellow-text {color: #fe5a1d  ;}</style>""",unsafe_allow_html=True)
        st.write("<span class='lemon-yellow-text'>The top 10 states which have the highest Transaction count</span>",df, unsafe_allow_html=True)
        st.plotly_chart(fig)
    if select=="8.	The Top 10 district which have the highest transaction amounts?":
        df=pd.read_sql_query("select dristict,sum(amt) as transaction_amt from map_transaction group by dristict order by transaction_amt limit 10",conection)
        df.index=range(1,len(df)+1)
        fig=px.scatter(df,x="dristict",y="transaction_amt",size="transaction_amt",title="Top 10 dristict having the transaction amount highly")
        st.write("Top 10 District which have the highest transaction amount",df)
        st.plotly_chart(fig)
    if select=="9.	The Top 10 states which is having the number of  user is high?":
        df=pd.read_sql_query("select state, sum(no_of_user) as users from aggregation_user group by state order by users desc limit 10",conection)
        df.index=range(1,len(df)+1)
        fig=px.bar(df,x="state",y="users",title="Top 10 state that having high no of user",height=500,width=800)
        st.write("Top 10 states having the highest No of user ",df)
        st.plotly_chart(fig)

    if select=="10.	Give some state  the number of user is  low?":
        df=pd.read_sql_query("select state, sum(no_of_user) as users from aggregation_user group by state order by users  limit 10",conection)
        df.index=range(1,len(df)+1)
        fig=px.bar(df,x="state",y="users",title="10 state that having no of user is low",height=500,width=800)
        st.write("Ten states which having the lowest No of User",df)
        st.plotly_chart(fig)

        
            


