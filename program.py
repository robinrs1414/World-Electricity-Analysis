import glob
import pandas as pd
import pyodbc
import sqlalchemy
import socket
from sqlalchemy.sql import text

lst_file = glob.glob("**/*.json")
conn = sqlalchemy.create_engine(f'mssql+pyodbc://{socket.gethostname()}/Evaluation?trusted_connetion=yes&driver=ODBC Driver 17 for SQL Server')

for i in lst_file:
    df = pd.read_json(i)
    nam = i.split("\\")[0]
    f_name= nam+".csv"
    df.to_csv(f_name)
    df = pd.read_csv(f_name)
    
    query = "CREATE TABLE "+nam+"(CountryName varchar(100),CountryCode varchar(50), IndicatorName varchar(200),IndicatorCode varchar(200),[1960] float,[1961] float,[1962] float,[1963] float,[1964] float,[1965] float,[1966] float,[1967] float,[1968] float,[1969] float,[1970] float,[1971] float,[1972] float,[1973] float,[1974] float,[1975] float,[1976] float,[1977] float,[1978] float,[1979] float,[1980] float,[1981] float,[1982] float,[1983] float,[1984] float,[1985] float,[1986] float,[1987] float,[1988] float,[1989] float,[1990] float,[1991] float,[1992] float,[1993] float,[1994] float,[1995] float,[1996] float,[1997] float,[1998] float,[1999] float,[2000] float,[2001] float,[2002] float,[2003] float,[2004] float,[2005] float,[2006] float,[2007] float,[2008] float,[2009] float,[2010] float,[2011] float,[2012] float,[2013] float,[2014] float,[2015] float,[2016] float,[2017] float,[2018] float,[2019] float,[2020] float,[2021] float);"

    
    conn.execute(text(query).execution_options(autocommit=True))


    clm = df.iloc[1,1:5]
    clm = clm.to_list()

    for i in range(1960,2022,1):
        clm.append(i)

    df.drop(['Unnamed: 0','66'],axis=1,inplace=True)
    df.drop([0,1],axis=0,inplace=True)

    df.columns = clm

    df.fillna(0, inplace=True)

    df.reset_index()

    

    df.to_sql(nam,con=conn,if_exists="replace",index=False)
