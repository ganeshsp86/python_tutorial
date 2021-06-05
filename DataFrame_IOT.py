#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  2 18:55:36 2021

@author: ganeshsp
"""
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime 
import warnings 
warnings.filterwarnings('ignore')
df=pd.concat(pd.read_excel('datasets v2.xlsx',sheet_name=None),ignore_index=True)
unq_siteids=df['Site ID'].unique()
final_df=pd.DataFrame()
def db_connect():
    con=psycopg2.connect(dbname='dev', 
                         host='redshift-cluster-1.choephusvs2p.us-east-2.redshift.amazonaws.com',
                         port='5439',user='awsuser',password='Windows07!')
    return con
con=db_connect()
for i in range (len(unq_siteids)):
    print(unq_siteids[i])
    temp_df=df[df['Site ID']==unq_siteids[i]]
    temp_df=temp_df.sort_values(by=['OSS Time']) 
    temp_df=temp_df.reset_index(drop=True)    
    temp_df['time_diff']=temp_df['End Time']-temp_df['Start Time']     
    temp_df['time_insecs']=[(x-y).total_seconds() for x,y in zip(temp_df['End Time'], temp_df['Start Time'])]
    temp_df['time_inmins']=temp_df['time_insecs']/60     
    temp_df['shifted_OSSTime']=temp_df['OSS Time'].shift(1)
    temp_df['shifted_End Time']=temp_df['End Time'].shift(1)
    temp_df['abnormality_flag']=[1 if (y<=x<=z) else 0 for x,y,z in 
                                 zip(temp_df['OSS Time'],temp_df['shifted_OSSTime'],temp_df['shifted_End Time'])]
    temp_df['abnormality_flag']=[0 if x==1 and y=='SITE ON BATTERY' and z<120 else x for x,y,z in 
                                 zip(temp_df['abnormality_flag'],temp_df['Alarm Text'],temp_df['time_inmins'])]
    temp_df['final_abnflag']=['MF' if a=='MAINS FAIL' else 'SOB' if ((a=='SITE ON BATTERY') & (b==0))
                              else 'LV' if ((a=='SITE ON BATTERY') & (b==1)) 
                              else 'DGOL' if ((a=='DG ON LOAD') & (b==0))
                              else 'Fault' if ((a=='DG ON LOAD') & (b==1))
                              else 'DGMF' if ((a=='DG MAJOR FAULT') & (b==0))
                              else 'DGMM' if ((a=='DG MAJOR FAULT') & (b==1))
                              else 'NA' for a,b in zip(temp_df['Alarm Text'], temp_df['abnormality_flag'])]
# =============================================================================
#     final_df=final_df.append(temp_df, ignore_index=True)
# =============================================================================
    cur=con.cursor()
    temp_df=temp_df.astype(str)
    push_tuple=[tuple(x) for x in temp_df.values]    
    psycopg2.extras.execute_values(cur, 'insert into data_iot values %s', push_tuple)
    cur.close()
    con.commit()
con.close()
