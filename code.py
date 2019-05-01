"""
    This program, 'Separate and/not Equal', using open databases to drive 
    neopixels

    Copyright (C) 2019 John Patrick Fahrenbach

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""


import requests #for talking to chicago's API
import pandas as pd #for data munging
import datetime #for timing
from datetime import datetime, timedelta #for timing
import numpy as np #for data munging
import board #lights
import neopixel #lights
import time #forgot and afraid to remove
from scipy.stats import chi2_contingency #yeah statistics!
from scipy.stats import chi2 #yeah statistics!
import psycopg2 #for sending data to website
import RPi.GPIO as GPIO #lights

"""
This function is a lookup table for convert entries to colors

Parameters
----------
primeType : str
    the key for the lookup table

Returns
-------
dict
    the color to display

"""
def pixel_look_up(primeType):
    rx=2
    gx=2
    bx=2
    time_dur=0.5
    if primeType=="ROBBERY" or primeType=="BURGLARY" or primeType=="CRIMINAL DAMAGE":
      rx=225
      gx=95
      bx=0
      time_dur=0.5
    elif primeType=="HOMICIDE":
      rx=235
      gx=0
      bx=0
      time_dur=30
    elif primeType=="CRIM SEXUAL ASSAULT":
      rx=148
      gx=0
      bx=211
      time_dur=20
    elif primeType=="MOTOR VEHICLE THEFT" or primeType=="THEFT":
      rx=235
      gx=235
      bx=0
      time_dur=0.5
    elif primeType=="BATTERY" or primeType=="ASSAULT":
      rx=245
      gx=75
      bx=75
      time_dur=1
    elif primeType=="Pothole in Street Complaint":  
      rx=0
      gx=245
      bx=0
      time_dur=0.5
    elif primeType=="Tree Trim Request":  
      rx=9
      gx=235
      bx=235
      time_dur=0.5
    elif primeType=="Aircraft Noise Complaint":  
      rx=87 
      gx=245
      bx=171
      time_dur=0.5
    elif primeType=="Rodent Baiting/Rat Complaint":  
      rx=0
      gx=0
      bx=235
      time_dur=0.5

    time_dur=time_dur/20   
    return {'rx':rx, 'gx':gx, 'bx':bx,'time_dur':time_dur}

"""
This function is a lookup table for convert entries to categories for anova test

Parameters
----------
primeType : str
    the key for the lookup table

Returns
-------
str
    the category corresponding to entry

"""

def cat_look_up(primeType):
    cat="?"
    if primeType=="ROBBERY" or primeType=="BURGLARY":
      cat='a'
    elif primeType=="HOMICIDE":
      cat='b'
    elif primeType=="CRIMINAL DAMAGE":
      cat='c'
    elif primeType=="CRIM SEXUAL ASSAULT":
      cat='d'
    elif primeType=="MOTOR VEHICLE THEFT" or primeType=="THEFT":
      cat='e'
    elif primeType=="BATTERY" or primeType=="ASSAULT":
      cat='f'
    return (cat)

"""
This function is a lookup table for convert entries to colors

Parameters
----------
qry : str
    An API call

Returns
-------
dict
    the color to display

"""
def getdataframe(qry):
    r=requests.get(qry)
    df=pd.read_json(r.text)
    
    #change colnames and cast to datetime object in service query results
    if "created_date" in df.columns:
        df.columns=['community_area','date','primary_type']
        df.date=df.date.str.replace('T',' ')
        df.date=pd.to_datetime(df['date'])

    df['time_diff']=df.date.diff().dt.total_seconds()/60.0
    df['time_diff'].iloc[0]=0
    x_nrow=(df.shape[0]-1)
    itr=0
    while itr < x_nrow :
         if df['time_diff'].iloc[itr+1]==0:
            itr+=1
            itr_start=itr
            while itr < x_nrow:
                if df['time_diff'].iloc[itr+1]==0:
                    itr+=1
                else:
                    break
            itr_end=itr
            new_value=np.mean(df.time_diff.iloc[itr_start:(itr_end+2)])
     
            df.time_diff.iloc[itr_start:(2+itr_end)]=new_value
         itr+=1
    df['global_time_diff']=df['time_diff'].cumsum()
    return(df)

def getdataframes():
    d = datetime.today() - timedelta(days=7)
    date_str_max=str(d)[0:19].replace(' ','T')
  
    d = datetime.today() - timedelta(days=21)
    date_str_min=str(d)[0:19].replace(' ','T')
  
    connectionStr_high="https://data.cityofchicago.org/resource/6zsd-86xi.json?$select=date,primary_type&$where=community_area+in('67','71','44','69','68')+and+date+between+'"\
    +date_str_min+"'+and+'"+date_str_max+"'+and+primary_type+in+('HOMICIDE','CRIMINAL+DAMAGE','CRIM+SEXUAL+ASSAULT','MOTOR+VEHICLE',+'THEFT','ROBBERY','BURGLARY','THEFT','BATTERY','ASSAULT')&$order=date"
  
    connectionStr_low="https://data.cityofchicago.org/resource/6zsd-86xi.json?$select=date,primary_type&$where=community_area+in('11','12','13','14','16')+and+date+between+'"\
    +date_str_min+"'+and+'"+date_str_max+"'+and+primary_type+in+('HOMICIDE','CRIMINAL+DAMAGE','CRIM+SEXUAL+ASSAULT','MOTOR+VEHICLE',+'THEFT','ROBBERY','BURGLARY','THEFT','BATTERY','ASSAULT')&$order=date"

    high=getdataframe(connectionStr_high)
    low=getdataframe(connectionStr_low)

    d = datetime.today() - timedelta(days=21)
    date_str_max=str(d)[0:19].replace(' ','T')
  
    d = datetime.today() - timedelta(days=28)
    date_str_min=str(d)[0:19].replace(' ','T')

    connectionStr_high="https://data.cityofchicago.org/resource/6a9s-gvue.json?$select=community_area,created_date,sr_type&$where=community_area+in+('67','71','44','69','68')+and+created_date+between+'"\
    +date_str_min+"'+and+'"+date_str_max+"'+and+sr_type+in+('Pothole+in+Street+Complaint','Tree+Trim+Request','Aircraft+Noise+Complaint','Rodent+Baiting/Rat+Complaint')&$order=created_date"
  
    connectionStr_low="https://data.cityofchicago.org/resource/6a9s-gvue.json?$select=community_area,created_date,sr_type&$where=community_area+in+('11','12','13','14','16')+and+created_date+between+'"\
    +date_str_min+"'+and+'"+date_str_max+"'+and+sr_type+in+('Pothole+in+Street+Complaint','Tree+Trim+Request','Aircraft+Noise+Complaint','Rodent+Baiting/Rat+Complaint')&$order=created_date"
      
    high_service=getdataframe(connectionStr_high)
    low_service=getdataframe(connectionStr_low)

    return ([high,low,high_service,low_service])  
  
def fade_light(pixel_strip, old_pixel, new_pixel, pixels):
    
    Rstart_low=old_pixel['rx']
    Rend_low=new_pixel['rx']
    Gstart_low=old_pixel['gx']
    Gend_low=new_pixel['gx']
    Bstart_low=old_pixel['bx']
    Bend_low=new_pixel['bx']
    
    #What crime is the same in both regions! Let there be white light!
    if (equal):
        Rend_low=150
        Gend_low=150
        Bend_low=150
        
    n=10
    for i3 in range(1,n):

        Rnew = Rstart_low + (Rend_low - Rstart_low) * i3 / n;
        Gnew = Gstart_low + (Gend_low - Gstart_low) * i3 / n;
        Bnew = Bstart_low + (Bend_low - Bstart_low) * i3 / n;
        # Set pixel color here.
        
        for i in pixels:
            pixel_strip[i] = (int(Rnew),int(Gnew),int(Bnew))
        pixel_strip.show()
        time.sleep(0.01)


def offset_df(df,offset):
    df['time_diff']=df['time_diff']/offset
    df['global_time_diff']=df['time_diff'].cumsum()
    df['next_time']=[curr + timedelta(days=x/(24*60)) for x in df['global_time_diff']]
    return(df)


pixel_num=10
low_pixels=[5,6,7,8,9]
high_pixels=[0,1,2,3,4]

#Setup switch
GPIO.setmode(GPIO.BCM)
GPIO.setup(23, GPIO.IN, pull_up_down=GPIO.PUD_UP)


equal=False

pixel_pin = board.D18

pixel_strip = neopixel.NeoPixel(pixel_pin, pixel_num, brightness=1.0)
last_button_state=False
curr_button_state=False
crime=False

input_state = GPIO.input(23)
if input_state == False:
    crime=True
    last_button_state=True
    curr_button_state=True

curr=datetime.today()

switch_time=curr + timedelta(days=0.3)

try:
    [highpd,lowpd,highpd_service,lowpd_service]=getdataframes()
except:
    highpd = pd.read_csv("highpd.csv")
    highpd.date=pd.to_datetime(highpd['date'])
    lowpd = pd.read_csv("lowpd.csv")
    lowpd.date=pd.to_datetime(lowpd['date'])
    highpd_service = pd.read_csv("highpd_service.csv")
    highpd_service.date=pd.to_datetime(highpd_service['date'])
    lowpd_service = pd.read_csv("lowpd_service.csv")
    lowpd_service.date=pd.to_datetime(lowpd_service['date'])


highpd.to_csv("highpd.csv",index=False)
lowpd.to_csv("lowpd.csv",index=False)
highpd_service.to_csv("highpd_service.csv",index=False)
lowpd_service.to_csv("lowpd_service.csv",index=False)


#We are going to do every thing in reference the highpd mean time_diff for crimes
high_mean=highpd['time_diff'].mean()*60 #Let's average one blink per 10 seconds
highpd=offset_df(highpd,high_mean)
lowpd=offset_df(lowpd,high_mean)

#determine if crimes are equal      
highpd['cat']=highpd['primary_type'].apply(cat_look_up)
lowpd['cat']=lowpd['primary_type'].apply(cat_look_up)

#Sometimes that are no homicides which can cause the chi2 function to fail, so add 1 to each category
#Not statistically correct, but this is a art project so give me a break
table = [ [highpd[highpd['cat']=='a'].shape[0]+1, highpd[highpd['cat']=='b'].shape[0]+1, highpd[highpd['cat']=='c'].shape[0]+1, highpd[highpd['cat']=='d'].shape[0]+1,highpd[highpd['cat']=='e'].shape[0]+1,highpd[highpd['cat']=='f'].shape[0]+1],
          [lowpd[lowpd['cat']=='a'].shape[0]+1, lowpd[lowpd['cat']=='b'].shape[0]+1, lowpd[lowpd['cat']=='c'].shape[0]+1, lowpd[lowpd['cat']=='d'].shape[0]+1,lowpd[lowpd['cat']=='e'].shape[0]+1,lowpd[lowpd['cat']=='f'].shape[0]+1]]

stat, p, dof, expected = chi2_contingency(table)
equal=True
if p <=  0.05:
    equal=False  

#We are going to do every thing in reference the lowpd_service mean time_diff for service
high_service_mean=lowpd_service['time_diff'].mean()*60 #Let's average one blink per 10 seconds
highpd_service=offset_df(highpd_service,high_service_mean)
lowpd_service=offset_df(lowpd_service,high_service_mean)

#let's default to taking crimes
curr_highpd=highpd_service.copy()
curr_lowpd=lowpd_service.copy()

if (crime):
    curr_highpd=highpd.copy()
    curr_lowpd=lowpd.copy()

curr_high_block=curr
curr_high_pixel=pixel_look_up(curr_highpd.primary_type.iloc[0])    
curr_high_nrow=curr_highpd.shape[0]
  
curr_low_block=curr
curr_low_pixel=pixel_look_up(curr_lowpd.primary_type.iloc[0])    
curr_low_nrow=curr_lowpd.shape[0]

curr_high_itr=0
curr_low_itr=0

while(True):
    #get current time
    curr=datetime.today()
      
    #print(switch_time)
    #if past switch_time than update date set
    if (curr > switch_time):
        print("New time")
        try:
            [highpd,lowpd,highpd_service,lowpd_service]=getdataframes()
            
        except:
            highpd = pd.read_csv("highpd.csv")
            highpd.date=pd.to_datetime(highpd['date'])
            lowpd = pd.read_csv("lowpd.csv")
            lowpd.date=pd.to_datetime(lowpd['date'])
            highpd_service = pd.read_csv("highpd_service.csv")
            highpd_service.date=pd.to_datetime(highpd_service['date'])
            lowpd_service = pd.read_csv("lowpd_service.csv")
            lowpd_service.date=pd.to_datetime(lowpd_service['date'])
      
        #We are going to do every thing in reference the highpd mean time_diff for crimes
        high_mean=highpd['time_diff'].mean()*60 #Let's average one blink per 10 seconds
        highpd=offset_df(highpd,high_mean)
        lowpd=offset_df(lowpd,high_mean)
      
        #determine if crimes are equal      
        highpd['cat']=highpd['primary_type'].apply(cat_look_up)
        lowpd['cat']=lowpd['primary_type'].apply(cat_look_up)
      
        #Sometimes that are no homicides which can cause the chi2 function to fail, so add 1 to each category
        #Not statistically correct, but this is a art project so give me a break
        table = [ [highpd[highpd['cat']=='a'].shape[0]+1, highpd[highpd['cat']=='b'].shape[0]+1, highpd[highpd['cat']=='c'].shape[0]+1, highpd[highpd['cat']=='d'].shape[0]+1,highpd[highpd['cat']=='e'].shape[0]+1,highpd[highpd['cat']=='f'].shape[0]+1],
                  [lowpd[lowpd['cat']=='a'].shape[0]+1, lowpd[lowpd['cat']=='b'].shape[0]+1, lowpd[lowpd['cat']=='c'].shape[0]+1, lowpd[lowpd['cat']=='d'].shape[0]+1,lowpd[lowpd['cat']=='e'].shape[0]+1,lowpd[lowpd['cat']=='f'].shape[0]+1]]
      
        stat, p, dof, expected = chi2_contingency(table)
        equal=True
        if p <=  0.05:
            equal=False  
      
        #We are going to do every thing in reference the lowpd_service mean time_diff for service
        high_service_mean=lowpd_service['time_diff'].mean()*60 #Let's average one blink per 10 seconds
        highpd_service=offset_df(highpd_service,high_service_mean)
        lowpd_service=offset_df(lowpd_service,high_service_mean)
      
      
        #let's default to taking crimes
        curr_highdf=highpd_service
        curr_lowdf=lowpd_service
      
        if (crime):
            curr_highdf=highpd
            curr_lowdf=lowpd
      
        curr_high_block=curr
        curr_high_pixel=pixel_look_up(curr_highpd.primary_type.iloc[0])    
        curr_high_nrow=curr_highpd.shape[0]
          
        curr_low_block=curr
        curr_low_pixel=pixel_look_up(curr_lowpd.primary_type.iloc[0])    
        curr_low_nrow=curr_lowpd.shape[0]
      
        curr_high_itr=5
        curr_low_itr=0
        switch_time=datetime.today() + timedelta(days=1)

    input_state = GPIO.input(23)
    curr_button_state=False
    if input_state == False:
        curr_button_state=True

    #Did someone flip the switch
    if (last_button_state!=curr_button_state):
        if (curr_button_state):
            crime=True
            #Update the database
            try:
                conn = psycopg2.connect(host="NO",database="Just NO", user="Seriously", password="FU")
                cursor = conn.cursor()
                qry='''INSERT INTO flip_switch (current_selection, current_display) VALUES ('ROBBERY','CRIME')'''
                cursor.execute(qry)
                cursor.close()
                conn.commit()
                conn.close()
            except:
                pass

        else:
            crime=False
            try:
                conn = psycopg2.connect(host="NO",database="Just NO", user="Seriously", password="FU")
                cursor = conn.cursor()
                qry='''INSERT INTO flip_switch (current_selection, current_display) VALUES ('ROBBERY','311')'''
                cursor.execute(qry)
                cursor.close()
                conn.commit()
                conn.close()
            except:
                pass
      
        #let's default to taking services
        curr_highpd=highpd_service.copy()
        curr_lowpd=lowpd_service.copy()
      
        #if crime selected to select that
        if (crime):
            curr_highpd=highpd.copy()
            curr_lowpd=lowpd.copy()
      
        curr_high_block=curr
        curr_high_pixel=pixel_look_up(curr_highpd.primary_type.iloc[0])    
        curr_high_nrow=curr_highpd.shape[0]
          
        curr_low_block=curr
        curr_low_pixel=pixel_look_up(curr_lowpd.primary_type.iloc[0])    
        curr_low_nrow=curr_lowpd.shape[0]
      
        curr_high_itr=0
        curr_low_itr=0
        last_button_state=curr_button_state

  
    #if past next time then advance
    if (curr_highpd['next_time'].iloc[curr_high_itr]<curr):
        curr_high_itr+=1

        #don't change color unless block is free
        if curr_high_block < curr or curr_highpd.primary_type.iloc[curr_high_itr]=="HOMICIDE":
            old_pixel=curr_high_pixel
            curr_high_pixel=pixel_look_up(curr_highpd.primary_type.iloc[curr_high_itr])
            curr_high_block=curr + timedelta(days=curr_high_pixel['time_dur']/(24*60))
            fade_light(pixel_strip, old_pixel, curr_high_pixel,high_pixels)

               
    if (curr_high_itr>(curr_high_nrow-2)):
        curr_high_itr=0

        #update next_time so go through df again
        curr_highpd['next_time']=[curr + timedelta(days=x/(24*60)) for x in curr_highpd['global_time_diff']]
        curr_high_block=curr
  
    #we want to be black sometimes so if block is done then go black
    if(curr_high_block<curr):
        old_pixel=curr_high_pixel
        curr_high_pixel=pixel_look_up('None')
        fade_light(pixel_strip, old_pixel, curr_high_pixel,high_pixels)
        #print("go high block")

    #if past next time then advance
    if (curr_lowpd['next_time'].iloc[curr_low_itr]<curr):
        curr_low_itr+=1
      
        #don't change color unless block is free
        if curr_low_block < curr or curr_lowpd.primary_type.iloc[curr_low_itr]=="HOMICIDE":
            old_pixel=curr_low_pixel
            curr_low_pixel=pixel_look_up(curr_lowpd.primary_type.iloc[curr_low_itr])
            curr_low_block=curr + timedelta(days=curr_low_pixel['time_dur']/(24*60.0))
            fade_light(pixel_strip, old_pixel, curr_low_pixel, low_pixels)
            
    if (curr_low_itr>(curr_low_nrow-2)):
        curr_low_itr=0
        #update next_time so go through df again
        curr_lowpd['next_time']=[curr + timedelta(days=x/(24*60)) for x in curr_lowpd['global_time_diff']]
        curr_low_block=curr
  
    #we want to be black sometimes so if block is done then go black
    if(curr_low_block<curr):
        old_pixel=curr_low_pixel
        curr_low_pixel=pixel_look_up('None')
        fade_light(pixel_strip, old_pixel, curr_low_pixel, low_pixels)

#As Cesar says DGG 1 WR WKLV LS


