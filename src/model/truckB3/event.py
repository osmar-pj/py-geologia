from model.truckB3.listTrucks import getTripsTrucking
import pandas as pd

def getEventsTrucks():
    
    df_resume, df_operations = getTripsTrucking()
    
    df_copy = df_resume.copy()
    df_filter = df_copy.sort_values(by='createdAt', ascending=False)
    
    df_filter = df_filter[['name', 'tag', 'events', 'createdAt']]
    
    df_events = df_filter.loc[df_filter['events'].apply(lambda x: len(x) > 0)]
    
    return df_events