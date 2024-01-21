from model.wagonB2.listWagons import getOperationsWagon
import pandas as pd

def getEvents(df):
    df_resume, df_operations = getOperationsWagon()
    # eliminar una columna
    df_resume = df_resume.drop(['status'], axis=1)
    df_operations = df_operations.drop(['status'], axis=1)
    df_events = df_resume.query('qtyCheckListVerifyL > 0 or qtyCheckListConditionsL > 0 or qtyCheckListVerifyW > 0 or qtyCheckListConditionsW > 0')
    df_events = df_operations.loc[df_events.index]
    return df_events

# def getInoperatives():
