from model.truckB2.list import getOperationsWagon
import pandas as pd

def getEvents():
    df_resume, df_operations = getOperationsWagon()
    df_events = df_resume.query('qtyCheckListVerifyL > 0 or qtyCheckListConditionsL > 0 or qtyCheckListVerifyW > 0 or qtyCheckListConditionsW > 0')
    df_events = df_operations.loc[df_events.index]
    return df_events

# def getInoperatives():
