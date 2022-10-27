import pandas as pd
from preparing_monthly_feed import creating_monthly_feed
from Data_receiving import Data_receiving
from Data_Staging import  data_staging
from Data_mart import  creating_data_mart

if __name__ == "__main__":
    creating_monthly_feed()
    print("Monthly Feed Complete---------------------")
    Data_receiving()
    print("Data Receiving Complete-------------------")
    data_staging()
    print("Loading Staging Complete------------------")
    creating_data_mart()
    print("Loading Data Mart  Complete---------------")
