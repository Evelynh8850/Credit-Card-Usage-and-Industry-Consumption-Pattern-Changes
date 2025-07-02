# 全國各產業刷卡資料整理(數據包含實體及網購)(以各產業及性別劃分)
import pandas as pd

def get_all_data(file):
    # 讀入原始資料
    df = pd.read_csv(file)
    # 改欄位名稱，分割年月
    df.columns = ['yyyy/mm','region','industry','gender','quantity','amount']
    # 原分為不同性別比之消費資料，因只需該年月總數，故加總同年月各性別之筆數及金額
    df = df.groupby('yyyy/mm', as_index=False)[['quantity', 'amount']].sum()
    df.insert(1, column ='year',value = df['yyyy/mm'].astype(str).str[:4])
    df.insert(2, column ='month',value = df['yyyy/mm'].astype(str).str[4:])
    # 取出2019-2024年之資料
    filter_all_19to24 = df['year'].isin(['2019','2020','2021','2022','2023','2024'])
    df = df[filter_all_19to24].reset_index(drop = True)
    # 新增欄位-平均單筆消費金額
    df['amount per transaction'] = df['amount']/df['quantity']
    # 新增欄位-與前月相比之「筆數」差距
    df.insert(4, column='quantity growth', value=df['quantity'].diff())
    # 新增欄位-與前月相比之「金額」差距
    df.insert(6, column='amount growth', value=df['amount'].diff())
    # 新增欄位-筆數增長率&金額增長率
    df.insert(5, column='quantity growth percentage', value='nan')
    df.insert(8, column='amount growth percentage', value='nan')
    df['season']='nan'
    for j in range(len(df['quantity'])):
        if j == 0:
            df.loc[j, 'quantity growth percentage'] = 'nan'
            df.loc[j, 'amount growth percentage'] = 'nan'
        else:
            df.iloc[j, 5] = (df.iloc[j, 4] / df.iloc[j-1, 3] * 100)
            df.iloc[j, 8] = (df.iloc[j, 7] / df.iloc[j-1, 6] * 100)
        
        # 新增season欄位
        if df.loc[j, 'month'] in ['01', '02', '03']:
            df.loc[j, 'season'] = '%s'%df.loc[j,'year'] + 'Q1'
        elif df.loc[j, 'month'] in ['04', '05', '06']:
            df.loc[j, 'season'] = '%s'%df.loc[j,'year'] + 'Q2'
        elif df.loc[j, 'month'] in ['07', '08', '09']:
            df.loc[j, 'season'] = '%s'%df.loc[j,'year'] + 'Q3'
        else:
            df.loc[j, 'season'] = '%s'%df.loc[j,'year'] + 'Q4'
        
    return df

# 分別取得各產業資料
df_all_food = get_all_data('各性別持卡人以信用卡支付食品餐飲類消費之總簽帳金額及筆數.csv')
df_all_clothing = get_all_data('各性別持卡人以信用卡支付服飾類消費之總簽帳金額及筆數.csv')
df_all_housing = get_all_data('各性別持卡人以信用卡支付住宿類消費之總簽帳金額及筆數.csv')
df_all_transportation = get_all_data('各性別持卡人以信用卡支付交通類消費之總簽帳金額及筆數.csv')
df_all_edu = get_all_data('各性別持卡人以信用卡支付文教康樂相關消費之總簽帳金額及筆數.csv')
df_all_grocery = get_all_data('各性別持卡人以信用卡支付百貨類消費之總簽帳金額及筆數.csv')
df_all_other = get_all_data('各性別持卡人以信用卡支付其他類消費之總簽帳金額及筆數.csv')

# 將各產業資料存入資料庫
import sqlite3

def save_to_sqlite(df,table_name):  
    conn = sqlite3.connect('BDA.db')
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name}(
            yyyymm TEXT,
            year TEXT,
            month TEXT,
            quantity REAL,
            quantity_growth REAL,
            quantity_growth_percentage REAL,
            amount REAL,
            amount_growth REAL,
            amount_growth_percentage REAL,
            amount_per_transaction REAL,
            season TEXT
        )
    ''')
    conn.commit()
    df.to_sql(table_name, conn, if_exists='replace', index=False)
try:
    save_to_sqlite(df_all_food, 'all_food')
    save_to_sqlite(df_all_clothing, 'all_clothing')
    save_to_sqlite(df_all_housing, 'all_housing')
    save_to_sqlite(df_all_transportation, 'all_transportation')
    save_to_sqlite(df_all_edu, 'all_edu')
    save_to_sqlite(df_all_grocery, 'all_grocery')
    save_to_sqlite(df_all_other, 'all_other')
except Exception as e:
    print(f'存入資料庫失敗：{e}')
    
    
# 轉換成csv檔，以匯入power BI
list_df=[df_all_food,df_all_clothing,df_all_housing,df_all_transportation,df_all_edu,df_all_grocery,df_all_other]
list_csv_name=['all_food','all_clothing','all_housing','all_transportation','all_edu','all_grocery','all_other']
try:
    for df, csv_file in zip(list_df,list_csv_name):
        df.to_csv(csv_file+'.csv', index = 0)
except Exception as e:
    print(f'轉換csv失敗：{e}')
    

# 計算2019 Q1 ~ 2024 Q4刷卡筆數及金額成長率    
list_ind = ['食','衣','住','行','文教康樂','百貨','其他']
for df, ind in zip(list_df,list_ind):
    df_season = df.groupby('season', as_index=False)[['quantity', 'amount']].sum()
    quantity_growth = ((df_season.iloc[-1,1]-df_season.iloc[0,1])/df_season.iloc[0,1])*100
    amount_growth = ((df_season.iloc[-1,2]-df_season.iloc[0,2])/df_season.iloc[0,2])*100
    
    print('2019 Q1 ~ 2024 Q4') 
    print(ind)
    print('整體刷卡筆數成長',df_season.iloc[0,1],'->',df_season.iloc[-1,1])
    print('整體刷卡筆數成長率: {:.2f}%'.format(quantity_growth))
    print('整體刷卡金額成長',df_season.iloc[0,2],'->',df_season.iloc[-1,2])
    print('整體刷卡金額成長率: {:.2f}%'.format(amount_growth))
    print()
    
