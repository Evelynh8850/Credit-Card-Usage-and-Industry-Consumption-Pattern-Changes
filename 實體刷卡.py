import pandas as pd
import sqlite3

conn = sqlite3.connect('BDA.db')

# 從sqlite取用各產業全體刷卡資料及網購資料，兩份資料相減取得實體刷卡資料
def get_ph_data(all_file, EC_file):
    
    df_all = pd.read_sql(f'''SELECT `yyyy/mm`, season, amount, quantity 
                            FROM {all_file}''',conn)
    df_EC = pd.read_sql(f'''SELECT `yyyy/mm`, season, amount, quantity
                          FROM {EC_file}''', conn)
    df = pd.DataFrame(df_all['season'],columns = ['season','amount','quantity','amount_per_transaction'])
    df['yyyy/mm'] = df_all['yyyy/mm']
    
    for i in range(len(df['season'])):
        df.loc[i,'amount'] = df_all.loc[i,'amount']-df_EC.loc[i,'amount']
        df.loc[i,'quantity'] = df_all.loc[i,'quantity']-df_EC.loc[i,'quantity']
        df.loc[i,'amount_per_transaction'] = df.loc[i,'amount']/  df.loc[i,'quantity']  
    
    # 將資料型態從'numpy.int64'轉為float
    df['amount'] = df['amount'].astype(float)
    df['quantity'] = df['quantity'].astype(float)
    return df

ph_food = get_ph_data('all_food', 'EC_food')
ph_clothing = get_ph_data('all_clothing', 'EC_clothing')
ph_housing = get_ph_data('all_housing', 'EC_housing')
ph_transportation = get_ph_data('all_transportation', 'EC_transportation')
ph_edu = get_ph_data('all_edu', 'EC_edu')
ph_grocery = get_ph_data('all_grocery', 'EC_grocery')

def save_to_sqlite(df,table_name):  
    conn = sqlite3.connect('BDA.db')
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name}(
            season TEXT,
            amount REAL,
            quantity REAL,
            amount_per_transaction REAL
        )
    ''')
    conn.commit()
    df.to_sql(table_name, conn, if_exists='replace', index=False)
try:
    save_to_sqlite(ph_food, 'ph_food')
    save_to_sqlite(ph_clothing, 'ph_clothing')
    save_to_sqlite(ph_housing, 'ph_housing')
    save_to_sqlite(ph_transportation, 'ph_transportation')
    save_to_sqlite(ph_edu, 'ph_edu')
    save_to_sqlite(ph_grocery, 'ph_grocery')
except Exception as e:
    print(f'存入資料庫失敗：{e}')


# 檢視amount欄位內的資料型態
series_amount = pd.Series(ph_food['amount'])
for k in range(72):
    print(type(series_amount[k]))


list_df=[ph_food,ph_clothing,ph_housing,ph_transportation,ph_edu,ph_grocery]
list_csv_name=['ph_food','ph_clothing','ph_housing','ph_transportation','ph_edu','ph_grocery']
try:
    for df, csv_file in zip(list_df,list_csv_name):
        df.to_csv(csv_file+'.csv', index = 0)
    print('轉換完畢')
except Exception as e:
    print(f'轉換csv失敗：{e}')


# 計算2019 Q1 ~ 2024 Q4刷卡筆數及金額成長率
list_ind = ['食','衣','住','行','文教康樂','百貨']
for df, ind in zip(list_df,list_ind):
    df_season = df.groupby('season', as_index=False)[['quantity', 'amount']].sum()
    quantity_growth = ((df_season.iloc[-1,1]-df_season.iloc[0,1])/df_season.iloc[0,1])*100
    amount_growth = ((df_season.iloc[-1,2]-df_season.iloc[0,2])/df_season.iloc[0,2])*100
    
    print('2019 Q1 ~ 2024 Q4') 
    print(ind)
    print('實體刷卡筆數成長',int(df_season.iloc[0,1]),'->',int(df_season.iloc[-1,1]))
    print('實體刷卡筆數成長率: {:.2f}%'.format(quantity_growth))
    print('實體刷卡金額成長',int(df_season.iloc[0,2]),'->',int(df_season.iloc[-1,2]))
    print('實體刷卡金額成長率: {:.2f}%'.format(amount_growth))
    print()
