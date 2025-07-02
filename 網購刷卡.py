import pandas as pd

# 全國網購刷卡資料整理
# 讀入原始資料
df_EC_org = pd.read_csv('以信用卡支付網路購物(EC)消費之各產業類別的金額及筆數.csv')
# 改columns names，分割yyyy/mm，以利之後選取資料
df_EC_org.columns = ['yyyy/mm','region','industry','quantity','amount']
df_EC_org.insert(1, column ='year',value = df_EC_org['yyyy/mm'].astype(str).str[:4])
df_EC_org.insert(2, column ='month',value = df_EC_org['yyyy/mm'].astype(str).str[4:])
# 取出2019-2024年之資料
filter_EC_19to24 = df_EC_org['year'].isin(['2019','2020','2021','2022','2023','2024'])
df_EC_19to24 = df_EC_org[filter_EC_19to24].reset_index(drop = True)
# 新增欄位-平均單筆消費金額
df_EC_19to24['amount per transaction'] = df_EC_19to24['amount']/df_EC_19to24['quantity']

# 以產業別分組，之後進行分析以df_EC_by_ind.get_group選取即可
df_EC_by_ind = df_EC_19to24.groupby('industry')

# 取得各產業資料
def get_df_ind(ind):
    df = df_EC_by_ind.get_group(ind).reset_index(drop=True)
    # 新增欄位-與前月相比之「筆數」差距
    df.insert(6, column='quantity growth', value=df['quantity'].diff())
    # 新增欄位-與前月相比之「金額」差距
    df.insert(8, column='amount growth', value=df['amount'].diff())
    # 新增欄位-筆數增長率&金額增長率
    df.insert(7, column='quantity growth percentage', value='nan')
    df.insert(10, column='amount growth percentage', value='nan')
    df['season']='nan'
    for j in range(len(df['quantity'])):
        if j == 0:
            df.loc[j, 'quantity growth percentage'] = 'nan'
            df.loc[j, 'amount growth percentage'] = 'nan'
        else:
            df.iloc[j, 7] = (df.iloc[j, 6] / df.iloc[j - 1, 5] * 100)
            df.iloc[j, 10] = (df.iloc[j, 9] / df.iloc[j - 1, 8] * 100)
        
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

df_EC_food = get_df_ind('食')
df_EC_clothing = get_df_ind('衣')
df_EC_housing = get_df_ind('住')
df_EC_transportation = get_df_ind('行')
df_EC_edu = get_df_ind('文教康樂')
df_EC_grocery = get_df_ind('百貨')
df_EC_other = get_df_ind('其他')

# 將各產業資料存入資料庫
import sqlite3

def save_to_sqlite(df,table_name):  
    conn = sqlite3.connect('BDA.db')
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name}(
            yyyymm TEXT,
            year TEXT,
            month TEXT,
            region TEXT,
            industry TEXT,
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
    save_to_sqlite(df_EC_food, 'EC_food')
    save_to_sqlite(df_EC_clothing, 'EC_clothing')
    save_to_sqlite(df_EC_housing, 'EC_housing')
    save_to_sqlite(df_EC_transportation, 'EC_transportation')
    save_to_sqlite(df_EC_edu, 'EC_edu')
    save_to_sqlite(df_EC_grocery, 'EC_grocery')
    save_to_sqlite(df_EC_other, 'EC_other')
except Exception as e:
    print(f'存入資料庫失敗：{e}')

# 轉換成csv檔，以匯入power BI
list_df=[df_EC_food,df_EC_clothing,df_EC_housing,df_EC_transportation,df_EC_edu,df_EC_grocery,df_EC_other]
list_csv_name=['EC_food','EC_clothing','EC_housing','EC_transportation','EC_edu','EC_grocery','EC_other']
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
    print('網購刷卡筆數成長',df_season.iloc[0,1],'->',df_season.iloc[-1,1])
    print('網購刷卡筆數成長率: {:.2f}%'.format(quantity_growth))
    print('網購刷卡金額成長',df_season.iloc[0,2],'->',df_season.iloc[-1,2])
    print('網購刷卡金額成長率: {:.2f}%'.format(amount_growth))
    print()









