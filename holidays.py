from irholiday import irHoliday

# initialise the class
calendar = irHoliday()

# export data to dataframe
df = calendar.to_df(1390, 1401)
df.to_pickle('data/holidays/full.pkl')
df.to_csv('data/holidays/full.csv')
print(df.shape, df.head())

df = calendar.get_holidays(1390, 1401)
df.to_pickle('data/holidays/holidays.pkl')
df.to_csv('data/holidays/holidays.csv')
print(df.shape, df.head())

# # export data to csv
# calendar.to_csv(1390, 1401, 'data/holidays/full.csv')
