import datetime

#datetime object -- attributes and methods
datetime_obj = datetime.datetime.now()
print("datetime_obj: ", datetime_obj)
print("datetime_obj year: ", datetime_obj.year)
print("datetime_obj month: ", datetime_obj.month)
print("datetime_obj isocalendar (tuple of year, week # and day of week) : ", datetime_obj.isocalendar())
print("datetime_obj day: ", datetime_obj.day)
print("datetime_obj hour: ", datetime_obj.hour)
print("datetime_obj minute: ", datetime_obj.minute)
print("datetime_obj weekday: ", datetime_obj.weekday())

#converting datetime object into UNIX timestamp 
unix_timestamp = datetime.datetime.timestamp(datetime_obj)
print("unix_timestamp: ", unix_timestamp)
#converting UNIX timestamp into datetime object  
datetime_obj_new = datetime.datetime.fromtimestamp(unix_timestamp)
print("datetime_obj_new: ", datetime_obj_new)

#converting from datetime object to string 
year=datetime_obj.strftime("%Y")
print("String year: ", year)
month=datetime_obj.strftime("%m")
print("String month: ", month)
day=datetime_obj.strftime("%d")
print("String day: ", day)
hour=datetime_obj.strftime("%H")
print("String hour: ", hour)
min=datetime_obj.strftime("%M")
print("String min: ", min)
second=datetime_obj.strftime("%S")
print("String second: ", second)
str_time=datetime_obj.strftime("%H:%M:%S")
print("String str_time: ", str_time)
str_date_time=datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
print("String str_date_time: ", str_date_time)

#converting from string to datetime object  
dt_string = "12/11/2018 09:15:32"
datetime_obj_1 = datetime.datetime.strptime(dt_string, "%m/%d/%Y %H:%M:%S")
print("datetime_obj_1: ", datetime_obj_1)
datetime_obj_2 = datetime.datetime.strptime(dt_string, "%d/%m/%Y %H:%M:%S")
print("datetime_obj_2: ", datetime_obj_2)






