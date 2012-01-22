import urllib
import sqlite3
import re

logfile=open(r'./mslogon.log','r')
mydbconn = sqlite3.connect('./vnc-logjam.sqlite')
mydb = mydbconn.cursor()
mydb.execute('''drop table if exists staging;''')
mydb.execute('''create table staging (eventtime varchar(50), eventtype char(1), ip varchar(20));''')
mydb.execute('''create index ipindex on staging (ip);''')

mydb.execute('''drop table if exists summary;''')

mydb.execute('''select count(*) from sqlite_master where type='table' and name='ipgeo';''')
sqlreturn=mydb.fetchone()
if sqlreturn[0] == 0:
    mydb.execute('''create table if not exists ipgeo (ip text, name text, countryname text, latlong text);''')
    mydb.execute('''create index if not exists ipindex on ipgeo (ip);''')

for line in logfile:
    datestring=re.search('(.*)\s\s\s', line).group(0)
    wordcommand=re.search('\s\s\s(\w+)',line).group(0).strip()
    ip=re.search('(\S*).$',line).group(0).strip()
    if wordcommand=="Client":
        ip=re.search('(Client )(.*)( disconnected)', line).group(2).strip()
        wordcommand="down"
    elif wordcommand=="Connection": wordcommand="up"
    elif wordcommand=="Invalid": wordcommand="bad"
    mydb.execute('insert into staging values (?,?,?)', [datestring, wordcommand, ip])

mydb.execute('''create table summary AS select ip, count(*) as mycount from staging group by ip order by count(*) desc;''')
mydb.execute('''create index if not exists ipindex on summary (ip);''')
mydb.execute('''alter table summary add column name text;''')
mydb.execute('''alter table summary add column countryname text;''')
mydb.execute('''alter table summary add column latlong text;''')
mydb.execute('''update summary, ipgeo set summary.name=ipgeo.name , summary.countryname=ipgeo.countryname, summary.latlong=ipgeo.latlongwhere ipgeo.ip=summary.ip''')


mydbconn.commit()



