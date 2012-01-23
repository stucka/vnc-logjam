import urllib
import sqlite3
import re
import elementtree.ElementTree as ET


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
    mydb.execute('''create table if not exists ipgeo (myip text, placename text, countryname text, latlong text);''')
    mydb.execute('''create index if not exists ipindex on ipgeo (ip);''')
    mydb.execute('''insert into ipgeo values ("127.0.0.1", "There's no place like home", "home", "n/a")''')


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
mydb.execute('''alter table summary add column placename text;''')
mydb.execute('''alter table summary add column countryname text;''')
mydb.execute('''alter table summary add column latlong text;''')
#mydb.execute('''update summary set placename = "SQLitesucks";''')
mydb.execute('''update summary set placename = (select placename
                from ipgeo where summary.ip = ipgeo.myip);''')
mydb.execute('''select ip from summary where length(placename) IS NULL; ''')
rows = mydb.fetchall()
for row in rows:
    remoteurl = "http://api.hostip.info/?ip=" + row[0]
    remotefile = urllib.urlopen(remoteurl)
    tree = ET.parse(remotefile)
    root = tree.getroot()
    placename = root[3][0][1].text
    countryname = root[3][0][2].text
    latlong = ""
    try:
        latlong = root[3][0][4][0][0][0].text
        print "Latlong found for ", row[0], ", ", placename, ",", "countryname"
    except (IndexError):
        print "No latlong found for ", row[0], "but it's ", placename, " in ", countryname
        latlong = ""
        pass
    mydb.execute('insert into ipgeo values (?,?,?,?);', [row[0], placename, countryname, latlong])
#    raw_input("Press Enter to try the next one")

mydb.execute('''update summary set placename = (select placename
                from ipgeo where summary.ip = ipgeo.myip);''')
mydb.execute('''update summary set countryname = (select countryname
                from ipgeo where summary.ip = ipgeo.myip);''')
mydb.execute('''update summary set latlong = (select latlong
                from ipgeo where summary.ip = ipgeo.myip);''')





#mydb.execute('''update summary set placename = t1.placename from summary t2
#                   inner
#                   join ipgeo t1 on t2.ip = t1.ip;''')


#,
#                   countryname = ipgeo.countryname,
#                   latlong = ipgeo.countryname
#mydb.execute('''update summary, ipgeo set summary.name=ipgeo.name , summary.countryname=ipgeo.countryname, summary.latlong=ipgeo.latlongwhere ipgeo.ip=summary.ip''')
## Gotta figure out what IPs we haven't done yet.
mydbconn.commit()

##remotefile = urllib.urlopen("http://api.hostip.info/?ip=12.215.42.19")
##tree = ET.parse(remotefile)
##root = tree.getroot()
##
##print root[3][0][0].text
##print root[3][0][1].text, "301t as placename"
##print root[3][0][2].text, "302t as countryname"
##print root[3][0][4][0][0][0].text, "304000t as coordinates" 
###300.text=ip


## root = ET.parse(remotefile)
## print iter.attrib.get("ipLocation")

## iter = root.getiterator()

##
##for element in iter:
##    #First the element tag name
##    print "Element:", element.tag
##    #Next the attributes (available on the instance itself using
##    #the Python dictionary protocol
##    if element.keys():
##        print "\tAttributes:"
##        for name, value in element.items():
##            print "\t\tName: '%s', Value: '%s'"%(name, value)
##    #Next the child elements and text
##    print "\tChildren:"
##    #Text that precedes all child elements (may be None)
##    if element.text:
##        text = element.text
##        text = len(text) > 40 and text[:40] + "..." or text
##        print "\t\tText:", repr(text)
##    if element.getchildren():
##        #Can also use: "for child in element.getchildren():"
##        for child in element:
##            #Child element tag name
##            print "\t\tElement", child.tag
##            #The "tail" on each child element consists of the text
##            #that comes after it in the parent element content, but
##            #before its next sibling.
##            if child.tail:
##                text = child.tail
##                text = len(text) > 40 and text[:40] + "..." or text
##                print "\t\tText:", repr(text)  
