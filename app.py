import sqlalchemy
from sqlalchemy import Column, String, Integer, Float, Date
from sqlalchemy.sql import text
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import csv
import datetime
import nltk
from lru_cache import lru_cache

Base = sqlalchemy.ext.declarative.declarative_base()

myLatLong = (-37.7893382, 175.3174343)

class StreetAddress(Base):
	# Example Row "POINT (170.9655665 -45.101049433299998)",
	# 1942438,1770171,4744,2D Ure Street,2D,2,,Ure Street,South Hill,
	# Waitaki District,Ure Street,2D Ure Street,South Hill,170.9655665,-45.1010494333

	__tablename__ = "streetAddress"

	WKT = Column(String(300)) # Point infomation in the format POINT ([lat] [long])

	id = Column(Integer, primary_key=True, autoincrement=False)
	rna_id = Column(Integer)
	rcl_id = Column(Integer)

	address = Column(String(300))
	house_number = Column(String(100))

	range_low = Column(Integer)
	range_high = Column(Integer)

	road_name = Column(String(200))
	locality = Column(String(200))
	territorial_authority = Column(String(200))
	road_name_utf8 = Column(String(200))
	address_utf8 = Column(String(200))
	locality_utf8 = Column(String(200))

	shape_X = Column(Float)
	shape_Y = Column(Float)

class WeatherStation(Base):
	# Example Row
	# 36593,H32895,01-Jan-2009,30-Jun-2015,100,Akaroa Ews,-43.80938,172.96574

	__tablename__ = "weatherStation"

	agent = Column(Integer, primary_key=True, autoincrement=False)
	network = Column(String(20))
	start_date = Column(Date)
	end_date = Column(Date)
	percent_complete = Column(Float)
	name = Column(String(200))
	p_lat = Column(Float)
	p_long = Column(Float)

POINT_TYPE_RAINFALL = 0
POINT_TYPE_WETDAYS = 1
POINT_TYPE_RAINDAYS = 40

class ClimateData(Base):
	# Example Row
	# Codes Total rainfall, wet days, rain days
	# 1041	2010	00	32.8	22.9	35.9	39.1	177.6	94.6	122.3	262.1	150.8	27.6	30.9	110.6	1107.2

	__tablename__ = "climateData"

	dataPointId = Column(Integer, primary_key=True)
	agent = Column(Integer)
	date = Column(Date)
	pointType = Column(Integer)
	point = Column(Float)

engine = sqlalchemy.create_engine("mysql://root:@localhost/govhack2015", echo=False)

Session = sqlalchemy.orm.sessionmaker(bind=engine)

def importClimateData(file):
	data = csv.DictReader(open(file, "r").read().split("\n"), delimiter="\t")
	session = Session()
	for row in data:
		for index, month in enumerate(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]):
			dataPoint = float(row[month]) if row[month] != "-" else None
			dateString = "01-%s-%s" % (month, row["Year "], )
			newRow = ClimateData(agent=row["Station"], date=datetime.date(int(row["Year "]), index + 1, 1), pointType=row["Stats_Code"], point=dataPoint)
			session.add(newRow)

	session.commit()
	session.close()

selectAddressWithLatLongQuery = """SELECT address, territorial_authority, ( 6371 * acos( cos( radians(shape_y) ) * cos( radians( :p_lat ) )
* cos( radians(:p_long) - radians(shape_x) ) + sin( radians(shape_y) ) * sin(radians(:p_lat)) ) ) AS distance
FROM streetAddress
HAVING distance < :distance
ORDER BY distance
LIMIT 1;"""

selectWeatherStationWithLatLongQuery = """SELECT agent, name, ( 6371 * acos( cos( radians(p_lat) ) * cos( radians( :p_lat ) )
* cos( radians(:p_long) - radians(p_long) ) + sin( radians(p_lat) ) * sin(radians(:p_lat)) ) ) AS distance
FROM weatherStation
WHERE percent_complete > 90 AND YEAR(end_date) = 2015
HAVING distance < :distance
ORDER BY distance
LIMIT 1;"""

connection = engine.connect()
session = Session()

def getAddressFromLatLong(latlong, distance=2000):
	p_lat, p_long = latlong
	for address, territorial_authority, distince in connection.execute(text(selectAddressWithLatLongQuery),
	p_lat=p_lat, p_long=p_long, distance=distance):
		return address, territorial_authority

def getWeatherStationsFromLatLong(latlong, distance=2000):
	p_lat, p_long = latlong
	for agent, name, distince in connection.execute(text(selectWeatherStationWithLatLongQuery),
	p_lat=p_lat, p_long=p_long, distance=distance):
		return agent, name

def getRainfallHistroyForAgent(agentId):
	return [(dataPoint.date, dataPoint.point) for dataPoint in session.query(ClimateData)\
		.filter(ClimateData.agent == agentId, ClimateData.pointType == POINT_TYPE_RAINFALL)]

@lru_cache(maxsize=None)
def getRailfallForAgent(agentId):
	dataPoints = [point for date, point in getRainfallHistroyForAgent(agentId) if point != None]
	if len(dataPoints) > 0:
		return sum(dataPoints) / len(dataPoints)
	else:
		return None

@lru_cache(maxsize=None)
def getLatLongFromAddress(address, territorial_authority):
	return session.query(StreetAddress.shape_Y, StreetAddress.shape_X)\
		.filter(StreetAddress.address == address, StreetAddress.territorial_authority == territorial_authority).all()

@lru_cache(maxsize=None)
def getRainfallForLatLong(latlong, distince=2000):
	agent, name = getWeatherStationsFromLatLong(latlong, distince)
	return getRailfallForAgent(agent)

def getRainfallForAllAddresses():
	failed = 0
	success = 0
	for address, territorial_authority, p_lat, p_long in session.query(StreetAddress.address,\
	StreetAddress.territorial_authority, StreetAddress.shape_Y, StreetAddress.shape_X):
		rainfall = getRainfallForLatLong((p_lat, p_long))
		if rainfall == None:
			failed += 1
		else:
			success += 1
		if (failed + success) % 1000 == 0:
			print failed, success

if __name__ == "__main__":
	Base.metadata.create_all(engine)
