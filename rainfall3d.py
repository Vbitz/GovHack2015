import app
import matplotlib.pyplot as pyp
import sqlalchemy
import sys
from matplotlib import cm

x = []
y = []
rainfall = []
all = []
for p_lat, p_long, agent, name in app.session.query(app.WeatherStation.p_lat, app.WeatherStation.p_long, app.WeatherStation.agent, app.WeatherStation.name).filter(app.WeatherStation.percent_complete > 90, sqlalchemy.extract("year", app.WeatherStation.end_date) == 2015, app.WeatherStation.p_long > 0):
    agentRainfall = app.getRailfallForAgent(agent)
    if agentRainfall != None:
        x += [-p_lat]
        y += [p_long]
        rainfall += [agentRainfall]
	all += [((p_lat, p_long), agentRainfall, agent, name)]

if sys.argv[1] == "maxRainfall":
	print all[rainfall.index(max(rainfall))]
	sys.exit(0)

from mpl_toolkits.mplot3d import Axes3D
fig = pyp.figure()
ax = fig.add_subplot(111, projection='3d')

if sys.argv[1] == "scatter":
	ax.scatter(x, y, rainfall, s=rainfall)
elif sys.argv[1] == "trisurf":
	ax.plot_trisurf(x, y, rainfall, cmap=cm.coolwarm)

pyp.show(ax)
