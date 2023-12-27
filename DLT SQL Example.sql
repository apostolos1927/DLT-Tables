-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE BronzeTurbineTS
AS SELECT * FROM hive_metastore.example.bronzeturbinet

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE BronzeWeatherTS
AS SELECT * FROM hive_metastore.example.bronzeweathert

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE SilverTurbineSQL
(
  CONSTRAINT angleConstraint EXPECT (AverageAngle IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT rpmConstraint EXPECT (AverageRpm > 0) ON VIOLATION FAIL UPDATE
)
AS
SELECT messageID,avg(deviceRPM) as AverageRpm,avg(deviceAngle) as AverageAngle FROM (
SELECT
  CAST(messageID AS INT) as messageID,
  angle as deviceAngle,
  rpm as deviceRPM,
  timestamp
FROM
  STREAM(LIVE.BronzeTurbineTS)
)
GROUP BY messageID,window(timestamp, '5 seconds')

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE SilverWeatherSQL
(
  CONSTRAINT temperatureConstraint EXPECT (AverageTemperature IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT humidityConstraint EXPECT (AverageHumidity > 0) ON VIOLATION FAIL UPDATE
)
AS 
SELECT messageID,winddirection,avg(temperature) as AverageTemperature,avg(humidity) as AverageHumidity, avg(windspeed) as AverageWindspeed FROM (
SELECT
  *
FROM
  STREAM(LIVE.BronzeWeatherTS)
WHERE winddirection = 'Left'
)
GROUP BY messageID,winddirection,window(timestamp, '5 seconds')

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE GoldSQL
AS 
SELECT SilverWeatherSQL.winddirection,SilverWeatherSQL.AverageTemperature,SilverTurbineSQL.* FROM LIVE.SilverWeatherSQL as SilverWeatherSQL
INNER JOIN Live.SilverTurbineSQL as SilverTurbineSQL
ON SilverWeatherSQL.messageID  =SilverTurbineSQL.messageID
WHERE AverageTemperature <> 5
