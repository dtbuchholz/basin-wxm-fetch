# Data

_Generated on **2024-02-25** for data in range **2024-02-19 00:00:00** to **2024-02-25 00:00:00**._

The schema for the raw data is as follows:

- `device_id` (varchar): Unique identifier for the device.
- `timestamp` (bigint): Timestamp (unix milliseconds).
- `temperature` (double): Temperature (Celsius).
- `humidity` (double): Relative humidity reading (%).
- `precipitation_accumulated` (double): Total precipitation (millimeters).
- `wind_speed` (double): Wind speed (meters per second).
- `wind_gust` (double): Wind gust (meters per second).
- `wind_direction` (double): Wind direction (degrees).
- `illuminance` (double): Illuminance (lux).
- `solar_irradiance` (double): Solar irradiance (watts per square meter).
- `fo_uv` (double): UV-related index value.
- `uv_index` (double): UV index.
- `precipitation_rate` (double): Precipitation rate (millimeters per hour).
- `pressure` (double): Pressure (HectoPascals).
- `name` (varchar): Name of the device.
- `utc_datetime` (varchar): Timestamp from the raw data in UTC.
- `model` (varchar): Model of the device (either WXM WS1000 or WXM WS2000).
- `cell_id` (varchar): Cell ID of the device.
- `lat` (double): Latitude of the cell.
- `lon` (double): Longitude of the cell.

Most of the columns above are included in the mean calculations, and there are three additional columns for aggregates:

- `total_precipitation` (double): Total `precipitation_accumulated` (millimeters).
- `number_of_devices` (int): Count of unique `device_id` values.
- `cell_id_mode` (varchar): Most common `cell_id` value.

And three additional columns for run metadata:

- `job_date` (varchar): Date the job was run.
- `range_start` (bigint): Start of the query range (unix milliseconds).
- `range_end` (bigint): End of the query range (unix milliseconds).

## Averages & cumulative metrics

| Job Date   | Range Start   | Range End     | Number Of Devices | Cell Id Mode    | Total Precipitation | Temperature | Humidity | Precipitation Accumulated |
| ---------- | ------------- | ------------- | ----------------- | --------------- | ------------------- | ----------- | -------- | ------------------------- |
| 2024-03-01 | 1708300800000 | 1708819200000 | 4605              | 871eda743ffffff | 211525643130.935    | 8.622       | 77.257   | 2182.201                  |

| Wind Speed | Wind Gust | Wind Direction | Illuminance | Solar Irradiance | Fo Uv   | Uv Index | Precipitation Rate | Pressure |
| ---------- | --------- | -------------- | ----------- | ---------------- | ------- | -------- | ------------------ | -------- |
| 0.851      | 1.189     | 188.817        | 328272.629  | 2586.728         | 499.633 | 0.527    | 0.069              | 989.563  |

## Precipitation accumulated maps

### North America

![North America](./assets/maps/precipitation_map_north_america.png)

### South America

![South America](./assets/maps/precipitation_map_south_america.png)

### Africa

![Africa](./assets/maps/precipitation_map_africa.png)

### Europe

![Europe](./assets/maps/precipitation_map_europe.png)

### Asia

![Asia](./assets/maps/precipitation_map_asia.png)

### Australia

![Australia](./assets/maps/precipitation_map_australia.png)

## Historical plots

### Number Of Devices

![Number Of Devices](./assets/averages/number_of_devices.png)

### Total Precipitation

![Total Precipitation](./assets/averages/total_precipitation.png)

### Temperature

![Temperature](./assets/averages/temperature.png)

### Humidity

![Humidity](./assets/averages/humidity.png)

### Precipitation Accumulated

![Precipitation Accumulated](./assets/averages/precipitation_accumulated.png)

### Wind Speed

![Wind Speed](./assets/averages/wind_speed.png)

### Wind Gust

![Wind Gust](./assets/averages/wind_gust.png)

### Wind Direction

![Wind Direction](./assets/averages/wind_direction.png)

### Illuminance

![Illuminance](./assets/averages/illuminance.png)

### Solar Irradiance

![Solar Irradiance](./assets/averages/solar_irradiance.png)

### Fo Uv

![Fo Uv](./assets/averages/fo_uv.png)

### Uv Index

![Uv Index](./assets/averages/uv_index.png)

### Precipitation Rate

![Precipitation Rate](./assets/averages/precipitation_rate.png)

### Pressure

![Pressure](./assets/averages/pressure.png)
