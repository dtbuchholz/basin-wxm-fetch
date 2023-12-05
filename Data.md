# Data

_Generated on **2023-12-05** for data in range **2023-11-20 00:00:00** to **2023-11-23 23:59:59**._

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
- `model` (varchar): Model of the device (either WXM WS1000 or WXM WS2000).
- `name` (varchar): Name of the device.
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
|------------|---------------|---------------|-------------------|-----------------|---------------------|-------------|----------|---------------------------|
| 2023-12-05 | 1700438400000 | 1700783999000 | 3414              | 871ec93adffffff | 96307242272.794     | 8.908       | 79.626   | 1421.832                  |

| Wind Speed | Wind Gust | Wind Direction | Illuminance | Solar Irradiance | Fo Uv   | Uv Index | Precipitation Rate | Pressure |
|------------|-----------|----------------|-------------|------------------|---------|----------|--------------------|----------|
| 1.038      | 1.374     | 189.856        | 271303.023  | 2136.558         | 224.874 | 0.255    | 0.068              | 993.612  |

## Historical plots

### Number Of Devices

![Number Of Devices](./assets/number_of_devices.png)

### Total Precipitation

![Total Precipitation](./assets/total_precipitation.png)

### Temperature

![Temperature](./assets/temperature.png)

### Humidity

![Humidity](./assets/humidity.png)

### Precipitation Accumulated

![Precipitation Accumulated](./assets/precipitation_accumulated.png)

### Wind Speed

![Wind Speed](./assets/wind_speed.png)

### Wind Gust

![Wind Gust](./assets/wind_gust.png)

### Wind Direction

![Wind Direction](./assets/wind_direction.png)

### Illuminance

![Illuminance](./assets/illuminance.png)

### Solar Irradiance

![Solar Irradiance](./assets/solar_irradiance.png)

### Fo Uv

![Fo Uv](./assets/fo_uv.png)

### Uv Index

![Uv Index](./assets/uv_index.png)

### Precipitation Rate

![Precipitation Rate](./assets/precipitation_rate.png)

### Pressure

![Pressure](./assets/pressure.png)

