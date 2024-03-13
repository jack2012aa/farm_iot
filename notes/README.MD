# Update Log
## 2024-03-13
* Tested on how to process weight data. Using BatchAvg, MovingAvg and Std seems good, unrelated with there order. 
It is impossible to verify there correctness. This will be done when calculating everyday feed consuming.
* Create more thorough test of time series filters.
## 2024-03-12
* Add new filters.
* Add a plot exporter.
## 2024-03-07
* Wrote app.py.
* First release.
* Add csv reader.
## 2024-03-06
* Wrote exporter factory and pipeline factory to help sensor manager creating sensor.
* Wrote initialize and run method of scale manager and tested.
* Wrote send alarm email method.
## 2024-03-05
* Wrote the initialize method of RTUGatewayManager and tested.
## 2024-03-04
* Modify modbus sensor due to unsopported thread safe.
* Wrote feed scale rtu sensor and tested.
## 2024-02-21
* Restructure `filter` to universal version and tested.
* Restructure `gateway` and tested.
* Tested common exporters.
* Restructure `export` interface and tested.
* Add `manage` interface and tested.
## 2024-02-04
* Implemented sensor and tested.
## 2024-02-03
* Degraded `pymodbus` to 3.5.2 so it can run successfully in Windows.
* Tested reader.
* Implemented filters and tested.
## 2024-02-02
* Wrote some feed_scale classes.
* Tested exporter.
* Next: test reader, then implement sensor and sensor group.
* `pymodbus` and `pyserial` do not support Windows. I tried to use Docker but wsl2 does not allow passing serial port to containers cause Microsoft is a piece of shit.
Hence I decide to move the project to ubuntu.
## 2024-02-01
* Began the project.