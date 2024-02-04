# Update Log
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