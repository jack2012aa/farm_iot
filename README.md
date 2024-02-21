> A simple python IoT edge layer used in farms.
# farm_IoT
During developing programs to read data from scales, air condition sensors, etc, I found that most IoT services are complicated to use. Hence, this project aims to develop a lightweigth edge layer package which can run independently in a farm computer. 

# Environment
This project is tested in Windows 11. \
com0com and ICDT Modbus RTU Slave are use to mimic RTU.

# Usage
Define a `Sensor` by implementing a child of `Reader`. Register `Pipeline` to `Sensor` and `DataExporter` to `DataGenerator`. \ 
Use `Manager` to manage sensors and gateways.