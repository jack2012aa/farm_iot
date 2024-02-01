> A simple python IoT edge layer used in farms.
# farm_IoT
During developing programs to read data from scales, air condition sensors, etc, I found that most IoT services are complicated to use. Hence, this project aims to develop a lightweigth edge layer package which can run independently in a farm computer. 

# Usage
Define a `Sensor` by implementing its `Reader`, `Filter` and `Pipeline`. Use the `Client` to manage list of sensors.