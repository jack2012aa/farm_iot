> A simple python IoT edge layer used in farms.
# farm_IoT
During developing programs to read data from scales, air condition sensors, etc, I found that most IoT services are complicated to use. Hence, this project aims to develop a lightweigth edge layer package which can run independently in a farm computer. 

# Environment
This project is tested in Windows 11. \
com0com and ICDT Modbus RTU Slave are used to mimic RTU.

# User Guide
1. Download `app.exe`.
2. Create a `config` folder. Configuration files have to be inside this folder.
3. Write configurations. Please refer to `example_config.json` and source code (base/exporter/common_exporters, base/pipeline/common_filters and base/sensor/modbus).
這邊是整個使用中最複雜的部份，除了看example_config外可以去看上面列的三個 source code 的註解。
4. Create folder for `CsvExporter`.
5. Execute `app.exe`.

# Developer Guide
## `exporter` module
`exporter` module is one of the most basic module in this project. It defines two abstract classes: `DataGenerator` and `DataExporter`. `DataGenerator` can subscribe multiple `DataExporter`, and notify them when data is generated to export to different source.

`ExporterFactory` class helps to create `DataExporter` from dict (generally from json file) configurations.

`common_exporter` package defines some common exporter. For example, `CsvExporter`.

## `manage` module
`manage` module is another basic module in this project. It defines two abstract classes: `Worker` and `Manager`. A `Worker` object can set a `Manager` object to handle their problems.

This module is designed because I don't want to pass exceptions layer by layer. When `Worker` objects face to expected error, they can directly call their `Manager` to handle it.

`Manager` also define `initialize()` method, which is used to initialize its `Worker` object based on json file configuration. This is not its role; however I decide to implement like this for my ease.

## `gateway` module
`gateway` module define ways to connect to different gateways. Such as modbus RTU/TCP gateway, or even Internet switch. Each type of gateway should has its own `GatewayManager` to ensure the IoT environment is working correctly.

## `sensor` module
`sensor` module define abstract classes to connect to sensors using different protocols. Since even same type of sensor may use different protocols, these classes help to develop corresponding concrete classes.

`Sensor` is a `DataGenerator`, which means it can subscribe `DataExporter` objects. This may help to export _raw data_ from the `Sensor`.

`Sensor` objects can subscribe `Pipeline` objects to process read data. Each `Pipeline` work seperately.

## `pipeline` module
`pipeline` module define `Filter`, which processes `DataFrame` objects, and `Pipeline`, which is a series of `Filter`. In a `Pipeline`, `DataFrame` will be process layer by layer.

`Filter` is a `DataGenerator`. It can export data everytime it finishes processing.

`common_filter` package defines some common exporter. For example, `StdFilter`.