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
4. Create folder for `CsvExporter`.
5. Execute `app.exe`.

## 如何設定 config.json
一開始要先確定設備使用哪種通訊方式。現在支持的有三種：1. modbus tcp, 2. modbus rtu, 3. mqtt。 每種通訊方式需要有自己的一個配置文件。以下是三種配置文件的範例：

### Modbus TCP
```json
{
    "//comment1": "這是 modbus tcp gateway 的範例，使用時請刪掉這一行", 
    "//comment2": "沒有用被雙引號框住的位置代表它是個數值，實際設定時也不要框住它",
    "閘道器名稱1 名稱不影響程式": {
        "host": "閘道器 ip 位置", 
        "port": 閘道器開放的通訊埠，通常是502,
    }, 
    "閘道器名稱2": {
        "host": "閘道器2 ip 位置", 
        "port": 閘道器2開放的通訊埠，通常是502,
    }
    ...
}
```

### Modbus RTU
```json
{
    "//comment1": "這是 modbus rtu gateway 的範例，使用時請刪掉這一行", 
    "//comment2": "沒有用被雙引號框住的位置代表它是個數值，實際設定時也不要框住它",
    "閘道器名稱1 名稱不影響程式": {
        "port": "閘道器連接到電腦上的序列埠位置，序列埠通常叫做 COM1, COM2, ...",
        "baudrate": 設備的 baudrate，在設備的使用手冊上會標註,
        "bytesize": modbus 傳輸一份資料的字節大小，通常是8，在設備的使用手冊上會標註,
        "parity": "在設備的使用手冊上會標註",
        "stopbits": modbus 字節的終止符，通常是1，在設備的使用手冊上會標註,
        "time_out": 響應時間上限(秒)，通常設為5
    }, 
    "閘道器名稱1 名稱不影響程式": {
        ...
    }, 
}

```

### MQTT
```json
{
    "//comment1": "這是 mqtt 的範例，使用時請刪掉這一行", 
    "//comment2": "broker 只會有一個，因此配置文件一定只有這兩行",
    "host": "MQTT broker 的 ip 位置或網域名稱", 
    "port": "MQTT broker 的通訊埠，通常是1883"
}
```

設置完閘道器之後是設置感測器，感測器的配置中又要求要配置`exporter`和`pipeline`。先講解這兩者：

### exporter
```json
{
    "//comment1": "這是 exporter 的範例，使用時請刪掉這一行", 
    "//comment2": "目前支援的 exporter 有 CsvExporter, WeeklyCsvExporter, PrintExporter, ScatterPlotExporter", 
    "exporters": [
        "//comment3: 接下來列出所有想要用的 exporters",
        {"type": "CsvExporter", "path": "儲存輸出的位置", "file_name": "輸出檔案名稱"}, 
        {"type": "WeeklyCsvExporter", "path": "儲存輸出的位置", "file_name": "輸出檔案名稱"},
        {"type": "PrintExporter"}, 
        {"type": "ScatterPlotExporter", "path": "儲存輸出的位置", "file_name": "輸出檔案名稱", "threshold": 一張圖要累積多少個資料點後再輸出}
    ]
}
```

### pipelines
```json
{
    "//comment1": "這是 pipeline 的範例，使用時請刪掉這一行", 
    "//comment2": "目前支援的 filter 有 StdFilter, BatchAverageFilter, MovingAverageFilter, BatchConsumptionFilterBySensor",
    "pipelines": [
        "//comment3: 接下來列出想要的 pipelines 組合，可以同時搭建多條 pipelines", 
        [{"type": "StdFilter"}, {"type": "BatchAverageFilter", "exporters": ["與上面的 exporter 範例寫法相同"]}], 
        [{"type": "MovingAverageFilter", "max_length": 移動平均的樣本數}, {"type": "BatchConsumptionFilterBySensor", "gate_names": "參考的自動下料閥在配置表中的名稱", "exporters": ["與上面的 exporter 範例寫法相同"]}]
    ]
}
```

目前支援的感測器有 `FeedScaleTCPSensor`, `FeedScaleRTUSensor`, `AirTCPSensor`, `AutoFeederGateMQTTSensor`，每個 sensor 可以連接到多個 pipelines 和 exporters。

### FeedScale
```json
{
    "飼料秤名稱1，必須是唯一的": {
        "connection type": "TCP or RTU (先示範 RTU)", 
        "connection settings":{
            "port": "RTU 閘道器連接的電腦序列埠位置，通常叫 COM1, COM2, ...", 
            "slave": 要呼叫的飼料秤機台編號，由廠商設定，是數字
        }, 
        "length": 一批次資料的長度，可以自由設定, 
        "duration": 一批次資料中兩筆資料間格的秒數，可以小於1,
        "waiting_time": 兩批次資料間格的秒數, 
        "belonging": ["負責人名稱，可以在負責人表中登錄以在感測器斷訊時接到email通知"],
        "exporters": ["同前面範例"], 
        "pipelines": ["同前面範例"] 
    }, 
    "飼料秤名稱2，必須是唯一的": {
        "connection type": "TCP", 
        "connection settings":{
            "host": "閘道器所在 ip", 
            "slave": 要呼叫的飼料秤機台編號，由廠商設定，是數字
        }, 
        "length": 一批次資料的長度，可以自由設定, 
        "duration": 一批次資料中兩筆資料間格的秒數，可以小於1,
        "waiting_time": 兩批次資料間格的秒數, 
        "belonging": ["負責人名稱，可以在負責人表中登錄以在感測器斷訊時接到email通知"],
        "exporters": ["同前面範例"], 
        "pipelines": ["同前面範例"] 
    }, 
}
```

### Air
```json
{
    "氣體感測器名稱，必須是唯一的":{
        "connection type": "TCP (目前只有 TCP)", 
        "connection settings":{
            "port": "RTU 閘道器連接的電腦序列埠位置，通常叫 COM1, COM2, ...", 
            "slave": 要呼叫的飼料秤機台編號，由廠商設定，是數字
        }, 
        "length": 一批次資料的長度，可以自由設定, 
        "duration": 一批次資料中兩筆資料間格的秒數，可以小於1,
        "waiting_time": 兩批次資料間格的秒數, 
        "belonging": ["負責人名稱，可以在負責人表中登錄以在感測器斷訊時接到email通知"],
        "exporters": ["同前面範例"], 
        "pipelines": ["同前面範例"] 
    }, 
}
```

### AutoFeederGate
```json
{
    "自動下料閥名稱，必須是唯一的": {
        "connection type": "MQTT", 
        "data_topic": "自動下料閥傳輸的資料 topic", 
        "heartbeat_topic": "自動下料閥傳輸的心跳 topic", 
        "timeout": "確認連線的秒數",
        "exporters": ["同前面範例"], 
        "//comment1": "不支援現有pipelines"
    }
}
```

可以設定負責人表，以在設備出問題時接收到email警報。
### Email
```json
{
    "subject": "Email 標題", 
    "from": "寄信者名稱", 
    "//comment1": "這裡的 email 帳號密碼我另外給",
    "domain_mail_account": "寄信者 email", 
    "domain_mail_password": "寄信者密碼", 
    "employeer_emails": {
        "負責人名稱，需與填寫在設備配置裡的相同": "負責人 email"
    }
}
```

最後寫一個主設定檔，告訴程式你上面寫的設定檔在什麼位置
### Main config
```json
{   
    "//comment1": "名稱必須是 config.json，必須放在 app.exe 文件夾下的 config 文件夾",
    "gateway_managers":[
        "//comment1: 列出要連接哪些種類的閘道器", 
        ["ModbusRTUGatewayManager", "設定檔位置"], 
        ["ModbusTCPGatewayManager", "設定檔位置"], 
        ["MQTTClientManager", "設定檔位置"]
    ], 
    "sensor_managers":[
        "//comment2: 列出會用到哪些種類的感測器", 
        ["FeedScaleManager", "設定檔位置"], 
        ["AirSensorManager", "設定檔位置"], 
        ["AutoFeederGateManager", "設定檔位置"]
    ], 
    "email_config_path": "設定檔位置"
}
```

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