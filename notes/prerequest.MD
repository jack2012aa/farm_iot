# 使用前所需基礎知識
## 目錄
在了解以下章節前，請勿改動程式碼以免造成不可預期的錯誤。
1. [Import](#import)
    1. [工作路徑](#工作路徑)
    2. [絕對路徑與相對路徑](#絕對路徑與相對路徑)
    3. [Module 和 Package](#module-和-package)
2. [虛擬環境](#虛擬環境)
    1. [版本管理-pyenv](#版本管理-pyenv)
    2. [虛擬環境-venv](#虛擬環境-venv)
3. [物件導向](#object-oriented-programming)
    1. [類別 Class](#類別-class)
    2. [繼承 Inheritance](#繼承-inheritance)
4. [單元測試](#單元測試)
    1. [Asyncio test](#asyncio-test)
5. [Asyncio](#asyncio)
    1. [Coroutine, task](#corountine-task)
    2. [Event loop](#event-loop)
    3. [Asyncio 與 multithreading](#asyncio-與-multithreading)

除此之外，請盡量遵循 PEP8 的風格撰寫程式碼 (雖然我也沒有完全遵守。)

## Import
Import 是 Python 中無法避免一定要用到的功能，作用是從別份 .py 檔引入程式碼。由於 import 牽涉到相對路徑、絕對路徑等問題，這邊先從路徑開始解釋。

### 工作路徑
首先解釋什麼是 __工作路徑__。工作路徑是指當下執行 Python 的那個路徑。假設我的文件夾"C:/Program_files/My_program"長成這樣(前綴的"-"代表一層資料夾)：
```
- farm_iot
-- __init__.py
-- farm_iot.py
-- helper.py
- test
-- __init__.py
-- test.py
app.py
```
用 cmd 或 powershell 移動到 "C:/Program_files/My_program"，或是使用 vs code 打開這個文件夾後開啟底下的終端(其實底下的終端也是 cmd 或 powershell)，就可以執行以下指令：
``` cmd
cd C:/Program_files/My_program
python app.py
```
之所以可以執行，是因為 `app.py` 就在你當前所在的資料夾下，所以作業系統找的到它。在執行 `app.py` 時，"C:/Program_files/My_program" 就是當前的 __工作路徑__。

### 絕對路徑與相對路徑
回到 import。Python 在 import 時，會尋找三種文件夾/路徑：
1. Python 自己存放第三方包的文件夾。
2. __絕對路徑__
3. 以 __工作路徑__ 為根目錄的 __相對路徑__。

所謂的絕對路徑就是像 "C:/Program_files/My_program" 這樣的系統看得懂的路徑。非常不推薦使用絕對路徑，多人協作時每個人放程式碼的位置不同，絕對路徑就會出錯。

相對路徑是指以當前的路徑為基處延伸的路徑。比如如果在 `app.py` 中這樣寫：
```python
# app.py
from farm_iot.farm_iot import something
```
Python 就會知道要從 {工作路徑}/farm_iot/farm_iot.py 中 import something。

現在把目光放到 `farm_iot` 文件夾上。如果 `farm_iot.py` 需要從 `helper.py` import 一些東西的時候該怎麼寫？直覺上因為兩者在同一個文件夾，所以這樣寫：
```python
# farm_iot.py
# 因為在同個文件夾，所以從當前文件夾引入
# Helper 是 helper.py 定義的某個 Class
from .helper import Helper
```
__這是錯的__。在專案中，通常只會有一個主程式會被執行，在範例中就是`app.py`。因此 Python 在解析相對路徑時，會以`app.py`所在的路徑當作根目錄。也就是說`.helper`會被 Python 讀作 "C:/Program_files/My_program" 底下的 `helper.py` 文件，但因為找不到它，程式就會出錯。想必正確的作法已經呼之欲出了：
```python
# farm_iot.py
from farm_iot.helper import Helper
```
同理，在`test.py`中，如果要測試`farm_iot.py`裡定義的 Class，需要這樣引入：
```python
# test.py
from farm_iot.farm_iot import MyClass
```
試試看在cmd執行以下命令：
```cmd
cd C:/Program_files/My_program/test
python test.py
```
__這樣也會跳錯__，因為這樣相當於把 "C:/Program_files/My_program/test" 當作工作路徑，`farm_iot.farm_iot` 就變成了不存在的 "C:/Program_files/My_program/test/farm_iot/farm_iot.py"。為此，你需要這樣呼叫 Python：
```cmd
cd C:/Program_files/My_program
python -m test.test
```
`-m` 的作用是告訴 Python 以當前的目錄為工作目錄，執行 test.test 這個 module。

### Module 和 Package
解釋一下什麼是 __module__ 跟 __package__ 。對 Python 而言，__module__ 相當於一個可以從裡面 import 東西的 .py 檔， __package__ 則是集合了多個 modules 的文件夾。一般來說我們會把功能相近的 classes, functions 放在同個 module 中，功能相近的 modules 放在同個 package 中。

只有被放在 package 中的 .py 檔會被認為是 module，而只有含有 `__init__.py` 這個文件的文件夾才會被 Python 認為是 package。就像上面示範的目錄：
```
- farm_iot
-- __init__.py
-- farm_iot.py
-- helper.py
- test
-- __init__.py
-- test.py
app.py
```
farm_iot 和 test 因為底下都有 `__init__.py`，所以都被認作是 package。如果缺少這個文件，`app.py` 就沒辦法順利的 import 它們底下的 modules。

## 虛擬環境
所謂 Python 的執行環境，是指執行 python 程式時的 python 版本，以及它安裝的第三方包。

使用 Python 時很容易遇到執行環境不同的問題：兩支不同人寫的程式使用的 Python 版本可能不同、第三方包可能不同、第三方包的版本可能不同。如果系統上只安裝一種 Python，就有可能導致其中一支程式執行不起來；安裝兩種又會有系統衝突。因此在使用別人的程式前，要先看清楚它使用哪個版本的 Python 以及它的依賴，也就是需要安裝哪些第三方包。

### 版本管理 Pyenv
Python 有一個很好用的版本管理器叫 `pyenv`，用於安裝多個版本的 Python 並在特定文件夾下指定要使用哪個版本。使用步驟如下：
1. 先安裝隨便一個版本的 Python。
2. cmd 執行 "pip install pyenv" 指令。
3. 如果想下載特定版本的 Python，執行 "pyenv install 3.x.x"指令。
4. cmd 移動到你想要的目錄下，執行 "pyenv local 3.x.x" 指定當前目錄使用的版本。
5. 在 vs code 中按 "ctrl + p"，輸入 "Python: select interpreter"，從列出來的 Python 版本選擇你想要的版本。

完成後可以在 vs code 中隨便打開一個 .py 檔，vs code 的右下角會顯示當前 Python 的版本用於確認。

### 虛擬環境 Venv
再來是第三方包的管理。我推薦使用 Python 自帶的 `venv`，使用步驟如下：
1. 確認當前 Python 版本正確
2. 在 cmd 或 vs code terminal 中(推薦使用後者)執行 `python -m venv myenv` 指令。 `myenv` 可以是任意你想取的名字。執行成功的話當前文件夾下會多出一個 `myenv` 文件夾，裡面是虛擬環境所需的文件。
3. 如果你是用 vs code 工作並且步驟 2 是在 cmd 中執行的話，可以像上面的步驟 5 一樣選擇到虛擬環境的 Python 版本。
4. 如果步驟 2 是在 vs code 中執行的話，建立完虛擬環境後 vs code 會說它檢測到新的環境，是否要使用，選擇是。
5. 如果你不使用vs code，記得執行 Python 前要執行 `./myenv/Scripts/activate`指令(在 Windows 環境下使用 powershell 執行。可能會遇到權限問題，可以把問題複製到 google 查詢)。

進入虛擬環境後，使用 pip 安裝的第三方包會被放在虛擬環境的文件夾中，虛擬環境外的 Python 不會被這些東西影響。

通常別人寫的程式會提供 `requirements.txt` 文件列出需要安裝的第三方包。我們只需要在進入虛擬環境後執行 `pip install -r requirements.txt` 就可以一次性把所有依賴都安裝好。

如果有之後有新增依賴，可以使用 `pip freeze > requirements.txt` 生成或更新 `requirements.txt`。

## Object-oriented programming
物件導向是很多程式語言的基礎特性，網路上有很多教學可以查。這邊僅僅是粗淺的解釋。

### 類別 Class
先解釋什麼是Class。Class 是一種將許多變數和許多函式包裝在一起的方式。想像你要建立一個描述一台車的 Class。你可能可以寫出以下的樣子：
```python
# In python
# 定義 Class 的名稱
Class Car():

    # 定義 Constructor
    def __init__(self, logo: str. color: str, seats_number: int):
        # 定義 Attributes
        self.logo = ""
        self.color = ""
        self.seats_number = None
    
    # 定義 Method
    def run(self):
        print("The car is running.")

# 實例化出物件 car_A
car_A = Car("toyota", "Black", 5)
car_A.run() # print "The car is running"
print(car_A.logo) # print "toyota"
```
專有名詞解釋如下：
* Constructor: 在實例化時呼叫的函式。`car_A = Car("toyota", "Black", 5)` 實際上是呼叫了 Constructor 建立了一個 __物件__ 並賦值給了變數 `car_A`。\
在 Python 中一個 Class 只能有一種 Constructor，在其他語言則未必。
* 物件 Instance/ Object: 把 Class 想像成一本說明書，物件就是照著說明書做出來的成品。
* Attributes: 跟物件綁定的變數。如果新建立了一個 `car_B = Car("honda", "White", 5)`，則 `print(car_b.logo)` 的結果會是 `Honda`。
* Method: 只有該 Class 的物件能夠使用的函式。
* self: 在 Class 內部呼叫 method 或是 attribute 時用於告訴 Python 現在是在呼叫物件裡定義的屬性。可以看到在定義 `run(self)` 時裡面有一個 self 參數，但呼叫時`car_A.run()`卻沒有傳入。其實 self 是指 `car_A`，`car_A` 被當作第一個參數傳入 `run(self)` 裡了。

### 繼承 Inheritance
接下來解釋物件導向最重要的一個概念，繼承 Inheritance。

我們已經定義了 Car ，但我們知道車子又很多種，例如油車跟電車。這兩種車加速的方式不同，也有一些不同的屬性。這時如果從頭定義兩個 Class 未免有些麻煩，因此我們可以直接讓他們繼承 Car：
```python
# In Python

# 繼承 Car
Class ElectricCar(Car):

    def __init__(
        self, battery_size, logo, color, seats_number):
        self.battery = battery_size
        # 呼叫父類的 constructor
        # 父類 Parent class -> Car
        super.__init__(logo, color, seats_number)

    # Overloading
    def run(self):
        print("Run by electricity.")
        self.battery = self.battery - 1

    def charge(self):
        self.battery = self.battery + 1


Class FuelCar(Car):

    def __init__(
        self, tank_size, logo, color, seats_number):
        self.tank = tank_size
        super.__init__(logo, color, seats_number)

    def run(self):
        print("Run by oil.")
        self.tank = self.tank - 1

    def refuel(self):
        self.tank = self.tank + 1

car_b = ElectricCar(100, "Tesla", "White", 5)
car_c = FuelCar(230, "Toyota", "Black", 5)

cars = [car_b, car_c]
for car in cars:
    # 因為 run() 是在 Car 中被定義的，因此做為子類別 child class
    # 的ElectricCar 跟 FuelCar 一定有這個 method。
    # 這個特性在編譯前會進行型別檢查的語言十分有用，
    # print "Run by electricity."
    # print "Run by oil."
    car.run()

    # 共有的 attributes 都能被成功呼叫    
    # print "Tesla"
    # print "Toyota"
    print(car.logo)


    # 非共有個 attributes 會報錯
    # print 99
    # Error
    print(car.battery)
```

* Overloading: 定義與父類別中相同名稱的函式。呼叫時只會執行 overloading 的 method 而不是被覆蓋的版本。

以上就是對物件導向的基本介紹，熟悉後請到網路上找更詳細的教程。

## 單元測試
只要是使用物件導向設計出的程式往往是用許多 class 互相配合來達成特定的功能。如果僅僅像是在課堂上那樣全部寫完再一併 debug 的話會因為不清楚究竟是哪個環節出錯導致困難。在設計程式時，一開始就該考慮到哪個 class 的作用是什麼，需要哪些 atttributes 和 methods，測試單一 class 或是 function 是否正常運作就是所謂的單元測試 __unit test__。

__請務必給測試每一個 Class。若改動了以前的 Class 一定要再次執行以前寫的單元測試。__

我使用的是 Python 自己的 unittest 模組。使用方法如下。
```python
# In Python

import unittest

# 需要繼承
class MyTestCase(unittest.TestCase):
    
    # 每個 test 開始之前會呼叫 setUp。
    # 用於初始化要用的變數。
    def setUp(self):
        self.args = (3, 2)

    # 每個 test 結束時會呼叫 tearDown，無論 test 是否通過
    # 用於釋放重要資源，如網路連線等。
    def tearDown(self):
        self.args = None

    # 以 test_ 開頭的 method 都會當作獨立的測試執行。
    def test_something(self):
        # 使用已經定義好的方法進行測試。
        # 如果測試出錯，這個測試會停止執行並呼叫 tearDown。
        self.assertEqual(1 + 1, 2)

        # 有很多這種測試方法，諸如
        self.assertTrue(something.is_alive())
        self.assertFalse(something.is_alive())
        d = {"one": 1}
        with self.assertRaises(KeyError):
            d["two"]

    def test_another_thing(self):
        pass

# 執行測試
if __name__ == "__main__"
    unittest.main()
```
在測試時，盡量使用符合文意的方法。例如 `self.assertTrue(plus(1, 1) == 2)` 雖然可以執行，但如果`plus()`出錯了，`assertTrue`並不會告訴你它回傳了什麼值，而`assertEqual`則會說明清楚。

最後兩行的作用是*當這個.py檔作為主程式執行時才執行這段代碼*。在執行這個 .py 檔時，[考慮到 import 的問題](#import)，應該使用 `python -m {test_name}.py`。

### Asyncio test
如果需要使用 asyncio 的功能(下一章會講解)，需要像以下這樣做：
```python

import unittest

class MQTTClientTestCase(unittest.IsolatedAsyncioTestCase):

    # 和 setUp()相同功能。
    # 在執行 setUp() 後被呼叫。
    async def asyncSetUp(self):
        await self.manager.initialize()

    # 和 tearDown()相同功能。
    # 在執行 tearDown() 後被呼叫。
    async def asyncTearDown(self):
        self.manager.disconnect()

    async def test_something(self):
        self.assertEqual(await plus(1, 1), 2)

# 執行測試
if __name__ == "__main__"
    unittest.main()
```

## Asyncio
Asynchronous IO 是一種電腦進行 IO 的模式。電腦中的 CPU 執行速度比記憶體和硬碟快許多，在 Synchronous 模式下， CPU 會等待讀取完成再執行下一條命令；Asynchronous 模式下則會先執行其他命令，等讀取結束後再回到原本的命令。

### Corountine, task
Python 中的 asyncio 模組正是為此而生。asyncio 允許建立許多 __任務__，當一個任務遇到 IO 時可以釋放執行權，輪到另一個任務執行。用法如下：
```python
# In Python
import asyncio

# 定義需要使用 asyncio 的函式時，需要用 async def 而不是 def
async def func1():
    print("Enter function 1.")
    # 休眠1秒，模擬進行 IO
    # await 代表在這邊放棄執行權
    await asyncio.sleep(1)
    print("Leave function 1.")

async def func2():
    print("Enter function 2.")
    # 在這邊放棄執行權並把 other_async_func() 安排進任務中
    # 等 other_async_func() 回傳後繼續執行
    # 如果 other_async_func() 裡面沒做多少 IO，其實就和一般
    # function相同
    temp = await other_async_func()
    print("Leave function 2.")

async def main():
    # 將 func1, func2 打包到一起等待他們完成
    # 在其中一者遇到 await 時，就會跳去執行另一個任務
    await asyncio.gather(func1(), func2())

# 執行 async 函式，一個應用只能使用一次
# print "Enter function 1."
# print "Enter function 2."
# print "Leave function 1."
# print "Leave function 2."
asyncio.run(main())
```

`async def` 定義出的函式叫做 __coroutine__，呼叫 coroutine 時，一定要在前面加上 `await`，否則會呼叫失敗。 coroutine 可以透過以下方法打包成 __Task__。 Task 有許多好用的 method 可以幫助管理程式。
```python
# In Python

async def func1():
    print("Enter function 1.")
    await asyncio.sleep(5)
    print("Leave function 1.")

async def cancel_in_future(task):
    # 等待一秒後把傳入的 task 取消
    await asyncio.sleep(1)
    task.cancel()

async def main(): 
    # async def 的函式回傳的是 coroutine
    # 因此這裡要傳入的不是 func1, 而是 func1()
    task1 = asyncio.create_task(func1())
    task2 = asyncio.create_task(cancel_in_future(task1))

    # task 一樣可以使用 gather
    # 當 gather 中的 task 或 coroutine 被取消時，其餘的也會被取消
    await asyncio.gather(task1, task2)

# Raise asyncio.CancelError
asyncio.run(main())
```

### Event loop
asyncio 的底層機制是由 `event_loop` 管理各種 coroutines 和 futures。不同作業系統預設的 event_loop 不同，支援的函式也不同。最常遇見的問題是 Windows 預設的 event_loop 無法使用 `event_loop.add_reader()` 。雖然可以調整成支援這個函式的 event_loop，但因為我們會用到的第三方包比較多，怕改了預設會導致第三方包運行不正確，因此在使用時需格外注意。

### Asyncio 與 multithreading
Python 的 asyncio 和 multithreading 通常不相互支援，所以用到使用 multithreading 的第三方包時需要小心使用。如 paho-mqtt 就是使用 multithreading。如果要從其他 thread 溝通取得資料的話，可以使用 `asyncio.Queue` 配合 `asyncio.run_corountine_threadsafe()`：
```python
# In Python

import asyncio
import threading

def job(queue, loop):
    while True:
        asyncio.run_coroutine_threadsafe(
            queue.put_nowait("Some data")
        )

async main():
    # 獲取現在正在執行的 event loop
    # 只能在 coroutine 中呼叫
    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()

    # threading 的用法參閱網路
    t = threading.Thread(target = job, args = (queue, loop))
    t.start()

    while True:
        # queue.get() 會等待到 queue 中有數值了才會回傳。
        # 因此如果 thread t 停止了，這裡的queue.get()就會卡死。
        print(await queue.get())

asyncio.run(main())
```





