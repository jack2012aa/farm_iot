"""Define some common filters."""

from datetime import datetime

from pandas import DataFrame

from general import type_check
from base.manage import Manager
from base.pipeline import Filter, Pipeline
from base.export.common_exporters import ExporterFactory


class StdFilter(Filter):
    """Compute the standard deviation of data and replace out-of-range data to average +- one std."""

    def __init__(self) -> None:
        """Compute the standard deviation of data and replace out-of-range data to average +- one std."""
        super().__init__()

    def __str__(self) -> str:
        return "StdFilter. Range: [-avg + std, +avg + std]"

    async def process(self, data: DataFrame) -> DataFrame:
        """Compute the standard deviation of data and replace out-of-range data to average +- one std."""

        type_check(data, "data", DataFrame)
        standard_deviation = data.std()
        average = data.mean()

        for i in range(1, data.shape[1]):
            # Define a function here to get std, avg and i.
            def threshold(value):
                value = min(value, average.iloc[i] + standard_deviation.iloc[i])
                value = max(value, average.iloc[i] - standard_deviation.iloc[i])
                return value
            data.iloc[:, i] = data.iloc[:, i].map(threshold)

        return data


class BatchAverageFilter(Filter):
    """Compute and return the average of a batch. `Datetime` will be set 
    as the last timestamp in original data. """

    def __init__(self) -> None:
        """Compute and return the average of a batch. `Datetime` will be 
        set as the last timestamp in original data. """
        super().__init__()

    def __str__(self) -> str:
        return "BatchAverageFilter."

    async def process(self, data: DataFrame) -> DataFrame:
        """Compute and return the average of a batch. `Datetime` will be set 
        as the last timestamp in original data. """

        type_check(data, "data", DataFrame)
        average = data.mean()
        average.iloc[0] = data.iloc[data.shape[0] - 1, 0]
        df = average.to_frame().T
        # Change dtype back.
        df[df.columns[0]] = df[df.columns[0]].astype(data[data.columns[0]].dtype)
        
        return df
    

class TimeFilter(Filter):
    """Transform seperate time info in the DataFrame into datetime."""

    def __init__(self, data_name: list[str], new_data_name: list[str] = None) -> None:
        """Transform seperate time info in the DataFrame into datetime.
        
        :param data_name: list of data name that is not related to time in the df.
        :param new_data_name: if you want to change the name of data in data_name.
        """
        self.data_name = data_name
        self.new_data_name = new_data_name
        super().__init__()

    async def process(self, data: DataFrame) -> DataFrame:

        new_data = {"Timestamp":[]}
        if self.new_data_name is not None:
            for name in self.new_data_name:
                new_data[name] = []
        else:
            for name in self.data_name:
                new_data[name] = []

        for _, row in data.iterrows():
            dt = datetime(
                year=int(float(str(row.get("year")))),
                month=int(float(str(row.get("month")))),
                day=int(float(str(row.get("day")))),
                hour=int(float(str(row.get("hour")))),
                minute=int(float(str(row.get("minute")))),
                second=int(float(str(row.get("second")))),
                microsecond=int(float(str(row.get("millisecond"))))
            )
            new_data["Timestamp"].append(dt)
            if self.new_data_name is not None:
                for old_name, new_name in list(zip(self.data_name, self.new_data_name)):
                    new_data[new_name].append(row.get(old_name))
            else:
                for name in self.data_name:
                    new_data[name].append(row.get(name))

        df = DataFrame(new_data)
        await self.notify_exporters(df)
        return df


class PipelineFactory():
    """A factory class to build pipeline using dictionary."""

    def __init__(self) -> None:
        pass

    def create(self, settings: list, manager: Manager = None) -> Pipeline:
        """Create a pipeline using the settings. 

        Settings should look like this:

        [
            {
                #Type and filter specific settings.
                "type": StdFilter, 
                "exporters":[exp1: dict, exp2: dict, ...]
            }
        ]

        Please refer to example json for detail.

        :param settings: settings of the pipeline, including filters, exporters, etc.
        :param manager: exporters' manager.
        :raise: ValueError, KeyError.
        """

        type_check(settings, "settings", list)

        pipeline = Pipeline()
        for filter_setting in settings:

            filter = None
            #Remember to add new filter type to here in the future.
            try:
                match filter_setting["type"]:
                    case "StdFilter":
                        filter = StdFilter()
                    case "BatchAverageFilter":
                        filter = BatchAverageFilter()
                    case _:
                        print(f"Filter type {filter_setting["type"]} does not exist.")
                        raise ValueError
            #Required specific setting not found.
            except KeyError as ex:
                print(f"Filter type {filter_setting["type"]} misses setting \"{ex.args[0]}\"")

            #Construct exporters.
            exporter_factory = ExporterFactory()
            exporters_settings = filter_setting.get("exporters")
            #Avoid nesting.
            if exporters_settings is None:
                exporters_settings = []
            for exporter in exporters_settings:
                new_exporter = exporter_factory.create(exporter)
                new_exporter.set_manager(manager)
                filter.add_exporter(new_exporter)
            pipeline.add_filter(filter)

        return pipeline