"""Define classes to manage works done in different hierarchies."""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from general import type_check


@dataclass
class Report:
    """A dataclass help `Worker` objects communicate with `Manager` objects.
    
    :param sign: sign of the reporting object.
    :param content: the report content.
    """
    
    sign: object = None
    content: object = None


class Manager(ABC):

    def __init__(self) -> None:
        """A class which manages `Worker` objects and handles there report."""
        self.workers: set[Worker] = set()

    @abstractmethod
    def handle(self, report: Report) -> None:
        return NotImplemented


class Worker(ABC):

    def __init__(self) -> None:
        """A class which do works and report to a manager."""
        self.__manager: Manager = None

    def set_manager(self, manager: Manager) -> None:
        """Set the manager of the worker.
        
        :param manager: manager to report when something happens.
        """
        type_check(manager, "manager", Manager)
        self.__manager = manager

    def notify_manager(self, report: Report) -> None:
        """Report to manager.
        
        :param report: things to report.
        """
        self.__manager.handle(report)