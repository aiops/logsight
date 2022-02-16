from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import TypeVar, Optional


class InputControlOperations(Enum):
    FLUSH = "FLUSH"


@dataclass
class ControlRequest:
    id: str
    orderNum: int
    logsCount: int

    @property
    def operation(self) -> Optional[InputControlOperations]:
        return None

    @operation.setter
    def operation(self, op: str) -> None:
        self.operation = InputControlOperations(op)


@dataclass
class ControlReply:
    id: str
    orderCounter: int
    logsCount: int
    currentLogsCount: int
    description: str
    status: str


# Register as type var to allow save instantiation through functions
TControlReply = TypeVar("TControlReply", bound=ControlReply)


@dataclass
class ControlReplySuccess(ControlReply):
    status: str = HTTPStatus(HTTPStatus.OK).phrase


@dataclass
class ControlReplyTimeout(ControlReply):
    status: str = HTTPStatus(HTTPStatus.REQUEST_TIMEOUT).phrase


@dataclass
class ControlReplyFail(ControlReply):
    status: str = HTTPStatus(HTTPStatus.INTERNAL_SERVER_ERROR).phrase


@dataclass
class ControlReplyValidationFail(ControlReply):
    id: str = ""
    orderCounter: int = -1
    logsCount: int = -1
    currentLogsCount: int = -1
    description: str = ""
    status: str = HTTPStatus(HTTPStatus.BAD_REQUEST).phrase
