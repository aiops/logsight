import dataclasses
import json
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import TypeVar


class InputControlOperations(Enum):
    FLUSH = "FLUSH"


@dataclass
class ControlRequest:
    id: str
    orderCounter: int
    logsCount: int
    operation: InputControlOperations


@dataclass
class ControlReply:
    id: str
    orderCounter: int
    logsCount: int
    currentLogsCount: int
    description: str
    status: int


# Register as type var to allow save instantiation through functions
TControlReply = TypeVar("TControlReply", bound=ControlReply)


@dataclass
class ControlReplySuccess(ControlReply):
    status: int = HTTPStatus.OK


@dataclass
class ControlReplyTimeout(ControlReply):
    status: int = HTTPStatus.REQUEST_TIMEOUT


@dataclass
class ControlReplyFail(ControlReply):
    status: int = HTTPStatus.INTERNAL_SERVER_ERROR


@dataclass
class ControlReplyValidationFail(ControlReply):
    id: str = ""
    orderCounter: int = -1
    logsCount: int = -1
    currentLogsCount: int = -1
    description: str = ""
    status: int = HTTPStatus.BAD_REQUEST
