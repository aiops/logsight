from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import TypeVar, Optional

from connectors.sources import Source


class InputControlOperations(Enum):
    FLUSH = "FLUSH"


@dataclass
class ControlRequest:
    id: str
    orderNum: int
    logsCount: int
    operation: InputControlOperations


@dataclass
class FlushReply:
    id: str
    orderNum: int
    logsCount: int
    currentLogsCount: int
    description: str
    status: str


# Register as type var to allow save instantiation through functions
TFlushReply = TypeVar("TFlushReply", bound=FlushReply)


@dataclass
class FlushReplySuccess(FlushReply):
    status: str = HTTPStatus.OK


@dataclass
class FlushReplyTimeout(FlushReply):
    status: str = HTTPStatus.REQUEST_TIMEOUT


@dataclass
class FlushReplyFail(FlushReply):
    status: str = HTTPStatus.INTERNAL_SERVER_ERROR


@dataclass
class FlushReplyValidationError(FlushReply):
    id: str = ""
    orderNum: int = -1
    logsCount: int = -1
    currentLogsCount: int = -1
    description: str = ""
    status: str = HTTPStatus.BAD_REQUEST
