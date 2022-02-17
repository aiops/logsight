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

    @property
    def operation(self) -> Optional[InputControlOperations]:
        return None

    @operation.setter
    def operation(self, op: str) -> None:
        self.operation = InputControlOperations(op)


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
    status: str = HTTPStatus(HTTPStatus.OK).phrase


@dataclass
class FlushReplyTimeout(FlushReply):
    status: str = HTTPStatus(HTTPStatus.REQUEST_TIMEOUT).phrase


@dataclass
class FlushReplyFail(FlushReply):
    status: str = HTTPStatus(HTTPStatus.INTERNAL_SERVER_ERROR).phrase


@dataclass
class FlushReplyValidationError(FlushReply):
    id: str = ""
    orderNum: int = -1
    logsCount: int = -1
    currentLogsCount: int = -1
    description: str = ""
    status: str = HTTPStatus(HTTPStatus.BAD_REQUEST).phrase
