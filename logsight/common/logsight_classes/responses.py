from dataclasses import dataclass
from http import HTTPStatus


@dataclass(frozen=True)
class ApplicationOperationResponse:
    app_id: str
    message: str
    status: int


@dataclass(frozen=True)
class ErrorApplicationOperationResponse(ApplicationOperationResponse):
    status: int = HTTPStatus.INTERNAL_SERVER_ERROR


@dataclass(frozen=True)
class SuccessApplicationOperationResponse(ApplicationOperationResponse):
    status: int = HTTPStatus.OK
