import datetime
from http import HTTPStatus
from pydantic import BaseModel


class Response(BaseModel):
    timestamp: datetime.datetime = datetime.datetime.now().isoformat()
    message: str
    status: HTTPStatus


class ErrorResponse(Response):
    def __init__(self, message, status = HTTPStatus.INTERNAL_SERVER_ERROR):
        super(ErrorResponse, self).__init__(message=message, status=status)


class SuccessResponse(Response):
    def __init__(self, message):
        super(SuccessResponse, self).__init__(message=message, status=HTTPStatus.OK)
