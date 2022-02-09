import datetime
from http import HTTPStatus
from pydantic import BaseModel


class Response(BaseModel):
    id: str
    message: str
    status: HTTPStatus


class ErrorResponse(Response):
    def __init__(self, id, message, status = HTTPStatus.INTERNAL_SERVER_ERROR):
        super(ErrorResponse, self).__init__(id=id, message=message, status=status)


class SuccessResponse(Response):
    def __init__(self, id, message):
        super(SuccessResponse, self).__init__(id=id, message=message, status=HTTPStatus.OK)
