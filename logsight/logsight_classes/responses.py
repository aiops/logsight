from http import HTTPStatus
from pydantic import BaseModel


class Response(BaseModel):
    id: str
    message: str
    status: int


class ErrorResponse(Response):
    def __init__(self, id: str, message: str, status=HTTPStatus.INTERNAL_SERVER_ERROR):
        Response.__init__(self, id=id, message=message, status=status)


class SuccessResponse(Response):
    def __init__(self, id: str, message: str):
        Response.__init__(self, id=id, message=message, status=HTTPStatus.OK)
