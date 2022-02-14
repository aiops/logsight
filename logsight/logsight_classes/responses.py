from http import HTTPStatus
from pydantic import BaseModel


class Response(BaseModel):
    app_id: str
    message: str
    status: str


class ErrorResponse(Response):
    def __init__(self, app_id: str, message: str, status=HTTPStatus.INTERNAL_SERVER_ERROR):
        Response.__init__(self, app_id=app_id, message=message, status=status)


class SuccessResponse(Response):
    def __init__(self, app_id: str, message: str):
        Response.__init__(self, app_id=app_id, message=message, status=HTTPStatus.OK)
