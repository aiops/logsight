from abc import abstractmethod, ABC


class Builder(ABC):
    @abstractmethod
    def build_object(self, object_config, app_settings):
        raise NotImplementedError
