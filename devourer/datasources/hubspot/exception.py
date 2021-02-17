from devourer.core.exception import DevourerException


class HubSpotException(DevourerException):
    pass


class HubSpotDatetimeFormatParseException(HubSpotException):
    pass
