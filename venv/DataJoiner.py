import pandas as pd

class DataJoiner:

    # region Fields
    flightInfo = None
    groundInfo = None
    # endregion Fields

    # region Constructor
    def __init__(self, flightInfo, groundInfo):
        if not isinstance(flightInfo, pd.DataFrame) or not isinstance(groundInfo, pd.DataFrame):
            raise Exception('Expected pandas data frame.')

        self.flightInfo = flightInfo
        self.groundInfo = groundInfo
    # endregion Constructor

    # region Methods
    def JoinData(self):
        logging.debug('Starting to join the flight and ground info.')
    # endregion Methods