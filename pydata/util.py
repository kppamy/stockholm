from pyalgotrade.dataseries import SequenceDataSeries
from pyalgotrade.technical import cross
CROSSABOVE = 'above'
CROSSBELOW = 'below'

def toSeqDataSeries(seris)
    '''
    @param: seris- pandas Series
     return : pyalgotrade SequenceDataSeries
    '''
    ds=dataseries.SequenceDataSeries()
    return ds.append(seris)

def cross(seris,window)
    '''
    @param: seris- pandas Series
    @param: window- target move average window
     return : True /False
    '''
    close = toSeqDataSeries(seris)
    target = toSeqDataSeries(seris.rolling(window).mean())
    if cross.cross_below(close,target) :
        return CROSSBELOW
    elif  cross.cross_above(close,target)
        return CROSSABOVE
    else :
        return None
