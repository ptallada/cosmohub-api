import json
import numpy as np
import pandas as pd

class WSEncoder(json.JSONEncoder):
    def default(self, obj):
        """If input object is an ndarray it will be converted into a dict 
        holding dtype, shape and the data, base64 encoded.
        """
        if isinstance(obj, np.bool_): # @UndefinedVariable
            return bool(obj)
        
        if isinstance(obj, pd.Series):
            return obj.tolist()
        
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)