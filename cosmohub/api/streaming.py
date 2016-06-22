import os
import werkzeug.exceptions as http_exc

from werkzeug.http import parse_range_header

def ContentRange(self, range_header, length):
    if not range_header:
        return
    
    range_ = parse_range_header(range_header)
    
    if not range_:
        raise http_exc.NotAcceptable("Cannot parse 'Range' header.")
    
    if not range_.range_for_length(length):
        raise http_exc.NotAcceptable("Cannot satisfy requested range.")
    
    return range_.make_content_range(length)

class HDFSPathStreamer(object):
    def __init__(self, client, path, chunk_size=512*1024, prefix=''):
        self._client = client
        self._path = path
        self._chunk_size = chunk_size
        self._prefix = prefix
        
        self._iter = None
        self._size = len(self._prefix)
        
        self._files = []
        
        status = client.status(path)
        if status['type'] == 'FILE':
            self._files.append({
                'name'   : os.path.basename(path),
                'size'   : status['length'],
                'offset' : 0,
            })
            self._size += status['length']
        else:
            for name, status in client.list(path, status=True):
                if status['type'] != 'FILE':
                    continue
                
                self._files.append({
                    'name'   : name,
                    'size'   : status['length'],
                    'offset' : 0,
                })
                self._size += status['length']
    
    def __getitem__(self, key):
        if not isinstance(key, slice):
            raise TypeError("slice object was expected.")
        
        if min(key.start, key.stop) < 0:
            raise IndexError("indexes must be positive")
        
        if key.start > key.stop:
            raise IndexError("start index must be less or equal than stop index.")
        
        if key.step:
            raise NotImplementedError("step slicing is not supported.")
        
        new_files = []
        new_size = 0
        new_prefix = self._prefix
        
        skip = key.start
        remaining = key.stop - key.start
        
        if len(self._prefix) <= skip:
            new_prefix = ''
            skip -= len(self._prefix)
        else:
            new_prefix = self._prefix[skip:skip+remaining]
            skip = 0
            remaining -= len(new_prefix)
        
        new_size += len(new_prefix)
        
        for entry in self._files:
            if entry['size'] <= skip:
                skip -= entry['size']
                continue
            
            if not remaining:
                break
            
            if skip:
                entry['offset'] += skip
                entry['size'] -= skip
                skip = 0
            
            if remaining < entry['size']:
                entry['size'] = remaining
            
            new_files.append(entry)
            
            remaining -= entry['size']
            new_size += entry['size']
        
        if remaining:
            raise IndexError("Requested range cannot be satisfied")
        
        self._prefix = new_prefix
        self._files = new_files
        self._size = new_size
        
        return self
    
    def __iter__(self):
        if self._prefix:
            yield self._prefix
        
        for entry in self._files:
            file_path = os.path.join(self._path, entry['name'])
            remaining = entry['size']
            
            with self._client.read(file_path, chunk_size=self._chunk_size, offset=entry['offset']) as reader:
                for chunk in reader:
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                    
                    if chunk:
                        yield chunk
                        remaining -= len(chunk)
                    
                    if not remaining:
                        break
            
            assert remaining == 0
    
    def __len__(self):
        return self._size
