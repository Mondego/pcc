from collections import OrderedDict
# recursive_dictionary.py
#     Created 2009-05-20 by Jannis Andrija Schnitzer.
#
# Copyright (c) 2009 Jannis Andrija Schnitzer
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

__author__ = 'jannis@itisme.org (Jannis Andrija Schnitzer)(original), ra.rohan@gmail.com (Rohan Achar)'

class OrderedSliceableDict(OrderedDict):
    __author__ = 'ra.rohan@gmail.com (Rohan Achar)'

    def lastkey(self):
        root = self._OrderedDict__root
        try:
            return root[0][2]
        except IndexError:
            return None

    def __getitem__(self, key):
        if isinstance(key, slice):
            map = self._OrderedDict__map
            root = self._OrderedDict__root
            step = key.step if key.step else 1
            reverse = True if step < 0 else False
            if key.start is None and key.stop is None:
                return self.values()[::key.step]
            if len(self) == 0:
                raise KeyError("Cannot slice empty dictionary")
            start = key.start if key.start else root[1 if reverse else 0][2]
            stop = (map[key.stop] if key.stop else root)[1 if reverse else 0][2]
            if start == stop:
                return list()
            items = list()
            curr = map[start]
            while True:
                items.append(self[curr[2]])
                if curr[2] == stop:
                    break
                curr = curr[0] if reverse else curr[1]
                if curr[2] is None:
                    # curr = curr[0] if reverse else curr[1]
                    break
            return items[::abs(step)]
        else:
            return OrderedDict.__getitem__(self, key)



class RecursiveDictionary(OrderedSliceableDict):
    """RecursiveDictionary provides the methods rec_update and iter_rec_update
    that can be used to update member dictionaries rather than overwriting
    them."""

    __author__ = 'jannis@itisme.org (Jannis Andrija Schnitzer)(original)'

    def rec_update(self, other, **third):
        """Recursively update the dictionary with the contents of other and
        third like dict.update() does - but don't overwrite sub-dictionaries.
                
        Example:
        >>> d = RecursiveDictionary({'foo': {'bar': 42}})
        >>> d.rec_update({'foo': {'baz': 36}})
        >>> d
        {'foo': {'baz': 36, 'bar': 42}}
        """
        try:
            iterator = other.iteritems()
        except AttributeError:
            iterator = other
        self.iter_rec_update(iterator)
        self.iter_rec_update(third.iteritems())
                
    def iter_rec_update(self, iterator):
        for (key, value) in iterator:
            if key in self: 
                if isinstance(self[key], RecursiveDictionary) and isinstance(value, RecursiveDictionary):
                    self[key].rec_update(value)
                elif isinstance(self[key], dict) and isinstance(value, dict):
                    self[key] = RecursiveDictionary(self[key])
                    self[key].rec_update(value)
                elif isinstance(self[key], list) and isinstance(value, list):
                    self[key].extend(value)
                elif isinstance(self[key], set) and isinstance(value, set):
                    self[key].update(value)
                elif hasattr(self[key], "__dict__") and hasattr(value, "__dict__"):
                    self[key].__dict__ = RecursiveDictionary(self[key].__dict__)
                    self[key].__dict__.rec_update(value.__dict__)
                else:
                    self[key] = value
            else:
                self[key] = value
        
    def CopyFrom(self, other_dict):
        self.rec_update(other_dict)