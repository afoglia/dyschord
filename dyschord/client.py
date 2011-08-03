import xmlrpclib

class NodeProxy(object) :
  def __init__(self, url, id=None, verbose=False) :
    # Should parse the URL to makes sure it's http, or if not, add the protocol
    self.server = xmlrpclib.ServerProxy(url, verbose=verbose)
    self.__id = id

  @property
  def id(self) :
    if self.__id is not None :
      return self.__id
    else :
      ping = self.server.ping()
      self.__id = int(ping["id"])
    
  def close() :
    self.server("close")()

    
  def _iternodes() :
    pass
    

  def __getattr__(self, attr) :
    # Maybe it's a method on the server...
    return getattr(self.server, attr)
