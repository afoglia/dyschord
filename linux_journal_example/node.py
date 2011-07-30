
# Clockwise ring function taken from <http://www.linuxjournal.com/article/6797>

def distance(a, b) :
  # k is the number of bits in the ids used.  For a uuid, this is 128
  # bytes.  If this were C, one would need to worry about overflow,
  # because 2**128 takes 129 bytes to store.
  if a < b:
    return b-a
  elif a == b :
    return 0
  else :
    return (2**k) + (b-a)
