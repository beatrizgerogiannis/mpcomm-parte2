# Transport layer for the peer-to-peer UDP multicast: encodes/decodes messages as JSON.
# No reliability logic here (see reliability.py). Messages are positional lists:
#   ["READY", round, sender]
#   ["DATA",  round, sender, seq, payload]           # payload: dict (application command)
#   ["END",   round, sender, total]
#   ["ACK",   round, acker, ackedKind, sender, seq]  # seq = -1 for READY and END
# Tested in tests/test_transport.py.

import json

import netsim

# Same size as the peer's recvfrom buffer: a larger datagram would arrive truncated
MAX_DATAGRAM = 1024

# kind -> number of fields
_FIELDS = {'READY': 3, 'DATA': 5, 'END': 4, 'ACK': 6}
_ACKED_KINDS = ('READY', 'DATA', 'END')


def _is_int(x):
  # bool is a subclass of int in Python, but True/False are not valid numbers here
  return isinstance(x, int) and not isinstance(x, bool)


def validate(msg):
  # Raises ValueError if msg is not a well-formed message (tuple or list)
  if not isinstance(msg, (list, tuple)) or not msg:
    raise ValueError('message must be a non-empty list')
  kind = msg[0]
  if not isinstance(kind, str) or kind not in _FIELDS:
    raise ValueError('unknown message type: %r' % (kind,))
  if len(msg) != _FIELDS[kind]:
    raise ValueError('%s must have %d fields, got %d' % (kind, _FIELDS[kind], len(msg)))
  if kind == 'ACK':
    ints = {'round': msg[1], 'acker': msg[2], 'sender': msg[4], 'seq': msg[5]}
    if msg[3] not in _ACKED_KINDS:
      raise ValueError('invalid ackedKind: %r' % (msg[3],))
  else:
    ints = {'round': msg[1], 'sender': msg[2]}
    if kind == 'DATA':
      ints['seq'] = msg[3]
      if not isinstance(msg[4], dict):
        raise ValueError('DATA payload must be a dict, got %s' % type(msg[4]).__name__)
    elif kind == 'END':
      ints['total'] = msg[3]
  for name, value in ints.items():
    if not _is_int(value):
      raise ValueError('%s %s must be an integer, got %r' % (kind, name, value))


def encode(msg):
  # Message (tuple or list) -> compact UTF-8 JSON bytes; ValueError if invalid or too large
  validate(msg)
  try:
    data = json.dumps(list(msg), separators=(',', ':'), ensure_ascii=False).encode('utf-8')
  except (TypeError, ValueError) as e:   # payload with a non-JSON value (e.g. a set)
    raise ValueError('message is not JSON-serializable: %s' % e)
  if len(data) > MAX_DATAGRAM:
    raise ValueError('encoded message has %d bytes (max %d)' % (len(data), MAX_DATAGRAM))
  return data


def decode(data):
  # Bytes received from the network -> message (list). Raises only ValueError
  try:
    msg = json.loads(data.decode('utf-8'))
  except (UnicodeDecodeError, ValueError, RecursionError) as e:
    raise ValueError('invalid JSON/UTF-8: %s' % e)
  except Exception as e:   # e.g. data is not bytes
    raise ValueError('cannot decode: %s' % e)
  validate(msg)
  return msg


def send(sock, msg, addr):
  # Encode and send through the network simulator
  netsim.send(sock, encode(msg), addr)
