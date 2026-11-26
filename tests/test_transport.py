import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Allow importing transport.py and reliability.py from the repository root
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import transport
from transport import encode, decode, validate, MAX_DATAGRAM
from reliability import pending_key, make_ack, key_from_ack

CMD = {"op": "DEPOSITAR", "conta": "A", "valor": 10000}


class TestRoundTrip(unittest.TestCase):

	def test_four_kinds(self):
		msgs = [
			('READY', 3, 1),
			('DATA', 3, 1, 7, {}),
			('END', 3, 1, 20),
			('ACK', 3, 2, 'DATA', 1, 7),
			('ACK', 3, 2, 'READY', 1, -1),
		]
		for msg in msgs:
			with self.subTest(msg=msg):
				out = decode(encode(msg))
				self.assertIsInstance(out, list)
				self.assertEqual(out, list(msg))

	def test_list_input_accepted(self):
		self.assertEqual(decode(encode(['END', 1, 0, 5])), ['END', 1, 0, 5])

	def test_data_payload_preserved(self):
		out = decode(encode(('DATA', 1, 0, 0, CMD)))
		self.assertEqual(out[4], CMD)

	def test_compact_utf8(self):
		data = encode(('DATA', 1, 0, 0, {"conta": "Ç"}))
		self.assertEqual(data, '["DATA",1,0,0,{"conta":"Ç"}]'.encode('utf-8'))


class TestDecodeInvalid(unittest.TestCase):

	def assertInvalid(self, data):
		with self.assertRaises(ValueError):
			decode(data)

	def test_not_utf8(self):
		self.assertInvalid(b'\xff\xfe\x00')

	def test_invalid_json(self):
		self.assertInvalid(b'lixo')
		self.assertInvalid(b'["READY", 1,')

	def test_json_not_list(self):
		self.assertInvalid(b'{"op": "READY"}')
		self.assertInvalid(b'42')
		self.assertInvalid(b'[]')

	def test_unknown_kind(self):
		self.assertInvalid(b'["HELLO",1,0]')
		self.assertInvalid(b'[1,1,0]')
		self.assertInvalid(b'[["READY"],1,0]')

	def test_wrong_field_count(self):
		self.assertInvalid(b'["READY",1]')
		self.assertInvalid(b'["DATA",1,0,0]')       # old 4-field DATA (no payload)
		self.assertInvalid(b'["END",1,0,5,6]')
		self.assertInvalid(b'["ACK",1,0,"DATA",1]')

	def test_int_field_with_string(self):
		self.assertInvalid(b'["READY","1",0]')
		self.assertInvalid(b'["DATA",1,0,"0",{}]')
		self.assertInvalid(b'["END",1,0,"5"]')
		self.assertInvalid(b'["ACK",1,"2","DATA",1,0]')

	def test_int_field_with_bool(self):
		self.assertInvalid(b'["READY",true,0]')
		self.assertInvalid(b'["DATA",1,0,false,{}]')
		self.assertInvalid(b'["ACK",1,2,"DATA",1,true]')

	def test_int_field_with_float(self):
		self.assertInvalid(b'["END",1,0,5.0]')

	def test_payload_not_dict(self):
		self.assertInvalid(b'["DATA",1,0,0,[]]')
		self.assertInvalid(b'["DATA",1,0,0,null]')
		self.assertInvalid(b'["DATA",1,0,0,"x"]')

	def test_invalid_acked_kind(self):
		self.assertInvalid(b'["ACK",1,2,"ACK",1,0]')
		self.assertInvalid(b'["ACK",1,2,"data",1,0]')
		self.assertInvalid(b'["ACK",1,2,null,1,0]')

	def test_not_bytes(self):
		self.assertInvalid(None)


class TestEncodeInvalid(unittest.TestCase):

	def test_too_large(self):
		msg = ('DATA', 1, 0, 0, {"x": "a" * MAX_DATAGRAM})
		with self.assertRaises(ValueError):
			encode(msg)

	def test_exactly_max_ok(self):
		base = len(encode(('DATA', 1, 0, 0, {"x": ""})))
		data = encode(('DATA', 1, 0, 0, {"x": "a" * (MAX_DATAGRAM - base)}))
		self.assertEqual(len(data), MAX_DATAGRAM)

	def test_invalid_message(self):
		for msg in [('DATA', 1, 0, 0), ('READY', 1, True), 'READY', None, ('NOPE', 1, 0)]:
			with self.subTest(msg=msg):
				with self.assertRaises(ValueError):
					encode(msg)

	def test_payload_not_json(self):
		with self.assertRaises(ValueError):
			encode(('DATA', 1, 0, 0, {"x": {1, 2}}))


class TestSend(unittest.TestCase):

	def test_send_uses_netsim(self):
		sock = MagicMock()
		with patch('transport.netsim.send') as ns:
			transport.send(sock, ('READY', 1, 0), ('10.0.0.1', 6789))
		ns.assert_called_once_with(sock, b'["READY",1,0]', ('10.0.0.1', 6789))

	def test_send_invalid_raises_before_sending(self):
		with patch('transport.netsim.send') as ns:
			with self.assertRaises(ValueError):
				transport.send(MagicMock(), ('READY', 1), ('10.0.0.1', 6789))
		ns.assert_not_called()


class TestReliabilityIntegration(unittest.TestCase):

	def test_pending_key_matches_ack_after_wire(self):
		dest = '10.0.0.2'
		for data_msg in [('DATA', 4, 1, 9, CMD), ('READY', 4, 1), ('END', 4, 1, 10)]:
			with self.subTest(msg=data_msg):
				received = decode(encode(data_msg))   # what the destination sees
				ack = decode(encode(make_ack(received, 2)))   # what the sender gets back
				self.assertEqual(pending_key(dest, decode(encode(data_msg))),
								 key_from_ack(ack, dest))
				# and the key computed on the sending side (tuple) is the same
				self.assertEqual(pending_key(dest, data_msg), key_from_ack(ack, dest))


if __name__ == '__main__':
	unittest.main()
