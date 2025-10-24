module Data.Response where

import Data.Protocol.Types

data Response
  = Pong
  | Nil
  | RawNilArray
  | OK
  | Error ByteString
  | RawString ByteString
  | RawInteger Integer
  | RawArray [Response]

encodeResponse :: Response -> RedisValue
encodeResponse Pong = SimpleString "PONG"
encodeResponse (Error s) = ErrorString s
encodeResponse Nil = NilString
encodeResponse RawNilArray = NilArray
encodeResponse OK = SimpleString "OK"
encodeResponse (RawString s) = BulkString s
encodeResponse (RawInteger i) = Integer i
encodeResponse (RawArray a) = Array $ encodeResponse <$> a
