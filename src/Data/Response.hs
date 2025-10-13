module Data.Response where

import Data.Protocol.Types

data Response
  = Pong
  | Nil
  | OK
  | Error ByteString
  | RawString (Maybe ByteString)

encodeResponse :: Response -> RedisValue
encodeResponse Pong = SimpleString "PONG"
encodeResponse (Error s) = ErrorString s
encodeResponse Nil = BulkString Nothing
encodeResponse OK = SimpleString "OK"
encodeResponse (RawString s) = BulkString s