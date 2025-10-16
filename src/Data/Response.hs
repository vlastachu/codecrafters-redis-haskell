module Data.Response where

import Data.Protocol.Types
import GHC.Integer (Integer)

data Response
  = Pong
  | Nil
  | OK
  | Error ByteString
  | RawString ByteString
  | RawInteger Integer

encodeResponse :: Response -> RedisValue
encodeResponse Pong = SimpleString "PONG"
encodeResponse (Error s) = ErrorString s
encodeResponse Nil = NilString
encodeResponse OK = SimpleString "OK"
encodeResponse (RawString s) = BulkString s
encodeResponse (RawInteger i) = Integer i