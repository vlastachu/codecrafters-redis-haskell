module Data.Protocol.Types where

data RedisValue
  = SimpleString ByteString
  | ErrorString ByteString
  | BulkString ByteString
  | NilString -- String with length = -1
  | Integer Integer
  | Array [RedisValue]
  | RawNilArray -- Array with length = -1
  deriving (Eq, Show)
