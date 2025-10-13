module Data.Protocol.Types where

data RedisValue
  = SimpleString ByteString
  | ErrorString ByteString
  | BulkString (Maybe ByteString) -- Nothing = nil
  | Integer Integer
  | Array (Maybe [RedisValue]) -- Nothing = nil
  deriving (Eq, Show)
