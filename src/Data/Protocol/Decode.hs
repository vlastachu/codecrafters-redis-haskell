module Data.Protocol.Decode (decode, parseRedisValue) where

import Data.Attoparsec.ByteString.Char8 as C8
import Data.Protocol.Types
import qualified Data.Text as T
import Prelude hiding (take)

isEndOfLine_ :: Char -> Bool
isEndOfLine_ w = w == '\n' || w == '\r'

parseRedisValue :: Parser RedisValue
parseRedisValue = do
  t <- anyChar
  case t of
    '+' -> SimpleString <$> simpleString
    '-' -> ErrorString <$> simpleString
    ':' -> Integer <$> integerValue
    '$' -> bulkString
    '*' -> arrayValue
    _ -> fail $ "Unknown RESP type: " ++ [t]

-- +OK\r\n
simpleString :: Parser ByteString
simpleString = takeTill isEndOfLine_ <* endOfLine

-- :123\r\n
integerValue :: Parser Integer
integerValue = signed decimal <* endOfLine

-- $6\r\nfoobar\r\n  |  $-1\r\n

bulkString :: Parser RedisValue
bulkString = do
  len <- signed decimal <* endOfLine
  if len == (-1)
    then pure NilString
    else do
      bs <- take len <* endOfLine
      pure $ BulkString bs

-- * 2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n  |  *-1\r\n

arrayValue :: Parser RedisValue
arrayValue = do
  len <- signed decimal <* endOfLine
  if len == (-1)
    then pure NilArray
    else do
      values <- count len parseRedisValue
      pure $ Array values

-------------------------------------------------------
-- Обёртка вокруг parseOnly
-------------------------------------------------------

decode :: ByteString -> Either Text RedisValue
decode = first T.pack . parseOnly parseRedisValue
