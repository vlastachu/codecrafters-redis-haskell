module Data.Request where

import qualified Data.ByteString.Char8 as BS
import Data.Char (toUpper)
import Data.Protocol.Types

data Request
  = Ping
  | Echo BS.ByteString
  | Get BS.ByteString
  | Set BS.ByteString BS.ByteString (Maybe Int)
  deriving (Show, Eq)

bsUpper :: ByteString -> ByteString
bsUpper = BS.map toUpper

decodeRequest :: RedisValue -> Maybe Request
decodeRequest (Array (Just (BulkString (Just cmd) : args))) = decodeInner (bsUpper cmd) args
decodeRequest _ = Nothing

decodeInner :: BS.ByteString -> [RedisValue] -> Maybe Request
decodeInner "PING" [] = Just Ping
decodeInner "ECHO" [BulkString (Just msg)] = Just (Echo msg)
decodeInner "GET" [BulkString (Just key)] = Just (Get key)
decodeInner "SET" (BulkString (Just key) : BulkString (Just val) : rest) =
  case parseExpiration rest of
    Nothing -> if null rest then Just (Set key val Nothing) else fail "Unrecognized Set args"
    justExp -> Just (Set key val justExp)
decodeInner _ _ = Nothing

-- Парсер expiration
parseExpiration :: [RedisValue] -> Maybe Int
parseExpiration [BulkString (Just cmd), BulkString (Just numBS)]
  | BS.map toUpper cmd == "EX",
    [(n, "")] <- reads (BS.unpack numBS) =
      Just (n * 1000)
  | BS.map toUpper cmd == "PX",
    [(n, "")] <- reads (BS.unpack numBS) =
      Just n
parseExpiration _ = Nothing