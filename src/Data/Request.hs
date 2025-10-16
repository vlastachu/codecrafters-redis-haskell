module Data.Request where

import qualified Data.ByteString.Char8 as BS
import Data.Char (toUpper)
import Data.Maybe (Maybe (Nothing))
import Data.Protocol.Types
import GHC.IO (unsafePerformIO)
import GHC.Num (Num (fromInteger))
import GHC.Real (Integral (toInteger))

data Request
  = Ping
  | Echo BS.ByteString
  | Get BS.ByteString
  | Set BS.ByteString BS.ByteString (Maybe Int)
  | RPush BS.ByteString [BS.ByteString]
  | LRange BS.ByteString Int Int
  deriving (Show, Eq)

bsUpper :: ByteString -> ByteString
bsUpper = BS.map toUpper

decodeRequest :: RedisValue -> Either String Request
decodeRequest (Array (BulkString cmd : args)) = decodeInner (bsUpper cmd) args
decodeRequest _ = Left "Expected Array with first element BulkString as command pattern"

decodeInner :: BS.ByteString -> [RedisValue] -> Either String Request
decodeInner "PING" [] = Right Ping
decodeInner "ECHO" [BulkString msg] = Right $ Echo msg
decodeInner "GET" [BulkString key] = Right $ Get key
decodeInner "SET" (BulkString key : BulkString val : rest) =
  case parseExpiration rest of
    Nothing -> if null rest then Right (Set key val Nothing) else Left "Unrecognized Set args"
    justExp -> Right (Set key val justExp)
decodeInner "RPUSH" (BulkString key : vals) =
  case traverse fromBulkString vals of
    Just bsList -> Right $ RPush key bsList
    Nothing -> Left "can't decode RPUSH args"
  where
    fromBulkString (BulkString b) = Just b
    fromBulkString _ = Nothing
decodeInner "LRANGE" [BulkString key, BulkString from, BulkString to] =
  case (BS.readInt from, BS.readInt to) of
    (Just (from, _), Just (to, _)) -> Right $ LRange key from to
    _ -> Left "can't decode LRANGE args"
decodeInner cmd _ = Left $ "unrecognized command: " <> show cmd

-- Парсер expiration
parseExpiration :: [RedisValue] -> Maybe Int
parseExpiration [BulkString cmd, BulkString numBS]
  | BS.map toUpper cmd == "EX",
    [(n, "")] <- reads (BS.unpack numBS) =
      Just (n * 1000)
  | BS.map toUpper cmd == "PX",
    [(n, "")] <- reads (BS.unpack numBS) =
      Just n
parseExpiration _ = Nothing