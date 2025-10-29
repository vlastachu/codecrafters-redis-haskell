module Data.Request where

import qualified Data.ByteString.Char8 as BS
import Data.Char (toUpper)
import Data.Protocol.Types
import qualified Storage.Entry as SE

data Request
  = Ping
  | Echo ByteString
  | Get ByteString
  | Set ByteString ByteString (Maybe Int)
  | -- ARRAY Commands
    RPush ByteString [ByteString]
  | LPush ByteString [ByteString]
  | LRange ByteString Int Int
  | LLen ByteString
  | LPop ByteString Int
  | BLPop ByteString Float
  | -- STREAM Commands
    Type ByteString
  | Xadd ByteString StreamEntryKey [(ByteString, ByteString)]
  | Xrange ByteString SE.StreamID SE.StreamID
  | Xread [(ByteString, SE.StreamID)]
  deriving (Show, Eq)

data StreamEntryKey
  = Explicit Word64 Word64
  | AutogenerateSequenceNumber Word64
  | Autogenerate
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
decodeInner "TYPE" [BulkString key] = Right $ Type key
decodeInner "SET" (BulkString key : BulkString val : rest) =
  case parseExpiration rest of
    Nothing -> if null rest then Right (Set key val Nothing) else Left "Unrecognized Set args"
    justExp -> Right (Set key val justExp)
----------------------------
-------ARRAYS---------------
decodeInner "RPUSH" (BulkString key : vals) = RPush key <$> fromBulkStrings vals
decodeInner "LPUSH" (BulkString key : vals) = LPush key <$> fromBulkStrings vals
decodeInner "LRANGE" [BulkString key, BulkString from, BulkString to] =
  case (BS.readInt from, BS.readInt to) of
    (Just (from', _), Just (to', _)) -> Right $ LRange key from' to'
    _ -> Left "can't decode LRANGE args"
decodeInner "LLEN" [BulkString key] = Right $ LLen key
decodeInner "LPOP" [BulkString key, BulkString len] =
  case BS.readInt len of
    Just (len', _) -> Right $ LPop key len'
    _ -> Left "can't decode LPOP args"
decodeInner "LPOP" [BulkString key] = decodeInner "LPOP" [BulkString key, BulkString "1"]
decodeInner "BLPOP" [BulkString key, BulkString timeout] =
  case readMaybe (BS.unpack timeout) of
    Just timeout' -> Right $ BLPop key timeout'
    _ -> Left "can't decode BLPOP args"
----------------------------
-------STREAMS--------------
decodeInner "XADD" (BulkString key : BulkString entryKey : keyValueEntries) = case parseEntryKey entryKey of
  Right parsedEntryKey -> Right $ Xadd key parsedEntryKey $ parseKeyValues keyValueEntries
  Left err -> Left err
decodeInner "XRANGE" [BulkString key, BulkString "-", BulkString "+"] = Right $ Xrange key (SE.StreamID 0 0) (SE.StreamID maxBound maxBound)
decodeInner "XRANGE" [BulkString key, BulkString "-", BulkString to] = Xrange key (SE.StreamID 0 0) <$> splitWithDefault maxBound to
decodeInner "XRANGE" [BulkString key, BulkString from, BulkString "+"] = (\f -> Xrange key f (SE.StreamID maxBound maxBound)) <$> splitWithDefault 0 from
decodeInner "XRANGE" [BulkString key, BulkString from, BulkString to] = Xrange key <$> splitWithDefault 0 from <*> splitWithDefault maxBound to
decodeInner "XREAD" (BulkString _ : keysIds) = do
  xs <- fromBulkStrings keysIds
  let n = length xs
  if odd n
    then Left "XREAD keysIds is odd"
    else do
      let (keys, ids) = splitAt (n `div` 2) xs
      streamIds <- mapM (splitWithDefault 0) ids
      return $ Xread (zip keys streamIds)
decodeInner cmd _ = Left $ "unrecognized command: " <> show cmd

splitWithDefault :: Word64 -> ByteString -> Either String SE.StreamID
splitWithDefault def s =
  case readMaybe . BS.unpack <$> BS.split '-' s of
    [Just ts] -> Right (SE.StreamID ts def)
    [Just ts, Just seqN] -> Right (SE.StreamID ts seqN)
    _ -> Left $ "Can't parse key: " <> show s

fromBulkStrings :: [RedisValue] -> Either String [ByteString]
fromBulkStrings = mapM fromBulkString

fromBulkString :: RedisValue -> Either String ByteString
fromBulkString (BulkString b) = Right b
fromBulkString e = Left $ "BulkString expected but got " <> show e

parseEntryKey :: ByteString -> Either String StreamEntryKey
parseEntryKey "*" = Right Autogenerate
parseEntryKey entrykey =
  case BS.split '-' entrykey of
    [tsPart, "*"] ->
      case readMaybe (BS.unpack tsPart) of
        Just ts -> Right (AutogenerateSequenceNumber ts)
        Nothing -> Left $ "invalid timestamp in entry ID: " <> BS.unpack entrykey
    [tsPart, seqPart] ->
      case (readMaybe (BS.unpack tsPart), readMaybe (BS.unpack seqPart)) of
        (Just ts, Just seqNum) -> Right (Explicit ts seqNum)
        _ -> Left $ "invalid numeric parts in entry ID: " <> BS.unpack entrykey
    _ -> Left $ "invalid entry ID format: " <> BS.unpack entrykey

parseKeyValues :: [RedisValue] -> [(ByteString, ByteString)]
parseKeyValues (BulkString key : BulkString value : rest) = (key, value) : parseKeyValues rest
parseKeyValues _ = []

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