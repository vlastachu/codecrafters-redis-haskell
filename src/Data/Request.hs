{-# LANGUAGE StrictData #-}

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
  | XreadBlock ByteString Int (Maybe SE.StreamID)
  | ---- Transactions
    Incr ByteString
  | Multi
  | Exec
  deriving (Show, Eq)

data StreamEntryKey
  = Explicit Word64 Word64
  | AutogenerateSequenceNumber Word64
  | Autogenerate
  deriving (Show, Eq)

decodeRequest :: RedisValue -> Either Text Request
decodeRequest (Array (BulkString cmd : args)) = decodeInner (bsUpper cmd) args
decodeRequest _ = Left "Expected Array with first element BulkString as command pattern"

decodeInner :: BS.ByteString -> [RedisValue] -> Either Text Request
decodeInner "PING" [] = Right Ping
decodeInner "ECHO" [BulkString msg] = Right $ Echo msg
decodeInner "GET" [BulkString key] = Right $ Get key
decodeInner "TYPE" [BulkString key] = Right $ Type key
decodeInner "SET" [BulkString key, BulkString val] = Right (Set key val Nothing)
decodeInner "SET" (BulkString key : BulkString val : rest) = Set key val . Just <$> parseExpiration rest
----------------------------
-------ARRAYS---------------
decodeInner "RPUSH" (BulkString key : vals) = RPush key <$> fromBulkStrings vals
decodeInner "LPUSH" (BulkString key : vals) = LPush key <$> fromBulkStrings vals
decodeInner "LRANGE" [BulkString key, BulkString from, BulkString to] =
  LRange key <$> read from <*> read to
decodeInner "LLEN" [BulkString key] = Right $ LLen key
decodeInner "LPOP" [BulkString key, BulkString len] =
  LPop key <$> read len
decodeInner "LPOP" [BulkString key] = decodeInner "LPOP" [BulkString key, BulkString "1"]
decodeInner "BLPOP" [BulkString key, BulkString timeout] = BLPop key <$> read timeout
----------------------------
-------STREAMS--------------
decodeInner "XADD" (BulkString key : BulkString entryKey : keyValueEntries) = case parseEntryKey entryKey of
  Right parsedEntryKey -> Right $ Xadd key parsedEntryKey $ parseKeyValues keyValueEntries
  Left err -> Left err
decodeInner "XRANGE" [BulkString key, BulkString "-", BulkString "+"] = Right $ Xrange key (SE.StreamID 0 0) (SE.StreamID maxBound maxBound)
decodeInner "XRANGE" [BulkString key, BulkString "-", BulkString to] = Xrange key (SE.StreamID 0 0) <$> splitWithDefault maxBound to
decodeInner "XRANGE" [BulkString key, BulkString from, BulkString "+"] = (\f -> Xrange key f (SE.StreamID maxBound maxBound)) <$> splitWithDefault 0 from
decodeInner "XRANGE" [BulkString key, BulkString from, BulkString to] = Xrange key <$> splitWithDefault 0 from <*> splitWithDefault maxBound to
decodeInner "XREAD" [BulkString block, BulkString ms, _, BulkString key, BulkString from] | block ≈ "BLOCK" = do
  msInt <- read ms
  entryId <-
    if from == "$"
      then pure Nothing
      else Just <$> splitWithDefault 0 from
  pure $ XreadBlock key msInt entryId
decodeInner "XREAD" (_ : keysIds) = do
  xs <- fromBulkStrings keysIds
  let n = length xs
  if odd n
    then Left "XREAD keysIds is odd"
    else do
      let (keys, ids) = splitAt (n `div` 2) xs
      streamIds <- mapM (splitWithDefault 0) ids
      return $ Xread (zip keys streamIds)

----------------TRANSACTIONS----
decodeInner "INCR" [BulkString key] = Right $ Incr key
decodeInner "MULTI" [] = Right Multi
decodeInner "Exec" [] = Right Exec
decodeInner cmd _ = Left $ "unrecognized command: " <> show cmd

read :: forall a. (Read a) => ByteString -> Either Text a
read s = readEither $ BS.unpack s

splitWithDefault :: Word64 -> ByteString -> Either Text SE.StreamID
splitWithDefault def s =
  case readMaybe . BS.unpack <$> BS.split '-' s of
    [Just ts] -> Right (SE.StreamID ts def)
    [Just ts, Just seqN] -> Right (SE.StreamID ts seqN)
    _ -> Left $ "Can't parse key: " <> show s

fromBulkStrings :: [RedisValue] -> Either Text [ByteString]
fromBulkStrings = mapM fromBulkString

fromBulkString :: RedisValue -> Either Text ByteString
fromBulkString (BulkString b) = Right b
fromBulkString e = Left $ "BulkString expected but got " <> show e

parseEntryKey :: ByteString -> Either Text StreamEntryKey
parseEntryKey "*" = Right Autogenerate
parseEntryKey entrykey =
  case BS.split '-' entrykey of
    [tsPart, "*"] -> AutogenerateSequenceNumber <$> read tsPart
    [tsPart, seqPart] -> Explicit <$> read tsPart <*> read seqPart
    _ -> Left $ "invalid entry ID format: " <> show entrykey

parseKeyValues :: [RedisValue] -> [(ByteString, ByteString)]
parseKeyValues (BulkString key : BulkString value : rest) = (key, value) : parseKeyValues rest
parseKeyValues _ = []

-- Парсер expiration
parseExpiration :: [RedisValue] -> Either Text Int
parseExpiration [BulkString cmd, BulkString numBS]
  | cmd ≈ "EX" = (1000 *) <$> read numBS
  | cmd ≈ "PX" = read numBS
parseExpiration _ = Left "EX/PX expected"

bsUpper :: ByteString -> ByteString
bsUpper = BS.map toUpper

-- case insensitive (right operand should be uppercased) equality check
(≈) :: ByteString -> ByteString -> Bool
(≈) bs1 bs2 = bsUpper bs1 == bs2
