module Data.Request.Encode (encodeRequest) where

import qualified Data.ByteString.Char8 as BS
import Data.Protocol.Types
import Data.Request
import qualified Storage.Entry as SE

encodeRequest :: Request -> RedisValue
encodeRequest req =
  Array (map BulkString (toArgs req))

toArgs :: Request -> [BS.ByteString]
toArgs Ping = ["PING"]
toArgs Info = ["INFO"]
toArgs Config = ["CONFIG"]
toArgs (Echo msg) = ["ECHO", msg]
toArgs (Get key) = ["GET", key]
toArgs (Type key) = ["TYPE", key]
toArgs (Set key val Nothing) =
  ["SET", key, val]
toArgs (Set key val (Just ttl)) =
  ["SET", key, val, "EX", BS.pack (show ttl)]
-- LIST commands
toArgs (RPush key vals) =
  "RPUSH" : key : vals
toArgs (LPush key vals) =
  "LPUSH" : key : vals
toArgs (LRange key from to) =
  ["LRANGE", key, bsShow from, bsShow to]
toArgs (LLen key) =
  ["LLEN", key]
toArgs (LPop key n) =
  ["LPOP", key, bsShow n]
toArgs (BLPop key timeout) =
  ["BLPOP", key, bsShowFloat timeout]
-- STREAM commands
toArgs (Xadd stream key fields) =
  "XADD" : stream : encodeStreamKey key ++ flattenPairs fields
  where
    encodeStreamKey (Explicit ms seq) =
      [BS.pack (show ms <> "-" <> show seq)]
    encodeStreamKey (AutogenerateSequenceNumber ms) =
      [BS.pack (show ms <> "-*")]
    encodeStreamKey Autogenerate =
      ["*"]
toArgs (Xrange stream from to) =
  ["XRANGE", stream, encodeId from, encodeId to]
toArgs (Xread streams) =
  ["XREAD", "STREAMS"] ++ encodePairs streams
toArgs (XreadBlock stream block maybeId) =
  ["XREAD", "BLOCK", bsShow block, "STREAMS", stream]
    ++ maybe ["$"] (pure . encodeId) maybeId
-- Transactions
toArgs (Incr key) = ["INCR", key]
toArgs Multi = ["MULTI"]
toArgs Exec = ["EXEC"]
toArgs Discard = ["DISCARD"]
toArgs (ReplConf a b) = ["REPLCONF", a, b]
toArgs (Psync a b) = ["PSYNC", a, b]

----------------------------------------------------
-- Helpers
----------------------------------------------------

bsShow :: (Show a) => a -> BS.ByteString
bsShow = BS.pack . show

bsShowFloat :: Float -> BS.ByteString
bsShowFloat = BS.pack . show

encodeId :: SE.StreamID -> BS.ByteString
encodeId (SE.StreamID ms seq) =
  BS.pack (show ms <> "-" <> show seq)

encodePairs :: [(BS.ByteString, SE.StreamID)] -> [BS.ByteString]
encodePairs = concatMap (\(k, id) -> [k, encodeId id])

flattenPairs :: [(BS.ByteString, BS.ByteString)] -> [BS.ByteString]
flattenPairs = concatMap (\(k, v) -> [k, v])
