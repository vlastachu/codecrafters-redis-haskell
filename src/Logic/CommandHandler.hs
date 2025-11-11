module Logic.CommandHandler where

import Data.Protocol.Types (RedisValue (..))
import Data.Request
import Network.ClientState
import qualified Storage.Entry as SE
import Storage.Storage

handleTx :: Storage -> IORef ClientState -> Request -> IO RedisValue
handleTx _ clientStateRef Multi = do
  startTx clientStateRef
  ok
handleTx _ clientStateRef Discard = do
  clientState <- readIORef clientStateRef
  if isTxReceiving clientState
    then do
      finishTx clientStateRef
      ok
    else pure $ ErrorString "ERR DISCARD without MULTI"
handleTx store clientStateRef Exec = do
  clientState <- readIORef clientStateRef
  if isTxReceiving clientState
    then do
      finishTx clientStateRef
      results <- forM (reverse $ txs clientState) $
        \tx -> handleCommand store tx
      pure $ Array results
    else pure $ ErrorString "ERR EXEC without MULTI"
handleTx store clientStateRef other = do
  clientState <- readIORef clientStateRef
  if isTxReceiving clientState
    then do
      addTxRequestIO clientStateRef other
      pure $ BulkString "QUEUED"
    else handleCommand store other

handleCommand :: Storage -> Request -> IO RedisValue
handleCommand _ Ping = pure $ SimpleString "PONG"
handleCommand _ (Echo str) = pure $ BulkString str
handleCommand store (Get key) = do
  mVal <- getValue store key
  pure $ maybe NilString BulkString mVal
handleCommand store (Type key) = do
  val <- getType store key
  pure $ SimpleString val
handleCommand store (Set key val mExp) = do
  setValue store key val mExp
  ok
handleCommand store (LPush key vals) = do
  len <- lpush store key vals
  pure $ Integer len
handleCommand store (RPush key vals) = do
  len <- rpush store key vals
  pure $ Integer len
handleCommand store (LRange key from to) = do
  range <- getRange store key from to
  pure $ Array $ BulkString <$> range
handleCommand store (LLen key) = do
  len <- llen store key
  pure $ Integer len
handleCommand store (LPop key len) = do
  range <- lpop store key len
  pure $ case range of
    [] -> NilString
    [oneString] -> BulkString oneString
    more -> Array $ BulkString <$> more
handleCommand store (BLPop key timeout) = do
  item <- blpop store key timeout
  pure $ maybe NilArray (Array . (BulkString <$>)) item
handleCommand store (Xadd key entryKey entries) = do
  mTimestamp <- xadd store key entryKey entries
  case mTimestamp of
    Right timestamp -> pure $ BulkString timestamp
    Left err -> pure $ ErrorString err
handleCommand store (Xrange key from to) = do
  entries <- xrange store key from to
  pure $ Array $ map formatStreamEntry entries
handleCommand store (Xread keyIds) = do
  keyEntries <- xread store keyIds
  let keyEntriesToArray (key, entries) = Array [BulkString key, Array $ formatStreamEntry <$> entries]
  pure $ Array $ keyEntriesToArray <$> keyEntries
handleCommand store (XreadBlock key mTimeout entryId) = do
  entries <- xreadBlock store key mTimeout entryId
  pure $ case entries of
    Nothing -> NilArray
    Just e -> Array [Array [BulkString key, Array $ formatStreamEntry <$> e]]
handleCommand store (Incr key) = do
  mVal <- incValue store key
  let err = ErrorString "ERR value is not an integer or out of range"
  pure $ maybe err Integer mVal
handleCommand _ Multi = ok
handleCommand _ Exec = ok
handleCommand _ Discard = ok
handleCommand _ Info = pure $ BulkString "redis_version:0.1.0\r\n"
handleCommand _ Config = pure $ Array [BulkString "databases", BulkString "16"]

formatKeyValue :: (ByteString, ByteString) -> [RedisValue]
formatKeyValue (key', value) = [BulkString key', BulkString value]

formatStreamEntry :: SE.StreamEntry -> RedisValue
formatStreamEntry (SE.StreamEntry entryId keyValues) = Array [BulkString (show entryId), Array $ formatKeyValue =<< keyValues]

ok :: IO RedisValue
ok = pure $ SimpleString "OK"
