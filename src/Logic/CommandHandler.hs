module Logic.CommandHandler where

import Data.Protocol.Types (RedisValue (..))
import Data.Request
import Network.ClientState
import qualified Storage.Entry as SE
import Storage.Storage
import Logic.TxStep

handleTx :: Storage -> IORef ClientState -> Request -> IO RedisValue
handleTx _ clientStateRef Multi = do
  startTx clientStateRef
  redisok
handleTx _ clientStateRef Discard = do
  clientState <- readIORef clientStateRef
  if isTxReceiving clientState
    then do
      finishTx clientStateRef
      redisok
    else pure $ ErrorString "ERR DISCARD without MULTI"
handleTx store clientStateRef Exec = do
  clientState <- readIORef clientStateRef
  if isTxReceiving clientState
    then do
      finishTx clientStateRef
      steps <- forM (reverse $ txs clientState) $
        \tx -> handleCommand store tx
      let (finalizers, stms) = unzip steps
      results <- atomically $ sequence stms
      sequence_ finalizers
      pure $ Array results
    else pure $ ErrorString "ERR EXEC without MULTI"
handleTx store clientStateRef other = do
  clientState <- readIORef clientStateRef
  if isTxReceiving clientState
    then do
      addTxRequestIO clientStateRef other
      pure $ BulkString "QUEUED"
    else do
      (finalizer, stmAction) <- handleCommand store other
      response <- atomically stmAction
      finalizer
      pure response



handleCommand :: Storage -> Request -> TxStep
handleCommand _ Ping = txStepFromA $ SimpleString "PONG"
handleCommand _ (Echo str) = txStepFromA $ BulkString str
handleCommand store (Get key) = txStepFromSTM $ getValue store key
handleCommand store (Type key) = txStepFromSTM $ getType store key
handleCommand store (Set key val mExp) = setValue store key val mExp
-- handleCommand store (LPush key vals) = do
--   len <- lpush store key vals
--   pure $ Integer len
-- handleCommand store (RPush key vals) = do
--   len <- rpush store key vals
--   pure $ Integer len
-- handleCommand store (LRange key from to) = do
--   range <- getRange store key from to
--   pure $ Array $ BulkString <$> range
-- handleCommand store (LLen key) = do
--   len <- llen store key
--   pure $ Integer len
-- handleCommand store (LPop key len) = do
--   range <- lpop store key len
--   pure $ case range of
--     [] -> NilString
--     [oneString] -> BulkString oneString
--     more -> Array $ BulkString <$> more
-- handleCommand store (BLPop key timeout) = do
--   item <- blpop store key timeout
--   pure $ maybe NilArray (Array . (BulkString <$>)) item
-- handleCommand store (Xadd key entryKey entries) = do
--   mTimestamp <- xadd store key entryKey entries
--   case mTimestamp of
--     Right timestamp -> pure $ BulkString timestamp
--     Left err -> pure $ ErrorString err
-- handleCommand store (Xrange key from to) = do
--   entries <- xrange store key from to
--   pure $ Array $ map formatStreamEntry entries
-- handleCommand store (Xread keyIds) = do
--   keyEntries <- xread store keyIds
--   let keyEntriesToArray (key, entries) = Array [BulkString key, Array $ formatStreamEntry <$> entries]
--   pure $ Array $ keyEntriesToArray <$> keyEntries
-- handleCommand store (XreadBlock key mTimeout entryId) = do
--   entries <- xreadBlock store key mTimeout entryId
--   pure $ case entries of
--     Nothing -> NilArray
--     Just e -> Array [Array [BulkString key, Array $ formatStreamEntry <$> e]]
handleCommand store (Incr key) = txStepFromSTM $ incValue store key
handleCommand _ Multi = ok
handleCommand _ Exec = ok
handleCommand _ Discard = ok
handleCommand _ Info = txStepFromA $ BulkString "redis_version:0.1.0\r\n"
handleCommand _ Config = txStepFromA $ Array [BulkString "databases", BulkString "16"]

formatKeyValue :: (ByteString, ByteString) -> [RedisValue]
formatKeyValue (key', value) = [BulkString key', BulkString value]

formatStreamEntry :: SE.StreamEntry -> RedisValue
formatStreamEntry (SE.StreamEntry entryId keyValues) = Array [BulkString (show entryId), Array $ formatKeyValue =<< keyValues]

ok :: TxStep
ok = txStepFromA $ SimpleString "OK"

redisok :: IO RedisValue
redisok = pure $ SimpleString "OK"
