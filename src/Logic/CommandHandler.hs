module Logic.CommandHandler where

import Data.Protocol.Types (RedisValue (..))
import Data.Request
import Logic.TxStep
import Network.ClientState
import qualified Storage.Entry as SE
import Storage.Storage

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
      sequence_ finalizers
      results <- atomically $ sequence stms
      pure $ Array results
    else pure $ ErrorString "ERR EXEC without MULTI"
handleTx store clientStateRef other = do
  clientState <- readIORef clientStateRef
  if isTxReceiving clientState
    then do
      addTxRequestIO clientStateRef other
      pure $ SimpleString "QUEUED"
    else do
      (finalizer, stmAction) <- handleCommand store other
      finalizer
      atomically stmAction

handleCommand :: Storage -> Request -> TxStep
handleCommand _ Ping = txStepFromA $ SimpleString "PONG"
handleCommand _ (Echo str) = txStepFromA $ BulkString str
handleCommand store (Get key) = txStepFromSTM $ getValue store key
handleCommand store (Type key) = txStepFromSTM $ getType store key
handleCommand store (Set key val mExp) = setValue store key val mExp
handleCommand store (LPush key vals) = txStepFromSTM $ lpush store key vals
handleCommand store (RPush key vals) = txStepFromSTM $ rpush store key vals
handleCommand store (LRange key from to) = txStepFromSTM $ getRange store key from to
handleCommand store (LLen key) = txStepFromSTM $ llen store key
handleCommand store (LPop key len) = txStepFromSTM $ lpop store key len
handleCommand store (BLPop key timeout) = blpop store key timeout
handleCommand store (Xadd key entryKey entries) = xadd store key entryKey entries
handleCommand store (Xrange key from to) = txStepFromSTM $ xrange store key from to
handleCommand store (Xread keyIds) = txStepFromSTM $ xread store keyIds
handleCommand store (XreadBlock key mTimeout entryId) = xreadBlock store key mTimeout entryId
handleCommand store (Incr key) = txStepFromSTM $ incValue store key
handleCommand _ Multi = ok
handleCommand _ Exec = ok
handleCommand _ Discard = ok
handleCommand store Info = txStepFromA $ BulkString $ if null $ replicaOf store then "role:master" else "role:slave"
handleCommand _ Config = txStepFromA $ Array [BulkString "databases", BulkString "16"]

ok :: TxStep
ok = txStepFromA $ SimpleString "OK"

redisok :: IO RedisValue
redisok = pure $ SimpleString "OK"
