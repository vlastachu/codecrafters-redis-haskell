module Logic.CommandHandler where

import qualified Data.ByteString.Char8 as BS
import Data.Protocol.Types (RedisValue (NilString))
import Data.Request
import Data.Response
import Network.ClientState
import qualified Storage.Entry as SE
import Storage.Storage

handleTx :: Storage -> IORef ClientState -> Request -> IO Response
handleTx _ clientStateRef Multi = do
  startTx clientStateRef
  pure OK
handleTx _ clientStateRef Disgard = do
  finishTx clientStateRef
  pure OK
handleTx store clientStateRef Exec = do
  clientState <- readIORef clientStateRef
  if isTxReceiving clientState
    then do
      finishTx clientStateRef
      results <- forM (reverse $ txs clientState) $
        \tx -> handleCommand store tx
      pure $ RawArray results
    else pure $ Error "ERR EXEC without MULTI"
handleTx store clientStateRef other = do
  clientState <- readIORef clientStateRef
  if isTxReceiving clientState
    then do
      addTxRequestIO clientStateRef other
      pure $ RawSimpleString "QUEUED"
    else handleCommand store other

handleCommand :: Storage -> Request -> IO Response
handleCommand _ Ping = pure Pong
handleCommand _ (Echo str) = pure $ RawString str
handleCommand store (Get key) = do
  mVal <- getValue store key
  pure $ maybe Nil RawString mVal
handleCommand store (Type key) = do
  val <- getType store key
  pure $ RawSimpleString val
handleCommand store (Set key val mExp) = do
  setValue store key val mExp
  pure OK
handleCommand store (LPush key vals) = do
  len <- lpush store key vals
  pure $ RawInteger len
handleCommand store (RPush key vals) = do
  len <- rpush store key vals
  pure $ RawInteger len
handleCommand store (LRange key from to) = do
  range <- getRange store key from to
  pure $ RawArray $ RawString <$> range
handleCommand store (LLen key) = do
  len <- llen store key
  pure $ RawInteger len
handleCommand store (LPop key len) = do
  range <- lpop store key len
  pure $ case range of
    [] -> Nil
    [oneString] -> RawString oneString
    more -> RawArray $ RawString <$> more
handleCommand store (BLPop key timeout) = do
  item <- blpop store key timeout
  pure $ maybe RawNilArray (RawArray . (RawString <$>)) item
handleCommand store (Xadd key entryKey entries) = do
  mTimestamp <- xadd store key entryKey entries
  case mTimestamp of
    Right timestamp -> pure $ RawString timestamp
    Left err -> pure $ Error err
handleCommand store (Xrange key from to) = do
  entries <- xrange store key from to
  pure $ RawArray $ map formatStreamEntry entries
handleCommand store (Xread keyIds) = do
  keyEntries <- xread store keyIds
  let keyEntriesToArray (key, entries) = RawArray [RawString key, RawArray $ formatStreamEntry <$> entries]
  pure $ RawArray $ keyEntriesToArray <$> keyEntries
handleCommand store (XreadBlock key mTimeout entryId) = do
  entries <- xreadBlock store key mTimeout entryId
  pure $ case entries of
    Nothing -> RawNilArray
    Just e -> RawArray [RawArray [RawString key, RawArray $ formatStreamEntry <$> e]]
handleCommand store (Incr key) = do
  mVal <- incValue store key
  let error = Error "ERR value is not an integer or out of range"
  pure $ maybe error RawInteger mVal
handleCommand store Multi = pure OK
handleCommand store Exec = pure OK

formatKeyValue :: (ByteString, ByteString) -> [Response]
formatKeyValue (key', value) = [RawString key', RawString value]

formatStreamEntry :: SE.StreamEntry -> Response
formatStreamEntry (SE.StreamEntry entryId keyValues) = RawArray [RawString (show entryId), RawArray $ formatKeyValue =<< keyValues]
