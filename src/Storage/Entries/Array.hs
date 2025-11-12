module Storage.Entries.Array where

import Control.Concurrent
import Control.Concurrent.STM (retry)
import Data.Sequence (ViewL (..), (><))
import qualified Data.Sequence as Seq
import qualified StmContainers.Map as SM
import Storage.Definition
import qualified Storage.Entry as SE
import Data.Protocol.Types (RedisValue (..))
import Logic.TxStep (TxStep)

rpush :: Storage -> ByteString -> [ByteString] -> STM RedisValue
rpush store key vals = modifySeq store key (>< fromList vals)

lpush :: Storage -> ByteString -> [ByteString] -> STM RedisValue
lpush store key vals = modifySeq store key (fromList (reverse vals) ><)

getArray :: Storage -> ByteString -> STM (Seq ByteString)
getArray store key = do
  mArray <- SM.lookup key (storeMap store)
  case mArray of
    Just (SE.Array array) -> pure array
    Nothing -> pure mempty
    value -> throwSTM (TypeMismatch $ "Expected array, but got: " <> show value)

setArray :: Storage -> ByteString -> Seq ByteString -> STM ()
setArray store key val = SM.insert (SE.Array val) key (storeMap store)

modifySeq :: Storage -> ByteString -> (Seq ByteString -> Seq ByteString) -> STM RedisValue
modifySeq store key modifier = do
  modifiedArray <- modifier <$> getArray store key
  setArray store key modifiedArray
  pure $ Integer $ toInteger $ length modifiedArray

llen :: Storage -> ByteString -> STM RedisValue
llen store key = do
  array <- getArray store key
  pure $ Integer $ toInteger $ length array

lpop :: Storage -> ByteString -> Int -> STM RedisValue
lpop store key len =  do
  array <- getArray store key
  let (removed, rest) = Seq.splitAt len array
  setArray store key rest
  pure $ case toList removed of
    [] -> NilString
    [oneString] -> BulkString oneString
    more -> Array $ BulkString <$> more
  

getRange :: Storage -> ByteString -> Int -> Int -> STM RedisValue
getRange store key from to = do
  array <- getArray store key
  let len = length array
  let from' = if from < 0 then len + from else from
  let to' = if to < 0 then len + to else to
  pure $ Array $
    array
      & Seq.drop from'
      & Seq.take (to' - from' + 1)
      & fmap BulkString
      & toList

blpop :: Storage -> ByteString -> Float -> TxStep
blpop store key timeout = do
  cancelFlag <- newTVarIO False
  let ioAction = when (timeout > 0) $ void . forkIO $ do
        threadDelay (round $ timeout * 1000 * 1000)
        safeAtomically $ writeTVar cancelFlag True
  let stmAction = do
        array <- getArray store key
        case array of
          (Seq.viewl -> item :< rest) -> do
            setArray store key rest
            pure $ Array [BulkString key, BulkString item]
          _ -> do
            isCanceled <- readTVar cancelFlag
            if isCanceled then pure NilArray else retry
  pure (ioAction, stmAction)
