module Storage.Entries.Array where

import Control.Concurrent
import Control.Concurrent.STM (retry)
import qualified Control.Monad.STM as STM
import Data.Sequence (ViewL (..), (><))
import qualified Data.Sequence as Seq
import qualified StmContainers.Map as SM
import Storage.Definition
import qualified Storage.Entry as SE

rpush :: Storage -> ByteString -> [ByteString] -> IO Integer
rpush store key vals = modifySeq store key (>< fromList vals)

lpush :: Storage -> ByteString -> [ByteString] -> IO Integer
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

modifySeq :: Storage -> ByteString -> (Seq ByteString -> Seq ByteString) -> IO Integer
modifySeq store key modifier = defaultAtomically 0 $ do
  modifiedArray <- modifier <$> getArray store key
  setArray store key modifiedArray
  pure $ toInteger $ length modifiedArray

llen :: Storage -> ByteString -> IO Integer
llen store key = defaultAtomically 0 $ do
  array <- getArray store key
  pure $ toInteger $ length array

lpop :: Storage -> ByteString -> Int -> IO [ByteString]
lpop store key len = defaultAtomically [] $ do
  array <- getArray store key
  let (removed, rest) = Seq.splitAt len array
  setArray store key rest
  pure $ toList removed

getRange :: Storage -> ByteString -> Int -> Int -> IO [ByteString]
getRange store key from to = defaultAtomically [] $ do
  array <- getArray store key
  let len = length array
  let from' = if from < 0 then len + from else from
  let to' = if to < 0 then len + to else to
  pure $
    array
      & Seq.drop from'
      & Seq.take (to' - from' + 1)
      & toList

blpop :: Storage -> ByteString -> Float -> IO (Maybe [ByteString])
blpop store key timeout = do
  cancelFlag <- newTVarIO False
  when (timeout > 0) $ void . forkIO $ do
    threadDelay (round $ timeout * 1000 * 1000)
    safeAtomically $ writeTVar cancelFlag True
  defaultAtomically Nothing $ do
    array <- getArray store key
    case array of
      (Seq.viewl -> item :< rest) -> do
        setArray store key rest
        pure $ Just [key, item]
      _ -> do
        isCanceled <- readTVar cancelFlag
        if isCanceled then pure Nothing else retry
